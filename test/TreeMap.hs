module Tree
    ( Tree
    , keys
    , member
    , notMember
    , lookup
    , foldr
    , empty
    , include
    , exclude
    , adjust
    , ancestors
    , childMap
    , descendantMap
    , trySetParent
    , excludeSubtreeSet
    ) where

import Prelude hiding (foldr, lookup)
import Control.Monad
import Control.Monad.Identity
import Control.Monad.State.Strict
import qualified Data.Map.Lazy as MapLazy
import qualified Data.Map.Strict as Map
import qualified Data.Set as Set

newtype Tree a b = Tree
    { cells :: Map.Map a (TreeCell a b)
    }
    deriving (Show)

data TreeCell a b = TreeCell
    { cellData :: b
    , cellParent :: a
    , cellChildren :: Set.Set a
    }
    deriving (Show)

unionChildren :: Ord a => Set.Set a -> TreeCell a b -> TreeCell a b
unionChildren children cell = cell {cellChildren = Set.union children $ cellChildren cell}

insertChild :: Ord a => a -> TreeCell a b -> TreeCell a b
insertChild child cell = cell {cellChildren = Set.insert child $ cellChildren cell}

deleteChild :: Ord a => a -> TreeCell a b -> TreeCell a b
deleteChild child cell = cell {cellChildren = Set.delete child $ cellChildren cell}

adjustParent :: Eq a => a -> a -> TreeCell a b -> TreeCell a b
adjustParent oldParent newParent cell
    | cellParent cell == oldParent = cell {cellParent = newParent}
    | otherwise = cell

modifyTreeM :: Monad m => StateT (Map.Map a (TreeCell a b)) m () -> Tree a b -> m (Tree a b)
modifyTreeM act tm = fmap (Tree $!) $ execStateT act (cells tm)

modifyTree :: State (Map.Map a (TreeCell a b)) () -> Tree a b -> Tree a b
modifyTree act tm = Tree $! execState act (cells tm)

keys :: Tree a b -> [a]
keys tm = Map.keys $ cells tm

member :: Ord a => a -> Tree a b -> Bool
member k tm = Map.member k (cells tm)

notMember :: Ord a => a -> Tree a b -> Bool
notMember k tm = Map.notMember k (cells tm)

lookup :: Ord a => a -> Tree a b -> Maybe (b, a)
lookup k tm = case Map.lookup k (cells tm) of
    Nothing -> Nothing
    Just cell -> Just (cellData cell, cellParent cell)

foldr :: (a -> b -> a -> r -> r) -> r -> Tree a b -> r
foldr func seed tm = Map.foldrWithKey (\k cell r -> func k (cellData cell) (cellParent cell) r) seed (cells tm)

empty :: Tree a b
empty = Tree Map.empty

include :: Ord a => a -> b -> a -> Tree a b -> Tree a b
include k v parent = modifyTree $ do
    modify $ Map.insert k $ TreeCell v parent Set.empty
    modify $ Map.adjust (insertChild k) parent

exclude :: Ord a => a -> Tree a b -> (Maybe a, Tree a b)
exclude k tm = case mCell of
    Nothing -> (Nothing, tm)
    Just cell -> do
        let resultMap = flip execState reducedMap $ do
                modify $ Map.map (adjustParent k (cellParent cell))
                modify $ Map.adjust (deleteChild k . unionChildren (cellChildren cell)) (cellParent cell)
        (Just $ cellParent cell, Tree $! resultMap)
  where
    (mCell, reducedMap) = Map.alterF
        (\mCell -> (mCell, Nothing))
        k (cells tm)

adjust :: Ord a => (b -> b) -> a -> Tree a b -> Tree a b
adjust f k = modifyTree $ do
    modify $ Map.adjust (\cell -> cell {cellData = f (cellData cell)}) k

ancestors :: Ord a => a -> Tree a b -> [a]
ancestors k tm = k : case Map.lookup k (cells tm) of
    Nothing -> []
    Just cell -> ancestors (cellParent cell) tm

childMap :: Ord a => Tree a b -> Map.Map a (Set.Set a)
childMap tm = Map.map cellChildren (cells tm)

descendantMap :: Ord a => Tree a b -> Map.Map a (Set.Set a)
descendantMap tm = result
  where
    result = MapLazy.mapWithKey descendants (cells tm)
    descendants k cell = Set.unions $
        Set.singleton k : cellChildren cell :
            map (result Map.!) (Set.elems $ cellChildren cell)

trySetParent :: Ord a => a -> a -> Tree a b -> Maybe (Tree a b)
trySetParent child newParent tm = case Map.lookup child (cells tm) of
    Nothing -> Nothing
    Just childCell -> do
        let oldParent = cellParent childCell
        if oldParent == newParent
            then Just tm
            else flip modifyTreeM tm $ do
                guard $ child `notElem` ancestors newParent tm
                modify $ Map.adjust (deleteChild child) oldParent
                modify $ Map.adjust (insertChild child) newParent
                modify $ Map.insert child $ childCell {cellParent = newParent}

excludeSubtreeSet :: Ord a => Set.Set a -> Tree a b -> Tree a b
excludeSubtreeSet minusSet tm = Tree $ Map.filterWithKey
    (\k _ -> all (`Set.notMember` minusSet) $ ancestors k tm)
    (cells tm)
