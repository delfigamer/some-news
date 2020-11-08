module Bimap
    ( Bimap(..)
    , fromKeys
    , insert
    , delete
    , removeLeft
    , removeRight
    ) where

import Data.Foldable
import qualified Data.Map.Strict as Map
import qualified Data.Set as Set

data Bimap a b = Bimap
    { left :: Map.Map a (Set.Set b)
    , right :: Map.Map b (Set.Set a)
    }

fromKeys :: (Ord a, Ord b, Foldable f1, Foldable f2) => f1 a -> f2 b -> Bimap a b
fromKeys xs ys = Bimap
    (Map.fromList $ map (\x -> (x, Set.empty)) $ toList xs)
    (Map.fromList $ map (\y -> (y, Set.empty)) $ toList ys)

insert :: (Ord a, Ord b) => (a, b) -> Bimap a b -> Bimap a b
insert (x, y) (Bimap xtoy ytox) = Bimap
    (Map.alter (setInclude y) x xtoy)
    (Map.alter (setInclude x) y ytox)
  where
    setInclude z Nothing = Just (Set.singleton z)
    setInclude z (Just set) = Just (Set.insert z set)

delete :: (Ord a, Ord b) => (a, b) -> Bimap a b -> Bimap a b
delete (x, y) (Bimap xtoy ytox) = Bimap
    (Map.adjust (Set.delete y) x xtoy)
    (Map.adjust (Set.delete x) y ytox)

removeLeft :: Ord a => a -> Bimap a b -> Bimap a b
removeLeft x (Bimap xtoy ytox) = Bimap
    (Map.delete x xtoy)
    (Map.map (Set.delete x) ytox)

removeRight :: Ord b => b -> Bimap a b -> Bimap a b
removeRight y (Bimap xtoy ytox) = Bimap
    (Map.map (Set.delete y) xtoy)
    (Map.delete y ytox)
