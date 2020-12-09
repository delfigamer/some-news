module SN.Data.Multimap
    ( Multimap
    , new
    , insert
    , deleteAll
    , delete
    , keys
    , toList
    , toGroupsWith
    , lookupAll
    ) where

import Control.Concurrent.STM
import Data.Hashable
import qualified DeferredFolds.UnfoldlM as UnfoldlM
import qualified StmContainers.Multimap as StmMultimap
import qualified StmContainers.Map as StmMap

type Multimap a b = StmMultimap.Multimap a b

new :: IO (Multimap a b)
new = StmMultimap.newIO

insert :: (Eq a, Eq b, Hashable a, Hashable b) => Multimap a b -> a -> b -> IO ()
insert table key value = atomically $ StmMultimap.insert value key table

deleteAll :: (Eq a, Hashable a) => Multimap a b -> a -> IO ()
deleteAll table key = atomically $ StmMultimap.deleteByKey key table

delete :: (Eq a, Eq b, Hashable a, Hashable b) => Multimap a b -> a -> b -> IO ()
delete table key value = atomically $ StmMultimap.delete value key table

keys :: Multimap a b -> a -> IO [a]
keys table key = atomically $ collect $ StmMultimap.unfoldMKeys table

toList :: Multimap a b -> IO [(a, b)]
toList table = atomically $ collect $ StmMultimap.unfoldlM table

toGroupsWith :: (Eq a, Hashable a) => Multimap a b -> StmMap.Map a c -> IO [(a, [b])]
toGroupsWith table keys = atomically $ UnfoldlM.foldlM' onElem [] $ StmMap.unfoldlM keys
  where
    onElem rest (key, _) = do
        elems <- collect $ StmMultimap.unfoldMByKey key table
        return $ (key, elems) : rest

lookupAll :: (Eq a, Hashable a) => Multimap a b -> a -> IO [b]
lookupAll table key = atomically $ collect $ StmMultimap.unfoldMByKey key table

collect :: UnfoldlM.UnfoldlM STM a -> STM [a]
collect fold = do
    UnfoldlM.foldlM' (\buf x -> return $ x : buf) [] $ fold
