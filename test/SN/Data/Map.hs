module SN.Data.Map
    ( Map
    , new
    , fromList
    , insert
    , delete
    , toList
    , keys
    , elems
    , lookup
    , lookup'
    , member
    , modify
    ) where

import Prelude hiding (lookup)
import Control.Concurrent.STM
import Control.Monad
import Data.Hashable
import Data.Maybe (isJust)
import qualified DeferredFolds.UnfoldlM as UnfoldlM
import qualified StmContainers.Map as StmMap

type Map a b = StmMap.Map a b

new :: IO (Map a b)
new = StmMap.newIO

fromList :: (Eq a, Hashable a) => [(a, b)] -> IO (Map a b)
fromList xs = do
    table <- new
    forM_ xs $ \(k, v) -> insert table k v
    return table

insert :: (Eq a, Hashable a) => Map a b -> a -> b -> IO ()
insert inner key value = atomically $ StmMap.insert value key inner

delete :: (Eq a, Hashable a) => Map a b -> a -> IO ()
delete inner key = atomically $ StmMap.delete key inner

toList :: Map a b -> IO [(a, b)]
toList inner = atomically $ do
    UnfoldlM.foldlM' (\buf x -> return $ x : buf) [] $
        StmMap.unfoldlM inner

keys :: Map a b -> IO [a]
keys inner = map fst <$> toList inner

elems :: Map a b -> IO [b]
elems inner = map snd <$> toList inner

lookup :: (Eq a, Hashable a) => Map a b -> a -> IO (Maybe b)
lookup inner key = atomically $ StmMap.lookup key inner

lookup' :: (Eq a, Hashable a) => Map a b -> a -> IO b
lookup' inner key = do
    mr <- lookup inner key
    case mr of
        Just r -> return r
        Nothing -> error "TData.Map.lookup': no value"

member :: (Eq a, Hashable a) => Map a b -> a -> IO Bool
member inner key = isJust <$> lookup inner key

modify :: (Eq a, Hashable a) => Map a b -> (b -> Maybe b) -> IO ()
modify inner func = atomically $ do
    UnfoldlM.forM_ (StmMap.unfoldlM inner) $ \(k, v) -> do
        case func v of
            Nothing -> return ()
            Just v2 -> StmMap.insert v2 k inner
