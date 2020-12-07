module TData.TimedHashmap
    ( TimedHashmap
    , new
    , lookup
    , insert
    ) where

import Prelude hiding (lookup)
import Control.Concurrent.STM
import Control.Monad
import Data.Hashable
import Data.Time.Clock
import qualified Data.Sequence as Seq
import qualified StmContainers.Map as Map
import qualified TData.MinHeap as MinHeap

data TimedHashmap k v = TimedHashmap
    (Map.Map k v)
    (TVar (MinHeap.MinHeap (MinHeap.Arg UTCTime k)))

new :: IO (TimedHashmap k v)
new = TimedHashmap <$> Map.newIO <*> newTVarIO MinHeap.empty

lookup :: (Eq k, Hashable k) => TimedHashmap k v -> k -> IO (Maybe v)
lookup (TimedHashmap table _) key = atomically $ Map.lookup key table

insert :: (Eq k, Hashable k) => TimedHashmap k v -> k -> v -> NominalDiffTime -> IO ()
insert (TimedHashmap table tQueue) key value deltaTime
    | deltaTime <= 0 = error "TimedHashmap.insert: non-positive deltaTime"
    | otherwise = do
        now <- getCurrentTime
        let endTime = addUTCTime deltaTime now
        toDelete <- atomically $ do
            q1 <- readTVar tQueue
            let (olds, q2) = MinHeap.break (\(MinHeap.Arg et _) -> et < now) q1
            let q3 = MinHeap.insert (MinHeap.Arg endTime key) q2
            writeTVar tQueue $! q3
            return olds
        forM_ toDelete $ \(MinHeap.Arg _ delKey) -> do
            atomically $ Map.delete delKey table
        atomically $ Map.insert value key table
