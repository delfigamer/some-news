module SN.Control.Parallel
    ( parallelAp
    , parallelSeq
    , parallelSeq_
    , parallelFor
    , parallelFor_
    ) where

import Control.Concurrent
import Control.Concurrent.STM
import Control.Exception
import Control.Monad

data AsyncResult a
    = Pending
    | Error SomeException
    | Done a

withAsync :: IO a -> (STM (AsyncResult a) -> IO b) -> IO b
withAsync async inner = bracket
    (do
        tvar <- newTVarIO Pending
        tid <- forkIOWithUnmask $ \unmask -> do
            r <- try $ unmask async
            atomically $ writeTVar tvar $ either Error Done r
        return (readTVar tvar, tid))
    (\(poll, tid) -> uninterruptibleMask_ $ do
        killThread tid
        atomically $ do
            ar <- poll
            case ar of
                Pending -> retry
                _ -> return ())
    (\(poll, _) -> do
        inner poll)

parallelAp :: IO (a -> b) -> IO a -> IO b
parallelAp ioF ioX = do
    withAsync ioF $ \pollF -> do
        withAsync ioX $ \pollX -> do
            join $ atomically $ do
                arf <- pollF
                arx <- pollX
                case (arf, arx) of
                    (Error e, _) -> return $ throwIO e
                    (_, Error e) -> return $ throwIO e
                    (Done f, Done x) -> return $ return $ f x
                    _ -> retry

parallelSeq :: [IO a] -> IO [a]
parallelSeq [] = return []
parallelSeq (t : ts) = fmap (:) t `parallelAp` parallelSeq ts

parallelSeq_ :: [IO ()] -> IO ()
parallelSeq_ [] = return ()
parallelSeq_ (t : ts) = fmap (\_ _ -> ()) t `parallelAp` parallelSeq_ ts

parallelFor :: [a] -> (a -> IO b) -> IO [b]
parallelFor xs f = parallelSeq (map f xs)

parallelFor_ :: [a] -> (a -> IO ()) -> IO ()
parallelFor_ xs f = parallelSeq_ (map f xs)
