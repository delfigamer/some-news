module Parallel
    ( parallelAp
    , parallelSeq
    , parallelSeq_
    , parallelFor
    , parallelFor_
    ) where

import Control.Concurrent
import Control.Exception

parallelAp :: IO (a -> b) -> IO a -> IO b
parallelAp aleft aright = uninterruptibleMask $ \restore -> do
    mvar <- newEmptyMVar
    tidL <- mask_ $ forkIOWithUnmask $ \unmask -> do
        r <- trySome $ unmask aleft
        putMVar mvar (Left r)
    tidR <- mask_ $ forkIOWithUnmask $ \unmask -> do
        r <- trySome $ unmask aright
        putMVar mvar (Right r)
    r1 <- trySome $ restore $ takeMVar mvar
    case r1 of
        Left ex -> do
            killThread tidL
            killThread tidR
            _ <- takeMVar mvar
            _ <- takeMVar mvar
            throwIO ex
        Right (Left (Left ex)) -> do
            killThread tidR
            _ <- takeMVar mvar
            throwIO ex
        Right (Left (Right left)) -> do
            r2 <- trySome $ restore $ takeMVar mvar
            case r2 of
                Left ex -> do
                    killThread tidR
                    _ <- takeMVar mvar
                    throwIO ex
                Right (Right (Left ex)) -> do
                    throwIO ex
                Right (Right (Right right)) -> do
                    restore $ return $ left right
                _ -> error "shouldn't happen"
        Right (Right (Left ex)) -> do
            killThread tidL
            _ <- takeMVar mvar
            throwIO ex
        Right (Right (Right right)) -> do
            r2 <- trySome $ restore $ takeMVar mvar
            case r2 of
                Left ex -> do
                    killThread tidL
                    _ <- takeMVar mvar
                    throwIO ex
                Right (Left (Left ex)) -> do
                    throwIO ex
                Right (Left (Right left)) -> do
                    restore $ return $ left right
                _ -> error "shouldn't happen"
  where
    trySome :: IO a -> IO (Either SomeException a)
    trySome = try

parallelSeq :: [IO a] -> IO [a]
parallelSeq [] = return []
parallelSeq [t] = fmap (: []) t
parallelSeq (t : ts) = fmap (:) t `parallelAp` parallelSeq ts

parallelSeq_ :: [IO ()] -> IO ()
parallelSeq_ [] = return ()
parallelSeq_ [t] = t
parallelSeq_ (t : ts) = fmap (\_ _ -> ()) t `parallelAp` parallelSeq_ ts

parallelFor :: [a] -> (a -> IO b) -> IO [b]
parallelFor xs f = parallelSeq (map f xs)

parallelFor_ :: [a] -> (a -> IO ()) -> IO ()
parallelFor_ xs f = parallelSeq_ (map f xs)

-- sleepUntil u = do
    -- t <- getCurrentTime
    -- if t < u
        -- then yield >> sleepUntil u
        -- else return ()

-- sleep dt = do
    -- t <- getCurrentTime
    -- sleepUntil $ addUTCTime dt t
