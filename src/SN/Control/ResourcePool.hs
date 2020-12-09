{-# LANGUAGE LambdaCase #-}

module SN.Control.ResourcePool
    ( ResourcePool(..)
    , withResourcePool
    ) where

import Control.Concurrent.STM
import Control.Exception
import Control.Monad
import Data.IORef

newtype ResourcePool a = ResourcePool
    { withResource :: forall r. (a -> IO r) -> IO r
    }

withResourcePool :: Int -> Int -> IO a -> (a -> IO ()) -> (a -> SomeException -> IO ()) -> (ResourcePool a -> IO r) -> IO r
withResourcePool initialCount maximumCount create destroy errnotify body = do
    resourcePool <- newTVarIO []
    mask $ \restore -> do
        bodyRet <- pcall restore $ do
            forM_ [initialCount+1 .. maximumCount] $ \_ -> do
                putOne resourcePool Nothing
            forM_ [1 .. initialCount] $ \_ -> do
                bracket
                    create
                    (\res -> putOne resourcePool (Just res))
                    (\_ -> return ())
            body $ ResourcePool $ bracket
                    (takeOne resourcePool >>= \case
                        Just x -> return x
                        Nothing -> create)
                    (\res -> putOne resourcePool (Just res))
        resources <- atomically $ readTVar resourcePool
        finRet <- foldM
            (\buf mres -> do
                case mres of
                    Nothing -> return buf
                    Just resource -> do
                        ret <- pcall restore $ destroy resource
                        case ret of
                            Left err -> (pcall restore $ errnotify resource err) >> return ()
                            Right () -> return ()
                        return $ buf >> ret)
            (Right ())
            resources
        case (bodyRet, finRet) of
            (Right result, Right ()) -> return result
            (Left bodyErr, _) -> throwIO bodyErr
            (_, Left finErr) -> throwIO finErr

takeOne :: TVar [a] -> IO a
takeOne pool = atomically $ do
    list <- readTVar pool
    case list of
        x : xs -> writeTVar pool xs >> return x
        [] -> retry

putOne :: TVar [a] -> a -> IO ()
putOne pool x = atomically $ do
    modifyTVar pool (x :)

pcall :: (forall a. IO a -> IO a) -> IO b -> IO (Either SomeException b)
pcall restore act = try $ restore act
