{-# LANGUAGE LambdaCase #-}

module ResourcePool
    ( ResourcePool(..)
    , withResourcePool
    ) where

import Control.Concurrent.MVar
import Control.Exception
import Control.Monad
import Data.IORef

newtype ResourcePool a = ResourcePool
    { withResource :: forall r. (a -> IO r) -> IO r
    }

withResourcePool :: Int -> Int -> IO a -> (a -> IO ()) -> (a -> SomeException -> IO ()) -> (ResourcePool a -> IO r) -> IO r
withResourcePool initialCount maximumCount create destroy errnotify body = do
    resourcePool <- newIORef []
    cvar <- newMVar ()
    mask $ \restore -> do
        bodyRet <- pcall restore $ do
            forM_ [initialCount+1 .. maximumCount] $ \_ -> do
                putOne resourcePool cvar Nothing
            forM_ [1 .. initialCount] $ \_ -> do
                res <- create
                putOne resourcePool cvar (Just res)
            body $ ResourcePool $ bracket
                    (takeOne resourcePool cvar >>= \case
                        Just x -> return x
                        Nothing -> create)
                    (\res -> putOne resourcePool cvar (Just res))
        resourceList <- readIORef resourcePool
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
            resourceList
        case (bodyRet, finRet) of
            (Right result, Right ()) -> return result
            (Left bodyErr, _) -> throwIO bodyErr
            (_, Left finErr) -> throwIO finErr

pcall :: (forall a. IO a -> IO a) -> IO b -> IO (Either SomeException b)
pcall restore act = try $ restore act

takeOne :: IORef [a] -> MVar () -> IO a
takeOne pool cvar = do
    mr <- atomicModifyIORef' pool $ \case
        x : [] -> ([], Just (x, True))
        x : xs -> (xs, Just (x, False))
        [] -> ([], Nothing)
    case mr of
        Just (x, False) -> return x
        Just (x, True) -> tryTakeMVar cvar >> return x
        Nothing -> readMVar cvar >> takeOne pool cvar

putOne :: IORef [a] -> MVar () -> a -> IO ()
putOne pool cvar x = do
    wasEmpty <- atomicModifyIORef' pool $ \xs -> (x : xs, null xs)
    when wasEmpty $ tryPutMVar cvar () >> return ()
