{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE FunctionalDependencies #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE UndecidableInstances #-}

module SN.Control.Monad.Cont
    ( ContT(..)
    , execContT
    , MonadTrans(..)
    , MonadIO(..)
    , MonadContPoly(..)
    , MonadShift(..)
    , MonadEscape(..)
    ) where

import Control.Exception
import Control.Monad
import Control.Monad.Trans
import Control.Monad.Reader

newtype ContT f m a = ContT
    { runContT :: (a -> m f) -> m f
    }

execContT :: Applicative m => ContT f m f -> m f
execContT inner = runContT inner pure

instance Functor (ContT f m) where
    fmap f ma = ContT $ \k ->
        runContT ma (k . f)

instance Applicative (ContT f m) where
    pure x = ContT $ \k -> k x
    mf <*> mx = ContT $ \k ->
        runContT mf $ \f ->
            runContT mx $ \x ->
                k (f x)

instance Monad (ContT f m) where
    mx >>= sel = ContT $ \k ->
        runContT mx $ \x ->
            runContT (sel x) k

instance MonadTrans (ContT f) where
    lift mm = ContT $ \k -> mm >>= k

instance MonadIO m => MonadIO (ContT f m) where
    liftIO = lift . liftIO

class MonadContPoly m where
    mcallcc :: ((forall void. a -> m void) -> m a) -> m a

instance MonadContPoly (ContT f m) where
    mcallcc body = ContT $ \k ->
        runContT
            (body $ \arg -> ContT $ \_ -> k arg)
            k

class MonadShift n m | m -> n where
    mshift :: (forall r. (a -> n r) -> n r) -> m a

instance MonadShift m (ContT f m) where
    mshift control = ContT $ \k -> control k

class MonadEscape nf m | m -> nf where
    mescape :: nf -> m void

instance MonadEscape (m f) (ContT f m) where
    mescape mm = ContT $ \_ -> mm

instance MonadContPoly m => MonadContPoly (ReaderT r m) where
    mcallcc inner = ReaderT $ \context -> do
        mcallcc $ \exit -> do
            inner (\arg -> ReaderT $ \_ -> exit arg) `runReaderT` context

instance MonadShift n m => MonadShift n (ReaderT r m) where
    mshift control = ReaderT $ \_ -> mshift control

instance MonadEscape nf m => MonadEscape nf (ReaderT r m) where
    mescape mm = ReaderT $ \_ -> mescape mm
