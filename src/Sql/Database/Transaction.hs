{-# LANGUAGE FunctionalDependencies #-}
{-# LANGUAGE MultiParamTypeClasses #-}

module Sql.Database.Transaction
    ( Transaction(..)
    , query
    , query_
    , abort
    , retry
    , rollback
    , MonadIO(..)
    , MonadShift(..)
    , runTransactionBody
    ) where

import Control.Monad
import Sql.Query
import Cont

newtype Transaction r a = Transaction
    { unwrapTransaction
        :: forall q.
           (forall result. Query result -> IO q -> IO q -> (result -> IO q) -> IO q)
        -> IO q
        -> IO q
        -> (r -> IO q)
        -> ContT q IO a
    }

query :: Query result -> Transaction r result
query q = Transaction $ \handleQuery kabort kretry _krollback -> do
    ContT $ \knext -> handleQuery q kabort kretry knext

query_ :: Query result -> Transaction r ()
query_ q = void $ query q

abort :: Transaction r a
abort = Transaction $ \_handleQuery kabort _kretry _krollback -> do
    mescape kabort

retry :: Transaction r a
retry = Transaction $ \_handleQuery _kabort kretry _krollback -> do
    mescape kretry

rollback :: r -> Transaction r a
rollback r = Transaction $ \_handleQuery _kabort _kretry krollback -> do
    mescape $ krollback r

instance Functor (Transaction r) where
    fmap = liftM

instance Applicative (Transaction r) where
    pure = return
    (<*>) = ap

instance Monad (Transaction r) where
    return x = Transaction $ \_handleQuery _kabort _kretry _krollback -> do
        return x
    m >>= sel = Transaction $ \handleQuery kabort kretry krollback -> do
        x <- unwrapTransaction m handleQuery kabort kretry krollback
        unwrapTransaction (sel x) handleQuery kabort kretry krollback

instance MonadIO (Transaction r) where
    liftIO io = Transaction $ \_handleQuery _kabort _kretry _krollback -> do
        liftIO io

instance MonadShift IO (Transaction r) where
    mshift wrapper = Transaction $ \_handleQuery _kabort _kretry _krollback -> do
        mshift wrapper

runTransactionBody
    :: Transaction r a
    -> (forall result. Query result -> IO q -> IO q -> (result -> IO q) -> IO q)
    -> IO q
    -> IO q
    -> (r -> IO q)
    -> (a -> IO q)
    -> IO q
runTransactionBody tr handleQuery kabort kretry krollback knext =
    unwrapTransaction tr handleQuery kabort kretry krollback `runContT` knext
