module Sql.Database
    ( Transaction(..)
    , query
    , query_
    , abort
    , retry
    , rollback
    , QueryError(..)
    , TransactionLevel(..)
    , Database(..)
    , executeRepeat
    ) where

import Control.Monad
import Control.Monad.IO.Class
import Sql.Query

newtype Transaction r a = Transaction
    { runTransactionBody
        :: forall q.
           (forall result. Query result -> IO q -> IO q -> (result -> IO q) -> IO q)
        -> IO q
        -> IO q
        -> (r -> IO q)
        -> (a -> IO q)
        -> IO q
    }

query :: Query result -> Transaction r result
query q = Transaction $ \handleQuery kabort kretry _krollback knext -> do
    handleQuery q kabort kretry knext

query_ :: Query result -> Transaction r ()
query_ q = Transaction $ \handleQuery kabort kretry _krollback knext -> do
    handleQuery q kabort kretry (\_ -> knext ())

abort :: Transaction r a
abort = Transaction $ \_handleQuery kabort _kretry _krollback _knext -> do
    kabort

retry :: Transaction r a
retry = Transaction $ \_handleQuery _kabort kretry _krollback _knext -> do
    kretry

rollback :: r -> Transaction r a
rollback r = Transaction $ \_handleQuery _kabort _kretry krollback _knext -> do
    krollback r

instance Functor (Transaction r) where
    fmap = liftM

instance Applicative (Transaction r) where
    pure = return
    (<*>) = ap

instance Monad (Transaction r) where
    return x = Transaction $ \_handleQuery _kabort _kretry _krollback knext -> do
        knext x
    m >>= sel = Transaction $ \handleQuery kabort kretry krollback knext -> do
        runTransactionBody m handleQuery kabort kretry krollback $ \x -> do
            runTransactionBody (sel x) handleQuery kabort kretry krollback knext

instance MonadIO (Transaction r) where
    liftIO io = Transaction $ \_handleQuery _kabort _kretry _krollback knext -> do
        io >>= knext

data QueryError
    = QueryError
    | SerializationError

data TransactionLevel
    = ReadCommited
    | RepeatableRead
    | Serializable

newtype Database = Database
    { execute :: forall r. TransactionLevel -> Transaction r r -> IO (Either QueryError r)
    }

executeRepeat :: Database -> Int -> TransactionLevel -> Transaction r r -> IO (Either QueryError r)
executeRepeat db count level tr
    | count <= 0 = return $ Left SerializationError
    | otherwise = do
        result <- execute db level tr
        case result of
            Left SerializationError -> executeRepeat db (count - 1) level tr
            _ -> return result
