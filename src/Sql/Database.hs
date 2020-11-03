module Sql.Database
    ( Handle(..)
    , QueryError(..)
    , TransactionLevel(..)
    ) where

import Sql.Query

data Handle = Handle
    { makeQuery :: forall result. Query result -> IO (Either QueryError result)
    , withTransaction :: forall r. TransactionLevel -> (Handle -> IO (Either QueryError r)) -> IO (Either QueryError r)
    }

data QueryError
    = QueryError
    | SerializationError

data TransactionLevel
    = ReadCommited
    | RepeatableRead
    | Serializable
