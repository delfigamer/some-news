module Sql.Database
    ( Database(..)
    , QueryError(..)
    , TransactionLevel(..)
    ) where

import Sql.Query

data Database = Database
    { makeQuery :: forall result. Query result -> IO (Either QueryError result)
    , withTransaction :: forall r. TransactionLevel -> (Database -> IO (Either QueryError r)) -> IO (Either QueryError r)
    }

data QueryError
    = QueryError
    | SerializationError

data TransactionLevel
    = ReadCommited
    | RepeatableRead
    | Serializable
