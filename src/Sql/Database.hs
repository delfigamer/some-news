module Sql.Database
    ( QueryError(..)
    , TransactionLevel(..)
    , Database(..)
    , executeRepeat
    , module Sql.Database.Transaction
    ) where

import Control.Monad
import Control.Monad.IO.Class
import Sql.Database.Transaction
import Sql.Query

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
