module Sql.Database
    ( Handle(..)
    , query
    ) where

import Sql.Query

data Handle = Handle
    { queryMaybe :: forall result. Query result -> IO (Maybe result)
    , withTransaction :: forall r. IO r -> IO r
    }

query :: Handle -> Query result -> IO result
query db queryData = do
    mr <- queryMaybe db queryData
    case mr of
        Just r -> return r
        Nothing -> fail "query failed"
