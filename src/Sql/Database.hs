module Sql.Database
    ( Handle(..)
    , query
    , renderQueryTemplate
    , withConditionValues
    ) where

import Data.List
import Sql.Query
import Tuple

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

renderQueryTemplate :: Query result -> String
renderQueryTemplate (CreateTable table columns constraints) =
    "CREATE TABLE " ++ table ++ " (" ++ strlist (map columnDecl columns ++ constraints) ++ ")"
renderQueryTemplate (AddTableColumn table column) =
    "ALTER TABLE " ++ table ++ " ADD COLUMN " ++ columnDecl column
renderQueryTemplate (DropTable table) =
    "DROP TABLE " ++ table
renderQueryTemplate (Select table fields mcond morder mrange) =
    "SELECT " ++ strlist (mapTuple fieldName fields) ++ " FROM " ++ table
        ++ case mcond of
            Nothing -> ""
            Just (Condition condt _) -> " WHERE " ++ condt
        ++ case morder of
            Nothing -> ""
            Just order -> " ORDER BY " ++ order
        ++ case mrange of
            Nothing -> ""
            Just (RowRange offset limit) -> " LIMIT " ++ show limit ++ " OFFSET " ++ show offset
renderQueryTemplate (Insert table fields _) =
    "INSERT INTO " ++ table ++ " (" ++ strlist (mapTuple fieldName fields) ++ ")"
        ++ " VALUES (" ++ strlist (mapTuple (const "?") fields) ++ ")"
renderQueryTemplate (InsertReturning table fields _ rets) =
    "INSERT INTO " ++ table ++ " (" ++ strlist (mapTuple fieldName fields) ++ ")"
        ++ " VALUES (" ++ strlist (mapTuple (const "?") fields) ++ ")"
        ++ " RETURNING (" ++ strlist (mapTuple fieldName rets) ++ ")"
renderQueryTemplate (Update table fields _ mcond) =
    "UPDATE " ++ table ++ " SET (" ++ strlist (mapTuple fieldName fields) ++ ")"
        ++ " = (" ++ strlist (mapTuple (const "?") fields) ++ ")"
        ++ case mcond of
            Nothing -> ""
            Just (Condition condt _) -> " WHERE " ++ condt
renderQueryTemplate (Delete table mcond) =
    "DELETE FROM " ++ table
        ++ case mcond of
            Nothing -> ""
            Just (Condition condt _) -> " WHERE " ++ condt

withConditionValues :: Maybe Condition -> (forall ts. TupleT Value ts -> r) -> r
withConditionValues Nothing f = f E
withConditionValues (Just (Condition _ values)) f = f values

columnDecl :: ColumnDecl -> String
columnDecl (ColumnDecl field "") = fieldName field ++ fieldType field
columnDecl (ColumnDecl field descr) = fieldName field ++ fieldType field ++ " " ++ descr

fieldType :: Field a -> String
fieldType (FInt _) = " INTEGER"
fieldType (FString _) = " TEXT"
fieldType (FText _) = " TEXT"

strlist :: [String] -> String
strlist = intercalate ","
