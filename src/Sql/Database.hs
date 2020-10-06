module Sql.Database
    ( Handle(..)
    , renderQueryTemplate
    ) where

import Data.List
import Sql.Query
import Tuple

data Handle = Handle
    { query :: forall result. Query result -> IO (Maybe result)
    , withTransaction :: forall r. IO r -> IO r
    }

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

columnDecl :: ColumnDecl -> String
columnDecl (ColumnDecl field "") = fieldName field ++ fieldType field
columnDecl (ColumnDecl field descr) = fieldName field ++ fieldType field ++ " " ++ descr

fieldType :: Field a -> String
fieldType (FString _) = " TEXT"
fieldType (FInteger _) = " INTEGER"

strlist :: [String] -> String
strlist = intercalate ","
