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

renderQueryTemplate :: (forall a. [ColumnConstraint a] -> String) -> Query result -> String
renderQueryTemplate renderConstraints (CreateTable table columns constraints) =
    "CREATE TABLE " ++ table ++ " (" ++ strlist (map (columnDecl renderConstraints) columns ++ constraints) ++ ")"
renderQueryTemplate renderConstraints (AddTableColumn table column) =
    "ALTER TABLE " ++ table ++ " ADD COLUMN " ++ columnDecl renderConstraints column
renderQueryTemplate _ (DropTable table) =
    "DROP TABLE " ++ table
renderQueryTemplate _ (Select table fields mcond morder mrange) =
    "SELECT " ++ fieldNames fields ++ " FROM " ++ table
        ++ case mcond of
            Nothing -> ""
            Just (Condition condt _) -> " WHERE " ++ condt
        ++ case morder of
            Nothing -> ""
            Just order -> " ORDER BY " ++ order
        ++ case mrange of
            Nothing -> ""
            Just (RowRange offset limit) -> " LIMIT " ++ show limit ++ " OFFSET " ++ show offset
renderQueryTemplate _ (Insert table fields _) =
    "INSERT INTO " ++ table ++ " (" ++ fieldNames fields ++ ")"
        ++ " VALUES (" ++ fieldPlaceholders fields ++ ")"
renderQueryTemplate _ (InsertReturning table fields _ rets) =
    "INSERT INTO " ++ table ++ " (" ++ fieldNames fields ++ ")"
        ++ " VALUES (" ++ fieldPlaceholders fields ++ ")"
        ++ " RETURNING (" ++ fieldNames rets ++ ")"
renderQueryTemplate _ (Update table fields _ mcond) =
    "UPDATE " ++ table ++ " SET (" ++ fieldNames fields ++ ")"
        ++ " = (" ++ fieldPlaceholders fields ++ ")"
        ++ case mcond of
            Nothing -> ""
            Just (Condition condt _) -> " WHERE " ++ condt
renderQueryTemplate _ (Delete table mcond) =
    "DELETE FROM " ++ table
        ++ case mcond of
            Nothing -> ""
            Just (Condition condt _) -> " WHERE " ++ condt

withConditionValues :: Maybe Condition -> (forall ts. TupleT Value ts -> r) -> r
withConditionValues Nothing f = f E
withConditionValues (Just (Condition _ values)) f = f values

columnDecl :: (forall a. [ColumnConstraint a] -> String) -> ColumnDecl -> String
columnDecl renderConstraints (ColumnDecl field constrs) = fieldName field ++ fieldType field ++ renderConstraints constrs

fieldType :: PrimField a -> String
fieldType (FInt _) = " INTEGER"
fieldType (FFloat _) = " REAL"
fieldType (FText _) = " TEXT"
fieldType (FBlob _) = " BLOB"

fieldNames :: TupleT Field ts -> String
fieldNames = intercalate "," . fieldNameList

fieldPlaceholders :: TupleT Field ts -> String
fieldPlaceholders = intercalate "," . map (const "?") . fieldNameList

fieldNameList :: TupleT Field ts -> [String]
fieldNameList E = []
fieldNameList (Field fs :* rest) = mapTuple fieldName fs ++ fieldNameList rest

strlist :: [String] -> String
strlist = intercalate ","
