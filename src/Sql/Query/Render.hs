module Sql.Query.Render
    ( DetailRenderer(..)
    , renderQueryTemplate
    , withConditionValues
    ) where

import Data.List
import Sql.Query
import Tuple

data DetailRenderer = DetailRenderer
    { renderConstraints :: forall a. [ColumnConstraint a] -> String
    , renderFieldType :: forall a. PrimField a -> String
    }

renderQueryTemplate :: DetailRenderer -> Query result -> String
renderQueryTemplate detail (CreateTable table columns constraints) =
    "CREATE TABLE " ++ table ++ " (" ++ intercalate "," (map (columnDecl detail) columns ++ constraints) ++ ")"
renderQueryTemplate detail (AddTableColumn table column) =
    "ALTER TABLE " ++ table ++ " ADD COLUMN " ++ columnDecl detail column
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
        ++ case normalizeRange mrange of
            Nothing -> ""
            Just (RowRange offset limit) -> " LIMIT " ++ show limit ++ " OFFSET " ++ show offset
renderQueryTemplate _ (Insert table fields _) =
    "INSERT INTO " ++ table ++ " (" ++ fieldNames fields ++ ")"
        ++ " VALUES (" ++ fieldPlaceholders fields ++ ")"
renderQueryTemplate _ (InsertReturning table fields _ rets) =
    "INSERT INTO " ++ table ++ " (" ++ fieldNames fields ++ ")"
        ++ " VALUES (" ++ fieldPlaceholders fields ++ ")"
        ++ " RETURNING " ++ fieldNames rets
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

columnDecl :: DetailRenderer -> ColumnDecl -> String
columnDecl detail (ColumnDecl field constrs) = fieldName field ++ renderFieldType detail field ++ renderConstraints detail constrs

fieldNames :: TupleT Field ts -> String
fieldNames = intercalate "," . fieldNameList

fieldPlaceholders :: TupleT Field ts -> String
fieldPlaceholders = intercalate "," . map (const "?") . fieldNameList

fieldNameList :: TupleT Field ts -> [String]
fieldNameList E = []
fieldNameList (Field fs :* rest) = mapTuple fieldName fs ++ fieldNameList rest

normalizeRange :: Maybe RowRange -> Maybe RowRange
normalizeRange Nothing = Nothing
normalizeRange (Just (RowRange offset limit))
    | limit <= 0 = Nothing
    | offset < 0 = Just (RowRange 0 limit)
    | otherwise = Just (RowRange offset limit)
