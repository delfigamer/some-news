module Sql.Query.Render
    ( DetailRenderer(..)
    , withQueryRender
    ) where

import Data.List
import Sql.Query
import Tuple

data DetailRenderer = DetailRenderer
    { renderFieldType :: forall a. PrimField a -> String
    }

withQueryRender :: DetailRenderer -> Query result -> (forall ts. HList PrimValue ts -> String -> r) -> r
withQueryRender detail (CreateTable (TableName table) columns constraints) onRender =
    onRender E $
        "CREATE TABLE " ++ table ++ " ("
            ++ intercalate ", " (map (renderColumnDecl detail) columns ++ map renderTableConstraint constraints)
            ++ ")"
withQueryRender _ (CreateIndex (IndexName index) (TableName table) order) onRender =
    onRender E $
        "CREATE INDEX " ++ index ++ " ON " ++ table ++ " ("
            ++ renderRowOrder order
            ++ ")"
withQueryRender _ (DropTable (TableName table)) onRender =
    onRender E $
        "DROP TABLE " ++ table
withQueryRender _ (Select tableList fields cond order range) onRender =
    withConditionValues cond $ \condVals -> onRender condVals $
        "SELECT " ++ fieldNames fields ++ " FROM " ++ renderTableList tableList
            ++ case cond of
                [] -> ""
                _ -> " WHERE " ++ renderConditionTemplate cond
            ++ case order of
                [] -> ""
                _ -> " ORDER BY " ++ renderRowOrder order
            ++ case range of
                AllRows -> ""
                RowRange offset limit -> " LIMIT " ++ show limit ++ " OFFSET " ++ show offset
withQueryRender _ (Insert_ (TableName table) fields values) onRender =
    onRender (encode values) $
        "INSERT INTO " ++ table ++ " (" ++ fieldNames fields ++ ")"
            ++ " VALUES (" ++ fieldPlaceholders fields ++ ")"
withQueryRender _ (Insert (TableName table) fields values rets) onRender =
    onRender (encode values) $
        "INSERT INTO " ++ table ++ " (" ++ fieldNames fields ++ ")"
            ++ " VALUES (" ++ fieldPlaceholders fields ++ ")"
            ++ " RETURNING " ++ fieldNames rets
withQueryRender _ (Update (TableName table) fields values cond) onRender =
    withConditionValues cond $ \condVals -> onRender (encode values ++/ condVals) $
        "UPDATE " ++ table ++ " SET (" ++ fieldNames fields ++ ")"
            ++ " = (" ++ fieldPlaceholders fields ++ ")"
            ++ case cond of
                [] -> ""
                _ -> " WHERE " ++ renderConditionTemplate cond
withQueryRender _ (Delete (TableName table) cond) onRender =
    withConditionValues cond $ \condVals -> onRender condVals $
        "DELETE FROM " ++ table
            ++ case cond of
                [] -> ""
                _ -> " WHERE " ++ renderConditionTemplate cond

withConditionValues :: [Where] -> (forall ts. HList PrimValue ts -> r) -> r
withConditionValues [] f = f E
withConditionValues (Where _ values:rest) f = withConditionValues rest $ \others -> f $ encode values ++/ others

renderColumnDecl :: DetailRenderer -> ColumnDecl -> String
renderColumnDecl detail (ColumnDecl field constrs) = primFieldName field ++ renderFieldType detail field ++ renderColumnConstraints constrs

renderColumnConstraints :: [ColumnConstraint a] -> String
renderColumnConstraints [] = ""
renderColumnConstraints (CCPrimaryKey:others) = " PRIMARY KEY" ++ renderColumnConstraints others
renderColumnConstraints (CCNotNull:others) = " NOT NULL" ++ renderColumnConstraints others
renderColumnConstraints (CCReferences (TableName table) (FieldName foreignField) onUpdate onDelete:others) =
    " REFERENCES " ++ table ++ " (" ++ foreignField ++ ")"
        ++ " ON UPDATE" ++ renderFKR onUpdate
        ++ " ON DELETE" ++ renderFKR onDelete
        ++ renderColumnConstraints others

renderTableConstraint :: TableConstraint -> String
renderTableConstraint (TCPrimaryKey fields) =
    "PRIMARY KEY (" ++ intercalate "," (map (\(FieldName f) -> f) fields) ++ ")"

renderFKR :: ForeignKeyResolution -> String
renderFKR FKRNoAction = " NO ACTION"
renderFKR FKRRestrict = " RESTRICT"
renderFKR FKRCascade = " CASCADE"
renderFKR FKRSetNull = " SET NULL"
renderFKR FKRSetDefault = " SET DEFAULT"

renderRowOrder :: [RowOrder] -> String
renderRowOrder = intercalate ", " . map r1
  where
    r1 (Asc (FieldName field)) = field
    r1 (Desc (FieldName field)) = field ++ " DESC"

renderConditionTemplate :: [Where] -> String
renderConditionTemplate = intercalate " AND " . map (\(Where str _) -> "(" ++ str ++ ")")

fieldNames :: HList Field ts -> String
fieldNames = intercalate ", " . fieldList

fieldPlaceholders :: HList Field ts -> String
fieldPlaceholders = intercalate ", " . map (const "?") . fieldList

fieldList :: HList Field ts -> [String]
fieldList E = []
fieldList (Field fs :/ rest) = homogenize primFieldName fs ++ fieldList rest

renderTableList :: [TableName] -> String
renderTableList = intercalate ", " . map (\(TableName table) -> table)
