{-# LANGUAGE GeneralizedNewtypeDeriving #-}

module Sql.Query.Render
    ( RenderDetail(..)
    , withQueryRender
    ) where

import Data.Maybe
import Data.Semigroup
import Data.String
import Sql.Query
import Tuple

data RenderDetail = RenderDetail
    { detailFieldType :: forall a. PrimField a -> String
    , detailInsertLeft :: String
    , detailInsertRight :: String
    , detailNullEquality :: String
    }

withQueryRender :: RenderDetail -> Query result -> (forall ts. HList PrimValue ts -> String -> r) -> r
withQueryRender detail query onRender = do
    let RenderPart valbuf strbuf = runRender (renderQuery query) detail
    withPrimValueBuf valbuf $ \prims -> do
        let str = maybe "" (\(Endo f) -> f "") strbuf
        onRender prims str

newtype PrimValueBuf = PrimValueBuf
    { runPrimValueBuf
        :: (forall q. (forall ts. HList PrimValue ts -> q) -> q)
        -> (forall r. (forall us. HList PrimValue us -> r) -> r)
    }

instance Semigroup PrimValueBuf where
    PrimValueBuf fa <> PrimValueBuf fb = PrimValueBuf (\fc -> fa (fb fc))

instance Monoid PrimValueBuf where
    mempty = PrimValueBuf (\fc -> fc)

withPrimValueBuf :: Maybe PrimValueBuf -> (forall ts. HList PrimValue ts -> r) -> r
withPrimValueBuf (Just (PrimValueBuf fa)) cont = fa (\fc -> fc E) cont
withPrimValueBuf Nothing cont = cont E

data RenderPart = RenderPart (Maybe PrimValueBuf) (Maybe (Endo String))

instance Semigroup RenderPart where
    RenderPart pv1 s1 <> RenderPart pv2 s2 = RenderPart (pv1 <> pv2) (s1 <> s2)

instance Monoid RenderPart where
    mempty = RenderPart Nothing Nothing

newtype Render = Render { runRender :: RenderDetail -> RenderPart }
    deriving (Semigroup, Monoid)

instance IsString Render where
    fromString "" = mempty
    fromString str = Render $ \_ -> RenderPart Nothing (Just (Endo (str ++)))

primValues :: HList PrimValue ts -> Render
primValues vals = Render $ \_ ->
    RenderPart
        (Just $ PrimValueBuf $
            \withRest cont ->
                withRest $ \rest -> cont $ vals ++/ rest)
        Nothing

detailString :: (RenderDetail -> String) -> Render
detailString f = Render $ \detail -> case f detail of
    "" -> RenderPart Nothing Nothing
    str -> RenderPart Nothing (Just (Endo (str ++)))

renderQuery :: Query result -> Render
renderQuery (CreateTable (TableName table) columns constraints) =
    "CREATE TABLE " <> fromString table <> " ("
        <> delimConcat ", " (map renderColumnDecl columns ++ map renderTableConstraint constraints)
        <> ")"
renderQuery (CreateIndex (IndexName index) (TableName table) order) =
    "CREATE INDEX " <> fromString index
        <> " ON " <> fromString table <> " ("
        <> renderRowOrder order
        <> ")"
renderQuery (DropTable (TableName table)) =
    "DROP TABLE " <> fromString table
renderQuery (Select sources fields cond order range) =
    delimConcat " " (map renderRecursiveSource sources)
        <> "SELECT " <> renderFieldList fields
        <> " FROM " <> delimConcat ", " (map renderTableSource sources)
        <> renderWhere cond
        <> case order of
            [] -> mempty
            _ -> " ORDER BY " <> renderRowOrder order
        <> renderRowRange range
renderQuery (Insert (TableName table) fields values rets) =
    "INSERT" <> detailString detailInsertLeft
        <> " INTO " <> fromString table
        <> " (" <> renderFieldList fields <> ")"
        <> " VALUES (" <> renderValueList values <> ")"
        <> detailString detailInsertRight
        <> case rets of
            E -> mempty
            _ -> " RETURNING " <> renderFieldList rets
renderQuery (Update (TableName table) (oneField :/ E) (oneValue :/ E) cond) =
    "UPDATE " <> fromString table
        <> " SET " <> renderField oneField
        <> " = " <> renderValue oneValue
        <> renderWhere cond
renderQuery (Update (TableName table) fields values cond) =
    "UPDATE " <> fromString table
        <> " SET (" <> renderFieldList fields <> ")"
        <> " = (" <> renderValueList values <> ")"
        <> renderWhere cond
renderQuery (Delete (TableName table) cond) =
    "DELETE FROM " <> fromString table
        <> renderWhere cond

renderColumnDecl :: ColumnDecl -> Render
renderColumnDecl (ColumnDecl field constrs) =
    fromString (primFieldName field)
        <> detailString (\d -> detailFieldType d field)
        <> foldMap renderColumnConstraint constrs

renderColumnConstraint :: ColumnConstraint a -> Render
renderColumnConstraint CCPrimaryKey = " PRIMARY KEY"
renderColumnConstraint CCNotNull = " NOT NULL"
renderColumnConstraint (CCCheck cond) = " CHECK (" <> fromString cond <> ")"
renderColumnConstraint (CCReferences (TableName table) (FieldName foreignField) onUpdate onDelete) =
    " REFERENCES " <> fromString table <> " (" <> fromString foreignField <> ")"
        <> " ON UPDATE" <> renderFKR onUpdate
        <> " ON DELETE" <> renderFKR onDelete

renderTableConstraint :: TableConstraint -> Render
renderTableConstraint (TCPrimaryKey fields) =
    "PRIMARY KEY (" <> delimConcat ", " (map (\(FieldName f) -> fromString f) fields) <> ")"

renderFKR :: ForeignKeyResolution -> Render
renderFKR FKRNoAction = " NO ACTION"
renderFKR FKRRestrict = " RESTRICT"
renderFKR FKRCascade = " CASCADE"
renderFKR FKRSetNull = " SET NULL"
renderFKR FKRSetDefault = " SET DEFAULT"

renderTableSource :: RowSource -> Render
renderTableSource (TableSource (TableName tableName)) = fromString tableName
renderTableSource (RecursiveSource (TableName tableName) _ _ _) = fromString tableName

renderRecursiveSource :: RowSource -> Render
renderRecursiveSource (RecursiveSource (TableName tableName) recFields initQuery recQuery) =
    "WITH RECURSIVE " <> fromString tableName
        <> " (" <> renderFieldList recFields <> ")"
        <> " AS (" <> renderQuery initQuery
        <> " UNION " <> renderQuery recQuery <> ") "
renderRecursiveSource _ = mempty

renderRowOrder :: [RowOrder] -> Render
renderRowOrder = delimConcat ", " . map r1
  where
    r1 (Asc (FieldName field)) = fromString field <> " ASC"
    r1 (Desc (FieldName field)) = fromString field <> " DESC"

renderFieldList :: HList Field ts -> Render
renderFieldList = delimConcat ", " . homogenize renderField

renderField :: Field a -> Render
renderField (Field prims) = delimConcat ", " (homogenize (fromString . primFieldName) prims)

renderValueList :: HList Value ts -> Render
renderValueList = delimConcat ", " . homogenize renderValue

renderValue :: Value a -> Render
renderValue v = case v of
    Value x -> primValues (primEncode x) <> delimConcat ", " (homogenize (const "?") (primProxies v))
    Null -> delimConcat ", " (homogenize (const "NULL") (primProxies v))
    Subquery query -> renderQuery query

renderWhere :: [Condition] -> Render
renderWhere [] = mempty
renderWhere cond = " WHERE " <> delimConcat " AND " (map renderCondition cond)

renderCondition :: Condition -> Render
renderCondition (Where str) = "(" <> fromString str <> ")"
renderCondition (WhereIs value str) = "(" <> fromString str <> detailString detailNullEquality <> renderValue (Value value) <> ")"
renderCondition (WhereWith value str) = "(" <> fromString str <> ")" <> primValues (primEncode value)
renderCondition (WhereWithList _ [] _) = "FALSE"
renderCondition (WhereWithList left vals right) =
    fromString left
        <> "(" <> delimConcat ", " (map (renderValue . Value) vals)
        <> ")" <> fromString right

renderRowRange :: RowRange -> Render
renderRowRange AllRows = mempty
renderRowRange (RowRange offset limit) =
    " LIMIT " <> fromString (show limit) <> " OFFSET " <> fromString (show offset)

delimConcat :: String -> [Render] -> Render
delimConcat _ [] = mempty
delimConcat _ [x] = x
delimConcat delim (x : xs) = Render $ \detail -> do
    case (runRender x detail, runRender (delimConcat delim xs) detail) of
        (RenderPart vx (Just sx), RenderPart vy (Just sy)) ->
            RenderPart (vx <> vy) (Just (sx <> (Endo (delim ++)) <> sy))
        (x, y) -> x <> y
