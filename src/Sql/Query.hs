{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE PatternSynonyms #-}
{-# LANGUAGE StandaloneDeriving #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE UndecidableInstances #-}

module Sql.Query
    ( TableName(..)
    , IndexName(..)
    , FieldName(..)
    , IsValue(..)
    , PrimValue(..)
    , Value(..)
    , pattern Val
    , pattern Null
    , PrimField(..)
    , Field(..)
    , MapPrims
    , primFields
    , decode
    , encode
    , Where(..)
    , RowRange(..)
    , RowOrder(..)
    , ForeignKeyResolution(..)
    , ColumnConstraint(..)
    , ColumnDecl(..)
    , TableConstraint(..)
    , Query(..)
    , primFieldName
    , fInt
    , fFloat
    , fText
    , fString
    , fBlob
    , fDateTime
    ) where

import qualified Data.ByteString as BS
import Data.Int
import Data.List
import Data.Proxy
import Data.String
import qualified Data.Text as Text
import Data.Time.Clock
import Tuple

newtype TableName = TableName String
deriving instance Show TableName
deriving instance Eq TableName
deriving instance IsString TableName

newtype IndexName = IndexName String
deriving instance Show IndexName
deriving instance Eq IndexName
deriving instance IsString IndexName

newtype FieldName = FieldName String
deriving instance Show FieldName
deriving instance Eq FieldName
deriving instance IsString FieldName

class IsValue a where
    type Prims a :: [*]
    primProxy :: proxy a -> TupleT Proxy (Prims a)
    primNulls :: proxy a -> TupleT PrimValue (Prims a)
    primDecode :: TupleT PrimValue (Prims a) -> Maybe a
    primEncode :: a -> TupleT PrimValue (Prims a)
    primNulls proxy = mkNulls (primProxy proxy)
      where
        mkNulls :: TupleT Proxy ts -> TupleT PrimValue ts
        mkNulls E = E
        mkNulls (_ :* rest) = VNull :* mkNulls rest

data PrimValue a where
    VInt :: !Int64 -> PrimValue Int64
    VFloat :: !Double -> PrimValue Double
    VText :: !Text.Text -> PrimValue Text.Text
    VBlob :: !BS.ByteString -> PrimValue BS.ByteString
    VDateTime :: !UTCTime -> PrimValue UTCTime
    VNull :: PrimValue a
deriving instance Show (PrimValue a)

instance HEq1 PrimValue where
    VInt xA ~= VInt xB = xA == xB
    VFloat xA ~= VFloat xB = xA == xB
    VText xA ~= VText xB = xA == xB
    VBlob xA ~= VBlob xB = xA == xB
    VDateTime xA ~= VDateTime xB = xA == xB
    VNull ~= VNull = True
    _ ~= _ = False

data Value a
    = IsValue a => Value (Maybe a)

pattern Val :: IsValue a => a -> Value a
pattern Val x = Value (Just x)

pattern Null :: IsValue a => Value a
pattern Null = Value Nothing

data PrimField a where
    FInt :: FieldName -> PrimField Int64
    FFloat :: FieldName -> PrimField Double
    FText :: FieldName -> PrimField Text.Text
    FBlob :: FieldName -> PrimField BS.ByteString
    FDateTime :: FieldName -> PrimField UTCTime
deriving instance Show (PrimField a)

instance HEq1 PrimField where
    FInt xA ~= FInt xB = xA == xB
    FFloat xA ~= FFloat xB = xA == xB
    FText xA ~= FText xB = xA == xB
    FBlob xA ~= FBlob xB = xA == xB
    FDateTime xA ~= FDateTime xB = xA == xB
    _ ~= _ = False

data Field a where
    Field :: IsValue a => TupleT PrimField (Prims a) -> Field a

type family MapPrims ts where
    MapPrims '[] = '[]
    MapPrims (a ': as) = ListCat (Prims a) (MapPrims as)

primFields :: TupleT Field ts -> TupleT PrimField (MapPrims ts)
primFields E = E
primFields (Field fs :* rest) = fs >* primFields rest

decode :: TupleT Field ts -> TupleT PrimValue (MapPrims ts) -> TupleT Value ts
decode E E = E
decode (Field fmine :* frest) row = splitTuple fmine row $ \vmine others -> Value (primDecode vmine) :* (decode frest others)

encode :: TupleT Value ts -> TupleT PrimValue (MapPrims ts)
encode E = E
encode (value :* rest) = case value of
    Value (Just x) -> primEncode x >* encode rest
    Value _ -> primNulls value >* encode rest

data Where = forall ts. Where String (TupleT Value ts)

instance Show Where where
    showsPrec d (Where str vals) = showParen (d > 10) $
        showString "Where " . showsPrec 11 str . showString " " . showsPrec 11 (encode vals)

instance Eq Where where
    Where sA tA == Where sB tB = sA == sB && encode tA ~= encode tB

data RowRange
    = RowRange Int64 Int64
    | AllRows
deriving instance Show RowRange
deriving instance Eq RowRange

data RowOrder
    = Asc FieldName
    | Desc FieldName
deriving instance Show RowOrder
deriving instance Eq RowOrder

data ForeignKeyResolution
    = FKRNoAction
    | FKRRestrict
    | FKRCascade
    | FKRSetNull
    | FKRSetDefault
deriving instance Show ForeignKeyResolution
deriving instance Eq ForeignKeyResolution

data ColumnConstraint a where
    CCPrimaryKey :: ColumnConstraint a
    CCNotNull :: ColumnConstraint a
    CCReferences :: TableName -> FieldName -> ForeignKeyResolution -> ForeignKeyResolution -> ColumnConstraint a
deriving instance Show (ColumnConstraint a)

instance HEq1 ColumnConstraint where
    CCPrimaryKey ~= CCPrimaryKey = True
    CCNotNull ~= CCNotNull = True
    CCReferences tableA fieldA onUpdateA onDeleteA ~= CCReferences tableB fieldB onUpdateB onDeleteB =
        tableA == tableB && fieldA == fieldB && onUpdateA == onUpdateB && onDeleteA == onDeleteB
    _ ~= _ = False

data ColumnDecl = forall a. ColumnDecl (PrimField a) [ColumnConstraint a]
deriving instance Show ColumnDecl

instance Eq ColumnDecl where
    ColumnDecl fA cA == ColumnDecl fB cB =
        fA ~= fB && setMatchBy (~=) cA cB

data TableConstraint = TCPrimaryKey [FieldName]
deriving instance Show TableConstraint
deriving instance Eq TableConstraint

data Query result where
    CreateTable :: TableName -> [ColumnDecl] -> [TableConstraint] -> Query ()
    CreateIndex :: IndexName -> TableName -> [RowOrder] -> Query ()
    DropTable :: TableName -> Query ()
    Select :: [TableName] -> TupleT Field rs -> [Where] -> [RowOrder] -> RowRange -> Query [TupleT Value rs]
    Insert :: TableName -> TupleT Field vs -> TupleT Value vs -> TupleT Field rs -> Query (TupleT Value rs)
    Update :: TableName -> TupleT Field vs -> TupleT Value vs -> [Where] -> Query ()
    Delete :: TableName -> [Where] -> Query ()

instance Show (Query ts) where
    showsPrec d (CreateTable table columns constr) = showParen (d > 10)
        $ showString "CreateTable " . showsPrec 11 table
        . showString " " . showsPrec 11 columns
        . showString " " . showsPrec 11 constr
    showsPrec d (CreateIndex index table order) = showParen (d > 10)
        $ showString "CreateIndex " . showsPrec 11 index
        . showString " " . showsPrec 11 table
        . showString " " . showsPrec 11 order
    showsPrec d (DropTable table) = showParen (d > 10)
        $ showString "DropTable " . showsPrec 11 table
    showsPrec d (Select tableList fields cond order range) = showParen (d > 10)
        $ showString "Select " . showsPrec 11 tableList
        . showString " " . showsPrec 11 (primFields fields)
        . showString " " . showsPrec 11 cond
        . showString " " . showsPrec 11 order
        . showString " " . showsPrec 11 range
    showsPrec d (Insert table fields values rets) = showParen (d > 10)
        $ showString "Insert " . showsPrec 11 table
        . showString " " . showsPrec 11 (primFields fields)
        . showString " " . showsPrec 11 (encode values)
        . showString " " . showsPrec 11 (primFields rets)
    showsPrec d (Update table fields values cond) = showParen (d > 10)
        $ showString "Update " . showsPrec 11 table
        . showString " " . showsPrec 11 (primFields fields)
        . showString " " . showsPrec 11 (encode values)
        . showString " " . showsPrec 11 cond
    showsPrec d (Delete table cond) = showParen (d > 10)
        $ showString "Delete " . showsPrec 11 table
        . showString " " . showsPrec 11 cond

instance HEq1 Query where
    CreateTable tableA columnsA constrA ~= CreateTable tableB columnsB constrB =
        tableA == tableB && setMatchBy (==) columnsA columnsB && setMatchBy (==) constrA constrB
    CreateIndex indexA tableA orderA ~= CreateIndex indexB tableB orderB =
        indexA == indexB && tableA == tableB && orderA == orderB
    DropTable tableA ~= DropTable tableB =
        tableA == tableB
    Select tableListA fieldsA condA orderA rangeA ~= Select tableListB fieldsB condB orderB rangeB =
        tableListA == tableListB && primFields fieldsA ~= primFields fieldsB && condA == condB && orderA == orderB && rangeA == rangeB
    Insert tableA fieldsA valuesA retsA ~= Insert tableB fieldsB valuesB retsB =
        tableA == tableB && primFields fieldsA ~= primFields fieldsB && encode valuesA ~= encode valuesB && primFields retsA ~= primFields retsB
    Update tableA fieldsA valuesA condA ~= Update tableB fieldsB valuesB condB =
        tableA == tableB && primFields fieldsA ~= primFields fieldsB && encode valuesA ~= encode valuesB && condA == condB
    Delete tableA condA ~= Delete tableB condB =
        tableA == tableB && condA == condB
    _ ~= _ = False

setMatchBy :: (a -> b -> Bool) -> [a] -> [b] -> Bool
setMatchBy _ [] [] = True
setMatchBy test2 (x:xs) b = case setExcludeBy (test2 x) b of
    Just ys -> setMatchBy test2 xs ys
    Nothing -> False

setExcludeBy :: (b -> Bool) -> [b] -> Maybe [b]
setExcludeBy _ [] = Nothing
setExcludeBy test (y:ys)
    | test y = Just ys
    | otherwise = (y:) <$> setExcludeBy test ys

primFieldName :: PrimField a -> String
primFieldName (FInt (FieldName name)) = name
primFieldName (FFloat (FieldName name)) = name
primFieldName (FText (FieldName name)) = name
primFieldName (FBlob (FieldName name)) = name
primFieldName (FDateTime (FieldName name)) = name

instance IsValue Int64 where
    type Prims Int64 = '[Int64]
    primProxy _ = Proxy :* E
    primDecode (VInt x :* E) = Just x
    primDecode _ = Nothing
    primEncode x = VInt x :* E

fInt :: FieldName -> Field Int64
fInt a = Field (FInt a :* E)

instance IsValue Bool where
    type Prims Bool = '[Int64]
    primProxy _ = Proxy :* E
    primDecode (VInt x :* E) = Just (x /= 0)
    primDecode _ = Just False
    primEncode True = VInt 1 :* E
    primEncode False = VNull :* E

fBool :: FieldName -> Field Bool
fBool a = Field (FInt a :* E)

instance IsValue Double where
    type Prims Double = '[Double]
    primProxy _ = Proxy :* E
    primDecode (VFloat x :* E) = Just x
    primDecode _ = Nothing
    primEncode x = VFloat x :* E

fFloat :: FieldName -> Field Double
fFloat a = Field (FFloat a :* E)

instance IsValue Text.Text where
    type Prims Text.Text = '[Text.Text]
    primProxy _ = Proxy :* E
    primDecode (VText x :* E) = Just x
    primDecode _ = Nothing
    primEncode x = VText x :* E

fText :: FieldName -> Field Text.Text
fText a = Field (FText a :* E)

instance IsValue String where
    type Prims String = '[Text.Text]
    primProxy _ = Proxy :* E
    primDecode (VText x :* E) = Just $ Text.unpack x
    primDecode _ = Nothing
    primEncode x = VText (Text.pack x) :* E

fString :: FieldName -> Field String
fString a = Field (FText a :* E)

instance IsValue BS.ByteString where
    type Prims BS.ByteString = '[BS.ByteString]
    primProxy _ = Proxy :* E
    primDecode (VBlob x :* E) = Just x
    primDecode _ = Nothing
    primEncode x = VBlob x :* E

fBlob :: FieldName -> Field BS.ByteString
fBlob a = Field (FBlob a :* E)

instance IsValue UTCTime where
    type Prims UTCTime = '[UTCTime]
    primProxy _ = Proxy :* E
    primDecode (VDateTime x :* E) = Just x
    primDecode _ = Nothing
    primEncode x = VDateTime x :* E

fDateTime :: FieldName -> Field UTCTime
fDateTime a = Field (FDateTime a :* E)
