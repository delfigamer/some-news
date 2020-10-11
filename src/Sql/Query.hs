{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE PatternSynonyms #-}
{-# LANGUAGE StandaloneDeriving #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE UndecidableInstances #-}

module Sql.Query
    ( TableName
    , ConstraintDecl
    , FieldName
    , RowOrder
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
    , Condition(..)
    , RowRange(..)
    , ColumnConstraint(..)
    , ColumnDecl(..)
    , Query(..)
    , fieldName
    , fInt
    , fFloat
    , fText
    , fString
    , fBlob
    ) where

import qualified Data.ByteString as BS
import Data.Int
import Data.List
import qualified Data.Text as Text
import Tuple

type TableName = String
type ConstraintDecl = String
type FieldName = String
type RowOrder = String

class IsValue a where
    type Prims a :: [*]
    primDecode
        :: TupleT PrimValue (ListCat (Prims a) ts)
        -> (Maybe a -> TupleT PrimValue ts -> r)
        -> r
    primEncode
        :: Maybe a
        -> TupleT PrimValue ts
        -> TupleT PrimValue (ListCat (Prims a) ts)

data PrimValue a where
    VInt :: !Int64 -> PrimValue Int64
    VFloat :: !Double -> PrimValue Double
    VText :: !Text.Text -> PrimValue Text.Text
    VBlob :: !BS.ByteString -> PrimValue BS.ByteString
    VNull :: PrimValue a
deriving instance Show (PrimValue a)

instance HEq1 PrimValue where
    VInt xA ~= VInt xB = xA == xB
    VFloat xA ~= VFloat xB = xA == xB
    VText xA ~= VText xB = xA == xB
    VBlob xA ~= VBlob xB = xA == xB
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
deriving instance Show (PrimField a)

instance HEq1 PrimField where
    FInt xA ~= FInt xB = xA == xB
    FFloat xA ~= FFloat xB = xA == xB
    FText xA ~= FText xB = xA == xB
    FBlob xA ~= FBlob xB = xA == xB
    _ ~= _ = False

data Field a where
    Field :: IsValue a => TupleT PrimField (Prims a) -> Field a

type family MapPrims ts where
    MapPrims '[] = '[]
    MapPrims (a ': as) = ListCat (Prims a) (MapPrims as)

primFields :: TupleT Field ts -> TupleT PrimField (MapPrims ts)
primFields E = E
primFields (Field fs :* rest) = joinTuple fs $ primFields rest

decode :: TupleT Field ts -> TupleT PrimValue (MapPrims ts) -> TupleT Value ts
decode E E = E
decode (Field _ :* frest) row = primDecode row $ \x vrest -> Value x :* decode frest vrest

encode :: TupleT Value ts -> TupleT PrimValue (MapPrims ts)
encode E = E
encode (Value x :* rest) = primEncode x $ encode rest

data Condition = forall ts. Condition String (TupleT Value ts)

instance Show Condition where
    showsPrec d (Condition str vals) = showParen (d > 10) $
        showString "Condition " . showsPrec 11 str . showString " " . showsPrec 11 (encode vals)

instance Eq Condition where
    Condition sA tA == Condition sB tB = sA == sB && encode tA ~= encode tB

data RowRange = RowRange Int64 Int64
deriving instance Show RowRange
deriving instance Eq RowRange

data ColumnConstraint a where
    CPrimaryKey :: ColumnConstraint a
    CIntegerId :: ColumnConstraint Int64
    CIntegerSalt :: ColumnConstraint Int64
deriving instance Show (ColumnConstraint a)
instance HEq1 ColumnConstraint where
    CPrimaryKey ~= CPrimaryKey = True
    CIntegerId ~= CIntegerId = True
    CIntegerSalt ~= CIntegerSalt = True
    _ ~= _ = False

data ColumnDecl = forall a. ColumnDecl (PrimField a) [ColumnConstraint a]
deriving instance Show ColumnDecl
instance Eq ColumnDecl where
    ColumnDecl fA cA == ColumnDecl fB cB =
        fA ~= fB && setEq cA cB
      where
        setEq [] [] = True
        setEq (x:xs) ys = case setSub x ys of
            Just zs -> setEq xs zs
            Nothing -> False
        setSub x [] = Nothing
        setSub x (y:ys)
            | x ~= y = Just ys
            | otherwise = (y:) <$> setSub x ys

data Query result where
    CreateTable :: TableName -> [ColumnDecl] -> [ConstraintDecl] -> Query ()
    AddTableColumn :: TableName -> ColumnDecl -> Query ()
    DropTable :: TableName -> Query ()
    Select :: TableName -> TupleT Field rs -> Maybe Condition -> Maybe RowOrder -> Maybe RowRange -> Query [TupleT Value rs]
    Insert :: TableName -> TupleT Field vs -> [TupleT Value vs] -> Query ()
    InsertReturning :: TableName -> TupleT Field vs -> TupleT Value vs -> TupleT Field rs -> Query (TupleT Value rs)
    Update :: TableName -> TupleT Field vs -> TupleT Value vs -> Maybe Condition -> Query ()
    Delete :: TableName -> Maybe Condition -> Query ()

instance Show (Query ts) where
    showsPrec d (CreateTable table columns constr) = showParen (d > 10)
        $ showString "CreateTable " . showsPrec 11 table
        . showString " " . showsPrec 11 columns
        . showString " " . showsPrec 11 constr
    showsPrec d (AddTableColumn table column) = showParen (d > 10)
        $ showString "AddTableColumn " . showsPrec 11 table
        . showString " " . showsPrec 11 column
    showsPrec d (DropTable table) = showParen (d > 10)
        $ showString "DropTable " . showsPrec 11 table
    showsPrec d (Select table fields cond order range) = showParen (d > 10)
        $ showString "Select " . showsPrec 11 table
        . showString " " . showsPrec 11 (primFields fields)
        . showString " " . showsPrec 11 cond
        . showString " " . showsPrec 11 order
        . showString " " . showsPrec 11 range
    showsPrec d (Insert table fields rows) = showParen (d > 10)
        $ showString "Insert " . showsPrec 11 table
        . showString " " . showsPrec 11 (primFields fields)
        . showString " " . showsPrec 11 (map encode rows)
    showsPrec d (InsertReturning table fields values rets) = showParen (d > 10)
        $ showString "InsertReturning " . showsPrec 11 table
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
        tableA == tableB && columnsA == columnsB && constrA == constrB
    AddTableColumn tableA columnA ~= AddTableColumn tableB columnB =
        tableA == tableB && columnA == columnB
    DropTable tableA ~= DropTable tableB =
        tableA == tableB
    Select tableA fieldsA condA orderA rangeA ~= Select tableB fieldsB condB orderB rangeB =
        tableA == tableB && primFields fieldsA ~= primFields fieldsB && condA == condB && orderA == orderB && rangeA == rangeB
    Insert tableA fieldsA rowsA ~= Insert tableB fieldsB rowsB =
        tableA == tableB && primFields fieldsA ~= primFields fieldsB && rowsEq rowsA rowsB
    InsertReturning tableA fieldsA valuesA retsA ~= InsertReturning tableB fieldsB valuesB retsB =
        tableA == tableB && primFields fieldsA ~= primFields fieldsB && encode valuesA ~= encode valuesB && primFields retsA ~= primFields retsB
    Update tableA fieldsA valuesA condA ~= Update tableB fieldsB valuesB condB =
        tableA == tableB && primFields fieldsA ~= primFields fieldsB && encode valuesA ~= encode valuesB && condA == condB
    Delete tableA condA ~= Delete tableB condB =
        tableA == tableB && condA == condB
    _ ~= _ = False

rowsEq :: [TupleT Value ts] -> [TupleT Value us] -> Bool
rowsEq (x:xs) (y:ys) = encode x ~= encode y && rowsEq xs ys
rowsEq [] [] = True
rowsEq _ _ = False

fieldName :: PrimField a -> String
fieldName (FInt name) = name
fieldName (FFloat name) = name
fieldName (FText name) = name
fieldName (FBlob name) = name

instance IsValue Int64 where
    type Prims Int64 = '[Int64]
    primDecode (VInt x :* rest) cont = cont (Just x) rest
    primDecode (_ :* rest) cont = cont Nothing rest
    primEncode (Just x) rest = VInt x :* rest
    primEncode Nothing rest = VNull :* rest

fInt :: FieldName -> Field Int64
fInt a = Field (FInt a :* E)

instance IsValue Double where
    type Prims Double = '[Double]
    primDecode (VFloat x :* rest) cont = cont (Just x) rest
    primDecode (_ :* rest) cont = cont Nothing rest
    primEncode (Just x) rest = VFloat x :* rest
    primEncode Nothing rest = VNull :* rest

fFloat :: FieldName -> Field Double
fFloat a = Field (FFloat a :* E)

instance IsValue Text.Text where
    type Prims Text.Text = '[Text.Text]
    primDecode (VText x :* rest) cont = cont (Just x) rest
    primDecode (_ :* rest) cont = cont Nothing rest
    primEncode (Just x) rest = VText x :* rest
    primEncode Nothing rest = VNull :* rest

fText :: FieldName -> Field Text.Text
fText a = Field (FText a :* E)

instance IsValue String where
    type Prims String = '[Text.Text]
    primDecode (VText x :* rest) cont = cont (Just (Text.unpack x)) rest
    primDecode (_ :* rest) cont = cont Nothing rest
    primEncode (Just x) rest = VText (Text.pack x) :* rest
    primEncode Nothing rest = VNull :* rest

fString :: FieldName -> Field String
fString a = Field (FText a :* E)

instance IsValue BS.ByteString where
    type Prims BS.ByteString = '[BS.ByteString]
    primDecode (VBlob x :* rest) cont = cont (Just x) rest
    primDecode (_ :* rest) cont = cont Nothing rest
    primEncode (Just x) rest = VBlob x :* rest
    primEncode Nothing rest = VNull :* rest

fBlob :: FieldName -> Field BS.ByteString
fBlob a = Field (FBlob a :* E)
