{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE PatternSynonyms #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE StandaloneDeriving #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE UndecidableInstances #-}

module Sql.Query
    ( TableName(..)
    , IndexName(..)
    , FieldName(..)
    , IsValue(..)
    , PrimType(..)
    , IsPrimType(..)
    , primProxies
    , PrimValue(..)
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
    , showPrimFields
    , showPrimValues
    , primFieldName
    , fInt
    , fReal
    , fText
    , fString
    , fBlob
    , fTime
    ) where

import qualified Data.ByteString as BS
import Data.Functor.Identity
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

data PrimType
    = TInt
    | TReal
    | TText
    | TBlob
    | TTime

class IsPrimType a where
    matchPrimType
        :: proxy a
        -> (a ~ 'TInt => r)
        -> (a ~ 'TReal => r)
        -> (a ~ 'TText => r)
        -> (a ~ 'TBlob => r)
        -> (a ~ 'TTime => r)
        -> r

instance IsPrimType 'TInt where
    matchPrimType _ onInt _onReal _onText _onBlob _onTime = onInt

instance IsPrimType 'TReal where
    matchPrimType _ _onInt onReal _onText _onBlob _onTime = onReal

instance IsPrimType 'TText where
    matchPrimType _ _onInt _onReal onText _onBlob _onTime = onText

instance IsPrimType 'TBlob where
    matchPrimType _ _onInt _onReal _onText onBlob _onTime = onBlob

instance IsPrimType 'TTime where
    matchPrimType _ _onInt _onReal _onText _onBlob onTime = onTime

class All IsPrimType (Prims a) => IsValue a where
    type Prims a :: [PrimType]
    primDecode :: HList PrimValue (Prims a) -> Maybe a
    primEncode :: a -> HList PrimValue (Prims a)

primProxies :: IsValue a => proxy a -> HList Proxy (Prims a)
primProxies _ = proxyHList Proxy

data PrimValue a where
    VInt :: !Int64 -> PrimValue 'TInt
    VReal :: !Double -> PrimValue 'TReal
    VText :: !Text.Text -> PrimValue 'TText
    VBlob :: !BS.ByteString -> PrimValue 'TBlob
    VTime :: !UTCTime -> PrimValue 'TTime
    VNull :: PrimValue a
deriving instance Show (PrimValue a)

-- instance HEq1 PrimValue where
    -- VInt xA ~= VInt xB = xA == xB
    -- VFloat xA ~= VFloat xB = xA == xB
    -- VText xA ~= VText xB = xA == xB
    -- VBlob xA ~= VBlob xB = xA == xB
    -- VDateTime xA ~= VDateTime xB = xA == xB
    -- VNull ~= VNull = True
    -- _ ~= _ = False

data PrimField a where
    FInt :: FieldName -> PrimField 'TInt
    FReal :: FieldName -> PrimField 'TReal
    FText :: FieldName -> PrimField 'TText
    FBlob :: FieldName -> PrimField 'TBlob
    FTime :: FieldName -> PrimField 'TTime
deriving instance Show (PrimField a)

-- instance HEq1 PrimField where
    -- FInt xA ~= FInt xB = xA == xB
    -- FFloat xA ~= FFloat xB = xA == xB
    -- FText xA ~= FText xB = xA == xB
    -- FBlob xA ~= FBlob xB = xA == xB
    -- FDateTime xA ~= FDateTime xB = xA == xB
    -- _ ~= _ = False

data Field a where
    Field :: HList PrimField (Prims a) -> Field a

type family MapPrims ts where
    MapPrims '[] = '[]
    MapPrims (a ': as) = ListCat (Prims a) (MapPrims as)

primFields :: HList Field ts -> HList PrimField (MapPrims ts)
primFields E = E
primFields (Field fs :/ rest) = fs ++/ primFields rest

decode :: forall ts. All IsValue ts => HList PrimValue (MapPrims ts) -> HList Maybe ts
decode primValues = constrainHList (Proxy :: Proxy IsValue) (proxyHList (Proxy :: Proxy ts))
    E
    (\myProxy _ -> do
        let myPrimsProxyList = primProxies myProxy
        takeHList myPrimsProxyList primValues $ \myPrimValues otherPrimValues -> do
            primDecode myPrimValues :/ decode otherPrimValues)

encode :: All IsValue ts => HList Maybe ts -> HList PrimValue (MapPrims ts)
encode values = constrainHList (Proxy :: Proxy IsValue) values
    E
    (\me rest -> do
        let myPrims = case me of
                Just x -> primEncode x
                Nothing -> toNulls (primProxies me)
        myPrims ++/ encode rest)
  where
    toNulls :: HList f ts -> HList PrimValue ts
    toNulls E = E
    toNulls (_ :/ rest) = VNull :/ toNulls rest

data Where = forall ts. (All IsValue ts, AllWith Maybe Show ts) => Where String (HList Maybe ts)
deriving instance Show Where

-- instance Eq Where where
    -- Where sA tA == Where sB tB = sA == sB && encode tA ~= encode tB

data RowRange
    = RowRange Int64 Int64
    | AllRows
deriving instance Show RowRange
-- deriving instance Eq RowRange

data RowOrder
    = Asc FieldName
    | Desc FieldName
deriving instance Show RowOrder
-- deriving instance Eq RowOrder

data ForeignKeyResolution
    = FKRNoAction
    | FKRRestrict
    | FKRCascade
    | FKRSetNull
    | FKRSetDefault
deriving instance Show ForeignKeyResolution
-- deriving instance Eq ForeignKeyResolution

data ColumnConstraint (a :: PrimType) where
    CCPrimaryKey :: ColumnConstraint a
    CCNotNull :: ColumnConstraint a
    CCReferences :: TableName -> FieldName -> ForeignKeyResolution -> ForeignKeyResolution -> ColumnConstraint a
deriving instance Show (ColumnConstraint a)

-- instance HEq1 ColumnConstraint where
    -- CCPrimaryKey ~= CCPrimaryKey = True
    -- CCNotNull ~= CCNotNull = True
    -- CCReferences tableA fieldA onUpdateA onDeleteA ~= CCReferences tableB fieldB onUpdateB onDeleteB =
        -- tableA == tableB && fieldA == fieldB && onUpdateA == onUpdateB && onDeleteA == onDeleteB
    -- _ ~= _ = False

data ColumnDecl = forall a. ColumnDecl (PrimField a) [ColumnConstraint a]
deriving instance Show ColumnDecl

-- instance Eq ColumnDecl where
    -- ColumnDecl fA cA == ColumnDecl fB cB =
        -- fA ~= fB && setMatchBy (~=) cA cB

data TableConstraint = TCPrimaryKey [FieldName]
deriving instance Show TableConstraint
-- deriving instance Eq TableConstraint

data Query result where
    CreateTable :: TableName -> [ColumnDecl] -> [TableConstraint] -> Query ()
    CreateIndex :: IndexName -> TableName -> [RowOrder] -> Query ()
    DropTable :: TableName -> Query ()
    Select :: All IsValue rs => [TableName] -> HList Field rs -> [Where] -> [RowOrder] -> RowRange -> Query [HList Maybe rs]
    Insert_ :: All IsValue vs => TableName -> HList Field vs -> HList Maybe vs -> Query ()
    Insert :: (All IsValue vs, All IsValue rs) => TableName -> HList Field vs -> HList Maybe vs -> HList Field rs -> Query (HList Maybe rs)
    Update :: All IsValue vs => TableName -> HList Field vs -> HList Maybe vs -> [Where] -> Query ()
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
        . showString " " . showPrimFields 11 (primFields fields)
        . showString " " . showsPrec 11 cond
        . showString " " . showsPrec 11 order
        . showString " " . showsPrec 11 range
    showsPrec d (Insert_ table fields values) = showParen (d > 10)
        $ showString "Insert_ " . showsPrec 11 table
        . showString " " . showPrimFields 11 (primFields fields)
        . showString " " . showPrimValues 11 (encode values)
    showsPrec d (Insert table fields values rets) = showParen (d > 10)
        $ showString "Insert " . showsPrec 11 table
        . showString " " . showPrimFields 11 (primFields fields)
        . showString " " . showPrimValues 11 (encode values)
        . showString " " . showPrimFields 11 (primFields rets)
    showsPrec d (Update table fields values cond) = showParen (d > 10)
        $ showString "Update " . showsPrec 11 table
        . showString " " . showPrimFields 11 (primFields fields)
        . showString " " . showPrimValues 11 (encode values)
        . showString " " . showsPrec 11 cond
    showsPrec d (Delete table cond) = showParen (d > 10)
        $ showString "Delete " . showsPrec 11 table
        . showString " " . showsPrec 11 cond

showPrimFields :: Int -> HList PrimField ts -> ShowS
showPrimFields _ E = showString "E"
showPrimFields d (x :/ xs) = showParen (d > 6) $ showsPrec 7 x . showString " :/ " . showPrimFields 7 xs

showPrimValues :: Int -> HList PrimValue ts -> ShowS
showPrimValues _ E = showString "E"
showPrimValues d (x :/ xs) = showParen (d > 6) $ showsPrec 7 x . showString " :/ " . showPrimValues 7 xs

-- instance HEq1 Query where
    -- CreateTable tableA columnsA constrA ~= CreateTable tableB columnsB constrB =
        -- tableA == tableB && setMatchBy (==) columnsA columnsB && setMatchBy (==) constrA constrB
    -- CreateIndex indexA tableA orderA ~= CreateIndex indexB tableB orderB =
        -- indexA == indexB && tableA == tableB && orderA == orderB
    -- DropTable tableA ~= DropTable tableB =
        -- tableA == tableB
    -- Select tableListA fieldsA condA orderA rangeA ~= Select tableListB fieldsB condB orderB rangeB =
        -- tableListA == tableListB && primFields fieldsA ~= primFields fieldsB && condA == condB && orderA == orderB && rangeA == rangeB
    -- Insert tableA fieldsA valuesA retsA ~= Insert tableB fieldsB valuesB retsB =
        -- tableA == tableB && primFields fieldsA ~= primFields fieldsB && encode valuesA ~= encode valuesB && primFields retsA ~= primFields retsB
    -- Update tableA fieldsA valuesA condA ~= Update tableB fieldsB valuesB condB =
        -- tableA == tableB && primFields fieldsA ~= primFields fieldsB && encode valuesA ~= encode valuesB && condA == condB
    -- Delete tableA condA ~= Delete tableB condB =
        -- tableA == tableB && condA == condB
    -- _ ~= _ = False

-- setMatchBy :: (a -> b -> Bool) -> [a] -> [b] -> Bool
-- setMatchBy _ [] [] = True
-- setMatchBy test2 (x:xs) b = case setExcludeBy (test2 x) b of
    -- Just ys -> setMatchBy test2 xs ys
    -- Nothing -> False

-- setExcludeBy :: (b -> Bool) -> [b] -> Maybe [b]
-- setExcludeBy _ [] = Nothing
-- setExcludeBy test (y:ys)
    -- | test y = Just ys
    -- | otherwise = (y:) <$> setExcludeBy test ys

primFieldName :: PrimField a -> String
primFieldName (FInt (FieldName name)) = name
primFieldName (FReal (FieldName name)) = name
primFieldName (FText (FieldName name)) = name
primFieldName (FBlob (FieldName name)) = name
primFieldName (FTime (FieldName name)) = name

instance IsValue Int64 where
    type Prims Int64 = '[ 'TInt ]
    primDecode (VInt x :/ E) = Just x
    primDecode _ = Nothing
    primEncode x = VInt x :/ E

fInt :: FieldName -> Field Int64
fInt a = Field (FInt a :/ E)

instance IsValue Bool where
    type Prims Bool = '[ 'TInt ]
    primDecode (VInt x :/ E) = Just (x /= 0)
    primDecode _ = Just False
    primEncode True = VInt 1 :/ E
    primEncode False = VInt 0 :/ E

fBool :: FieldName -> Field Bool
fBool a = Field (FInt a :/ E)

instance IsValue Double where
    type Prims Double = '[ 'TReal ]
    primDecode (VReal x :/ E) = Just x
    primDecode _ = Nothing
    primEncode x = VReal x :/ E

fReal :: FieldName -> Field Double
fReal a = Field (FReal a :/ E)

instance IsValue Text.Text where
    type Prims Text.Text = '[ 'TText ]
    primDecode (VText x :/ E) = Just x
    primDecode _ = Nothing
    primEncode x = VText x :/ E

fText :: FieldName -> Field Text.Text
fText a = Field (FText a :/ E)

instance IsValue String where
    type Prims String = '[ 'TText ]
    primDecode (VText x :/ E) = Just $ Text.unpack x
    primDecode _ = Nothing
    primEncode x = VText (Text.pack x) :/ E

fString :: FieldName -> Field String
fString a = Field (FText a :/ E)

instance IsValue BS.ByteString where
    type Prims BS.ByteString = '[ 'TBlob ]
    primDecode (VBlob x :/ E) = Just x
    primDecode _ = Nothing
    primEncode x = VBlob x :/ E

fBlob :: FieldName -> Field BS.ByteString
fBlob a = Field (FBlob a :/ E)

instance IsValue UTCTime where
    type Prims UTCTime = '[ 'TTime ]
    primDecode (VTime x :/ E) = Just x
    primDecode _ = Nothing
    primEncode x = VTime x :/ E

fTime :: FieldName -> Field UTCTime
fTime a = Field (FTime a :/ E)
