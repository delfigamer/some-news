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
    , Value(..)
    , MapPrims
    , primFields
    , Condition(..)
    , RowRange(..)
    , RowOrder(..)
    , ForeignKeyResolution(..)
    , ColumnConstraint(..)
    , ColumnDecl(..)
    , TableConstraint(..)
    , RowSource(..)
    , Query(..)
    , showPrimFields
    , showPrimValues
    , primFieldName
    , showBlob
    , fInt
    , fBool
    , fReal
    , fText
    , fString
    , fBlob
    , fTime
    ) where

import Data.Functor.Identity
import Data.Int
import Data.List
import Data.Proxy
import Data.String
import Data.Time.Clock
import qualified Data.ByteString as BS
import qualified Data.ByteString.Char8 as BSChar
import qualified Data.Text as Text
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
    | TBool
    | TText
    | TBlob
    | TTime

class IsPrimType a where
    matchPrimType
        :: proxy a
        -> (a ~ 'TInt => r)
        -> (a ~ 'TReal => r)
        -> (a ~ 'TBool => r)
        -> (a ~ 'TText => r)
        -> (a ~ 'TBlob => r)
        -> (a ~ 'TTime => r)
        -> r

instance IsPrimType 'TInt where
    matchPrimType _ onInt _onReal _onBool _onText _onBlob _onTime = onInt

instance IsPrimType 'TReal where
    matchPrimType _ _onInt onReal _onBool _onText _onBlob _onTime = onReal

instance IsPrimType 'TBool where
    matchPrimType _ _onInt _onReal onBool _onText _onBlob _onTime = onBool

instance IsPrimType 'TText where
    matchPrimType _ _onInt _onReal _onBool onText _onBlob _onTime = onText

instance IsPrimType 'TBlob where
    matchPrimType _ _onInt _onReal _onBool _onText onBlob _onTime = onBlob

instance IsPrimType 'TTime where
    matchPrimType _ _onInt _onReal _onBool _onText _onBlob onTime = onTime

class All IsPrimType (Prims a) => IsValue a where
    type Prims a :: [PrimType]
    primDecode :: HList PrimValue (Prims a) -> Maybe a
    primEncode :: a -> HList PrimValue (Prims a)

primProxies :: IsValue a => proxy a -> HList Proxy (Prims a)
primProxies _ = proxyHList Proxy

data PrimValue a where
    VInt :: !Int64 -> PrimValue 'TInt
    VReal :: !Double -> PrimValue 'TReal
    VBool :: !Bool -> PrimValue 'TBool
    VText :: !Text.Text -> PrimValue 'TText
    VBlob :: !BS.ByteString -> PrimValue 'TBlob
    VTime :: !UTCTime -> PrimValue 'TTime
    VTPosInf :: PrimValue 'TTime
    VTNegInf :: PrimValue 'TTime
    VNull :: PrimValue a
instance Show (PrimValue a) where
    showsPrec d (VInt x) = showParen (d > 10) $ showString "VInt " . showsPrec 10 x
    showsPrec d (VReal x) = showParen (d > 10) $ showString "VReal " . showsPrec 10 x
    showsPrec d (VBool x) = showParen (d > 10) $ showString "VBool " . showsPrec 10 x
    showsPrec d (VText x) = showParen (d > 10) $ showString "VText " . showsPrec 10 x
    showsPrec d (VBlob x) = showParen (d > 10) $ showString "VBlob " . showBlob "x" x
    showsPrec d (VTime x) = showParen (d > 10) $ showString "VInt " . showsPrec 10 x
    showsPrec _ VTPosInf = showString "VTPosInf"
    showsPrec _ VTNegInf = showString "VTNegInf"
    showsPrec _ VNull = showString "VNull"

data PrimField a where
    FInt :: FieldName -> PrimField 'TInt
    FReal :: FieldName -> PrimField 'TReal
    FBool :: FieldName -> PrimField 'TBool
    FText :: FieldName -> PrimField 'TText
    FBlob :: FieldName -> PrimField 'TBlob
    FTime :: FieldName -> PrimField 'TTime
deriving instance Show (PrimField a)

data Field a where
    Field :: IsValue a => HList PrimField (Prims a) -> Field a
deriving instance AllWith PrimField Show (Prims a) => Show (Field a)

data Value a where
    Value :: IsValue a => a -> Value a
    Null :: IsValue a => Value a
    Subquery :: IsValue a => Query [HList Maybe '[a]] -> Value a
deriving instance Show a => Show (Value a)

type family MapPrims ts where
    MapPrims '[] = '[]
    MapPrims (a ': as) = ListCat (Prims a) (MapPrims as)

primFields :: HList Field ts -> HList PrimField (MapPrims ts)
primFields E = E
primFields (Field fs :/ rest) = fs ++/ primFields rest

data Condition
    = Where String
    | WhereFieldIs String String
    | forall a. (Show a, IsValue a) => WhereIs a String
    | forall a. (Show a, IsValue a) => WhereWith a String
    | forall a. (Show a, IsValue a) => WhereWithList String [a] String
deriving instance Show Condition

data RowRange
    = RowRange Int64 Int64
    | AllRows
deriving instance Show RowRange

data RowOrder
    = Asc FieldName
    | Desc FieldName
deriving instance Show RowOrder

data ForeignKeyResolution
    = FKRNoAction
    | FKRRestrict
    | FKRCascade
    | FKRSetNull
    | FKRSetDefault
deriving instance Show ForeignKeyResolution

data ColumnConstraint (a :: PrimType) where
    CCPrimaryKey :: ColumnConstraint a
    CCNotNull :: ColumnConstraint a
    CCCheck :: String -> ColumnConstraint a
    CCReferences :: TableName -> FieldName -> ForeignKeyResolution -> ForeignKeyResolution -> ColumnConstraint a
deriving instance Show (ColumnConstraint a)

data ColumnDecl = forall a. ColumnDecl (PrimField a) [ColumnConstraint a]
deriving instance Show ColumnDecl

data TableConstraint = TCPrimaryKey [FieldName]
deriving instance Show TableConstraint

data RowSource
    = TableSource TableName
    | OuterJoinSource TableName Condition
    | forall rs. AllWith Field Show rs => RecursiveSource TableName (HList Field rs) (Select rs) (Select rs)
deriving instance Show RowSource

instance IsString RowSource where
    fromString = TableSource . TableName

type Select rs = Query [HList Maybe rs]

data Query result where
    CreateTable :: TableName -> [ColumnDecl] -> [TableConstraint] -> Query ()
    CreateIndex :: IndexName -> TableName -> [RowOrder] -> Query ()
    DropTable :: TableName -> Query ()
    Select :: All IsValue rs => [RowSource] -> HList Field rs -> [Condition] -> [RowOrder] -> RowRange -> Select rs
    Insert :: (AllWith Value Show vs, All IsValue rs) => TableName -> HList Field vs -> HList Value vs -> HList Field rs -> Query (Maybe (HList Maybe rs))
    Update :: AllWith Value Show vs => TableName -> HList Field vs -> HList Value vs -> [Condition] -> Query Int64
    Delete :: TableName -> [Condition] -> Query Int64

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
    showsPrec d (Insert table fields values rets) = showParen (d > 10)
        $ showString "Insert " . showsPrec 11 table
        . showString " " . showPrimFields 11 (primFields fields)
        . showString " " . showsPrec 11 values
        . showString " " . showPrimFields 11 (primFields rets)
    showsPrec d (Update table fields values cond) = showParen (d > 10)
        $ showString "Update " . showsPrec 11 table
        . showString " " . showPrimFields 11 (primFields fields)
        . showString " " . showsPrec 11 values
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

primFieldName :: PrimField a -> String
primFieldName (FInt (FieldName name)) = name
primFieldName (FReal (FieldName name)) = name
primFieldName (FBool (FieldName name)) = name
primFieldName (FText (FieldName name)) = name
primFieldName (FBlob (FieldName name)) = name
primFieldName (FTime (FieldName name)) = name

showBlob :: String -> BS.ByteString -> ShowS
showBlob m bs = showString $ "[" ++ m ++ "|" ++ inner ++ "|]"
  where
    inner = do
        c <- BS.unpack bs
        [hchar (c `div` 16), hchar (c `mod` 16)]
    hchar n = BSChar.index "0123456789abcdef" $ fromIntegral n

instance IsValue Int64 where
    type Prims Int64 = '[ 'TInt ]
    primDecode (VInt x :/ E) = Just x
    primDecode _ = Nothing
    primEncode x = VInt x :/ E

fInt :: FieldName -> Field Int64
fInt a = Field (FInt a :/ E)

instance IsValue Bool where
    type Prims Bool = '[ 'TBool ]
    primDecode (VBool x :/ E) = Just x
    primDecode _ = Nothing
    primEncode True = VBool True :/ E
    primEncode False = VBool False :/ E

fBool :: FieldName -> Field Bool
fBool a = Field (FBool a :/ E)

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
