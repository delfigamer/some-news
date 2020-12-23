{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE PatternSynonyms #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE StandaloneDeriving #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE UndecidableInstances #-}

module SN.Sql.Query
    ( TableName(..)
    , IndexName(..)
    , FieldName(..)
    , IsValue(..)
    , PrimValue(..)
    , PrimField(..)
    , Field(..)
    , Value(..)
    , Condition(..)
    , RowRange(..)
    , RowOrder(..)
    , ForeignKeyResolution(..)
    , ColumnConstraint(..)
    , ColumnDecl(..)
    , TableConstraint(..)
    , RowSource(..)
    , Query(..)
    , selectFields
    , primFieldName
    , fInt
    , fBool
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
import SN.Data.Base64
import SN.Data.HList

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

data PrimValue a where
    VInt :: !Int64 -> PrimValue Int64
    VBool :: !Bool -> PrimValue Bool
    VText :: !Text.Text -> PrimValue Text.Text
    VBlob :: !BS.ByteString -> PrimValue BS.ByteString
    VTime :: !UTCTime -> PrimValue UTCTime
    VTPosInf :: PrimValue UTCTime
    VTNegInf :: PrimValue UTCTime
    VNull :: PrimValue a

instance Show (PrimValue a) where
    showsPrec d (VInt x) = showParen (d > 10) $ showString "VInt " . showsPrec 11 x
    showsPrec d (VBool x) = showParen (d > 10) $ showString "VBool " . showsPrec 11 x
    showsPrec d (VText x) = showParen (d > 10) $ showString "VText " . showsPrec 11 x
    showsPrec d (VBlob x) = showParen (d > 10) $ showString "VBlob " . showBase64 11 x
    showsPrec d (VTime x) = showParen (d > 10) $ showString "VInt " . showsPrec 11 x
    showsPrec _ VTPosInf = showString "VTPosInf"
    showsPrec _ VTNegInf = showString "VTNegInf"
    showsPrec _ VNull = showString "VNull"

data PrimField a where
    FInt :: FieldName -> PrimField Int64
    FBool :: FieldName -> PrimField Bool
    FText :: FieldName -> PrimField Text.Text
    FBlob :: FieldName -> PrimField BS.ByteString
    FTime :: FieldName -> PrimField UTCTime
deriving instance Show (PrimField a)

class IsValue a where
    type PrimOf a :: *
    fromPrim :: PrimValue (PrimOf a) -> Maybe a
    toPrim :: a -> PrimValue (PrimOf a)

data Field a where
    Field :: IsValue a => PrimField (PrimOf a) -> Field a

data Value a where
    Value :: IsValue a => a -> Value a
    Null :: IsValue a => Value a

data Condition
    = Where String
    | WhereFieldIs String String
    | forall a. (Show a, IsValue a) => WhereIs a String
    | forall a. (Show a, IsValue a) => WhereWith a String
    | forall a. (Show a, IsValue a) => WhereWithList String [a] String
deriving instance Show Condition

data RowRange = RowRange !Int64 !Int64
deriving instance Show RowRange

data RowOrder
    = Asc !FieldName
    | Desc !FieldName
deriving instance Show RowOrder

data ForeignKeyResolution
    = FKRNoAction
    | FKRRestrict
    | FKRCascade
    | FKRSetNull
    | FKRSetDefault
deriving instance Show ForeignKeyResolution

data ColumnConstraint a where
    CCPrimaryKey :: ColumnConstraint a
    CCNotNull :: ColumnConstraint a
    CCCheck :: !String -> ColumnConstraint a
    CCReferences :: !TableName -> !FieldName -> !ForeignKeyResolution -> !ForeignKeyResolution -> ColumnConstraint a
deriving instance Show (ColumnConstraint a)

data ColumnDecl = forall a. ColumnDecl (PrimField a) [ColumnConstraint a]
deriving instance Show ColumnDecl

data TableConstraint = TCPrimaryKey [FieldName]
deriving instance Show TableConstraint

data RowSource
    = TableSource TableName
    | OuterJoinSource TableName Condition
    | forall rs. RecursiveSource TableName (HList Field rs) (Select rs) (Select rs)

instance Show RowSource where
    showsPrec d (TableSource (TableName tn)) = showsPrec d tn
    showsPrec d (OuterJoinSource (TableName tn) cond) = showParen (d > 10)
        $ showString "OuterJoinSource " . showsPrec 11 tn
        . showString " " . showsPrec 11 cond
    showsPrec d (RecursiveSource (TableName tn) fields q1 q2) = showParen (d > 10)
        $ showString "RecursiveSource " . showsPrec 11 tn
        . showString " " . showFieldHList fields
        . showString " " . showsPrec 11 q1
        . showString " " . showsPrec 11 q2

instance IsString RowSource where
    fromString = TableSource . TableName

type Select rs = Query [HList Maybe rs]

data Query result where
    CreateTable :: TableName -> [ColumnDecl] -> [TableConstraint] -> Query ()
    CreateIndex :: IndexName -> TableName -> [RowOrder] -> Query ()
    DropTable :: TableName -> Query ()
    Select :: [RowSource] -> HList Field rs -> [Condition] -> [RowOrder] -> RowRange -> Select rs
    Insert :: TableName -> HList Field vs -> HList Value vs -> Query Bool
    Update :: TableName -> HList Field vs -> HList Value vs -> [Condition] -> Query Int64
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
        . showString " " . showFieldHList fields
        . showString " " . showsPrec 11 cond
        . showString " " . showsPrec 11 order
        . showString " " . showsPrec 11 range
    showsPrec d (Insert table fields values) = showParen (d > 10)
        $ showString "Insert " . showsPrec 11 table
        . showString " " . showFieldHList fields
        . showString " " . showValueHList values
    showsPrec d (Update table fields values cond) = showParen (d > 10)
        $ showString "Update " . showsPrec 11 table
        . showString " " . showFieldHList fields
        . showString " " . showValueHList values
        . showString " " . showsPrec 11 cond
    showsPrec d (Delete table cond) = showParen (d > 10)
        $ showString "Delete " . showsPrec 11 table
        . showString " " . showsPrec 11 cond

selectFields :: Select rs -> HList Field rs
selectFields (Select _ fields _ _ _) = fields

showFieldHList :: HList Field ts -> ShowS
showFieldHList hs = showParen True $ hfoldr iter (showString "E") hs
  where
    iter (Field x) rest = showsPrec 7 x . showString " :/ " . rest

showValueHList :: HList Value ts -> ShowS
showValueHList hs = showParen True $ hfoldr iter (showString "E") hs
  where
    iter (Value x) rest = showsPrec 7 (toPrim x) . showString " :/ " . rest
    iter Null rest = showString "Null :/ " . rest

primFieldName :: PrimField a -> String
primFieldName (FInt (FieldName name)) = name
primFieldName (FBool (FieldName name)) = name
primFieldName (FText (FieldName name)) = name
primFieldName (FBlob (FieldName name)) = name
primFieldName (FTime (FieldName name)) = name

instance IsValue (PrimValue a) where
    type PrimOf (PrimValue a) = a
    fromPrim = Just
    toPrim = id

instance IsValue Int64 where
    type PrimOf Int64 = Int64
    fromPrim (VInt x) = Just x
    fromPrim _ = Nothing
    toPrim = VInt

fInt :: FieldName -> Field Int64
fInt = Field . FInt

instance IsValue Bool where
    type PrimOf Bool = Bool
    fromPrim (VBool x) = Just x
    fromPrim _ = Nothing
    toPrim = VBool

fBool :: FieldName -> Field Bool
fBool = Field . FBool

instance IsValue Text.Text where
    type PrimOf Text.Text = Text.Text
    fromPrim (VText x) = Just x
    fromPrim _ = Nothing
    toPrim = VText

fText :: FieldName -> Field Text.Text
fText = Field . FText

instance IsValue String where
    type PrimOf String = Text.Text
    fromPrim (VText x) = Just $ Text.unpack x
    fromPrim _ = Nothing
    toPrim = VText . Text.pack

fString :: FieldName -> Field String
fString = Field . FText

instance IsValue BS.ByteString where
    type PrimOf BS.ByteString = BS.ByteString
    fromPrim (VBlob x) = Just x
    fromPrim _ = Nothing
    toPrim = VBlob

fBlob :: FieldName -> Field BS.ByteString
fBlob = Field . FBlob

instance IsValue UTCTime where
    type PrimOf UTCTime = UTCTime
    fromPrim (VTime x) = Just x
    fromPrim _ = Nothing
    toPrim = VTime

fTime :: FieldName -> Field UTCTime
fTime = Field . FTime
