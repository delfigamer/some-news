{-# LANGUAGE StandaloneDeriving #-}

module Sql.Query
    ( TableName
    , ConstraintDecl
    , FieldName
    , RowOrder
    , Field(..)
    , Value(..)
    , Condition(..)
    , RowRange(..)
    , ColumnDecl(..)
    , Query(..)
    , fieldName
    ) where

import Data.Int
import qualified Data.Text as Text
import Tuple

type TableName = String
type ConstraintDecl = String
type FieldName = String
type RowOrder = String

data Field a where
    FInt :: FieldName -> Field Int64
    FString :: FieldName -> Field String
    FText :: FieldName -> Field Text.Text
deriving instance Show (Field a)

instance HEq1 Field where
    FInt fA ~= FInt fB = fA == fB
    FString fA ~= FString fB = fA == fB
    FText fA ~= FText fB = fA == fB
    _ ~= _ = False

data Value a where
    VInt :: Int64 -> Value Int64
    VString :: String -> Value String
    VText :: Text.Text -> Value Text.Text
deriving instance Show (Value a)

instance HEq1 Value where
    VInt xA ~= VInt xB = xA == xB
    VString xA ~= VString xB = xA == xB
    VText xA ~= VText xB = xA == xB
    _ ~= _ = False

data Condition = forall ts. Condition String (TupleT Value ts)
deriving instance Show Condition

instance Eq Condition where
    Condition sA tA == Condition sB tB = sA == sB && tA ~= tB

data RowRange = RowRange Int64 Int64
deriving instance Show RowRange
deriving instance Eq RowRange

data ColumnDecl = forall a. ColumnDecl (Field a) String
deriving instance Show ColumnDecl
instance Eq ColumnDecl where
    ColumnDecl fA cA == ColumnDecl fB cB = fA ~= fB && cA == cB

data Query result where
    CreateTable :: TableName -> [ColumnDecl] -> [ConstraintDecl] -> Query ()
    AddTableColumn :: TableName -> ColumnDecl -> Query ()
    DropTable :: TableName -> Query ()
    Select :: TableName -> TupleT Field rs -> Maybe Condition -> Maybe RowOrder -> Maybe RowRange -> Query [TupleT Value rs]
    Insert :: TableName -> TupleT Field vs -> [TupleT Value vs] -> Query ()
    InsertReturning :: TableName -> TupleT Field vs -> TupleT Value vs -> TupleT Field rs -> Query (TupleT Value rs)
    Update :: TableName -> TupleT Field vs -> TupleT Value vs -> Maybe Condition -> Query ()
    Delete :: TableName -> Maybe Condition -> Query ()
deriving instance Show (Query ts)

instance HEq1 Query where
    CreateTable tableA columnsA constrA ~= CreateTable tableB columnsB constrB =
        tableA == tableB && columnsA == columnsB && constrA == constrB
    AddTableColumn tableA columnA ~= AddTableColumn tableB columnB =
        tableA == tableB && columnA == columnB
    DropTable tableA ~= DropTable tableB =
        tableA == tableB
    Select tableA fieldsA condA orderA rangeA ~= Select tableB fieldsB condB orderB rangeB =
        tableA == tableB && fieldsA ~= fieldsB && condA == condB && orderA == orderB && rangeA == rangeB
    Insert tableA fieldsA rowsA ~= Insert tableB fieldsB rowsB =
        tableA == tableB && fieldsA ~= fieldsB && rowsEq rowsA rowsB
    InsertReturning tableA fieldsA valuesA retA ~= InsertReturning tableB fieldsB valuesB retB =
        tableA == tableB && fieldsA ~= fieldsB && valuesA ~= valuesB && retA ~= retB
    Update tableA fieldsA valuesA condA ~= Update tableB fieldsB valuesB condB =
        tableA == tableB && fieldsA ~= fieldsB && valuesA ~= valuesB && condA == condB
    Delete tableA condA ~= Delete tableB condB =
        tableA == tableB && condA == condB
    _ ~= _ = False

rowsEq :: [TupleT Value ts] -> [TupleT Value us] -> Bool
rowsEq (x:xs) (y:ys) = x ~= y && rowsEq xs ys
rowsEq [] [] = True
rowsEq _ _ = False

fieldName :: Field a -> String
fieldName (FInt name) = name
fieldName (FString name) = name
fieldName (FText name) = name
