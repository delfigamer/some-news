{-# LANGUAGE StandaloneDeriving #-}
{-# LANGUAGE TypeFamilies #-}

module Storage
    ( ObjectTag(..)
    , Reference
    , User(..)
    , RefBox(..)
    , InitFailure(..)
    , Handle(..)
    , withSqlStorage
    , currentSchema
    , upgradeSchema
    ) where

import Control.Exception
import Data.Int
import qualified Data.Text as Text
import qualified Logger
import Sql.Query
import qualified Sql.Database as Db
import Storage.Schema
import Tuple

data ObjectTag a where
    UserTag :: ObjectTag User
deriving instance Show (ObjectTag a)
deriving instance Eq (ObjectTag a)

data Reference a = Reference !(ObjectTag a) !Int64
    deriving (Show, Eq)

data User = User
    { userName :: !Text.Text
    , userSurname :: !Text.Text
    }
    deriving (Show, Eq)

data RefBox a = RefBox !a !(Reference a)
    deriving (Show, Eq)

data Handle = Handle
    { spawnObject :: forall a. ObjectTag a -> a -> IO (Maybe (Reference a))
    , getObject :: forall a. Reference a -> IO (Maybe a)
    , setObject :: forall a. Reference a -> a -> IO (Maybe ())
    , deleteObject :: forall a. Reference a -> IO (Maybe ())
    , enumObjects :: forall a. ObjectTag a -> Int64 -> Int64 -> IO [RefBox a]
    }

withSqlStorage :: Logger.Handle -> Db.Handle -> (InitFailure -> IO r) -> (Handle -> IO r) -> IO r
withSqlStorage logger db onFail onSuccess = do
    matchCurrentSchema logger db onFail $ do
        onSuccess $ Handle
            { spawnObject = sqlSpawnObject logger db
            , getObject = sqlGetObject logger db
            , setObject = sqlSetObject logger db
            , deleteObject = sqlDeleteObject logger db
            , enumObjects = sqlEnumObjects logger db
            }

type family FieldsOf a where
    FieldsOf User = '[Text.Text, Text.Text]

objectTable :: ObjectTag a -> TableName
objectTable UserTag = "sn_users"

objectFields :: ObjectTag a -> TupleT Field (FieldsOf a)
objectFields UserTag = FText "user_name" :* FText "user_surname" :* E

objectToValues :: ObjectTag a -> a -> TupleT Value (FieldsOf a)
objectToValues UserTag (User name surname) = VText name :* VText surname :* E

objectFromValues :: ObjectTag a -> TupleT Value (FieldsOf a) -> a
objectFromValues UserTag (VText name :* VText surname :* E) = User name surname

referenceField :: ObjectTag a -> String
referenceField UserTag = "user_id"

referenceFromValues :: ObjectTag a -> TupleT Value '[Int64] -> Reference a
referenceFromValues objectTag (VInt objectId :* E) = Reference objectTag objectId

referenceCondition :: ObjectTag a -> Int64 -> Maybe Condition
referenceCondition objectTag objectId = (Just $ Condition (referenceField objectTag ++ " = ?") $ VInt objectId :* E)

refBoxFromValues :: ObjectTag a -> TupleT Value (Int64 ': FieldsOf a) -> RefBox a
refBoxFromValues objectTag (VInt objectId :* values) = RefBox (objectFromValues objectTag values) (Reference objectTag objectId)

sqlSpawnObject :: Logger.Handle -> Db.Handle -> ObjectTag a -> a -> IO (Maybe (Reference a))
sqlSpawnObject logger db objectTag object = do
    mret <- Db.queryMaybe db $
        InsertReturning
            (objectTable objectTag)
            (objectFields objectTag)
            (objectToValues objectTag object)
            (FInt (referenceField objectTag) :* E)
    case mret of
        Just vals -> return $ Just $ referenceFromValues objectTag vals
        Nothing -> return Nothing

sqlGetObject :: Logger.Handle -> Db.Handle -> Reference a -> IO (Maybe a)
sqlGetObject logger db (Reference objectTag objectId) = do
    mret <- Db.queryMaybe db $
        Select
            (objectTable objectTag)
            (objectFields objectTag)
            (referenceCondition objectTag objectId)
            Nothing
            Nothing
    case mret of
        Just (vals:_) -> return $ Just $ objectFromValues objectTag vals
        _ -> return Nothing

sqlSetObject :: Logger.Handle -> Db.Handle -> Reference a -> a -> IO (Maybe ())
sqlSetObject logger db (Reference objectTag objectId) object = do
    Db.queryMaybe db $
        Update
            (objectTable objectTag)
            (objectFields objectTag)
            (objectToValues objectTag object)
            (referenceCondition objectTag objectId)

sqlDeleteObject :: Logger.Handle -> Db.Handle -> Reference a -> IO (Maybe ())
sqlDeleteObject logger db (Reference objectTag objectId) = do
    Db.queryMaybe db $
        Delete
            (objectTable objectTag)
            (referenceCondition objectTag objectId)

sqlEnumObjects :: Logger.Handle -> Db.Handle -> ObjectTag a -> Int64 -> Int64 -> IO [RefBox a]
sqlEnumObjects logger db objectTag offset limit = do
    mret <- Db.queryMaybe db $
        Select
            (objectTable objectTag)
            (FInt (referenceField objectTag) :* objectFields objectTag)
            Nothing
            (Just $ referenceField objectTag)
            (Just $ RowRange offset limit)
    case mret of
        Just rets -> return $ map (refBoxFromValues objectTag) rets
        Nothing -> return []
