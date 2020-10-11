{-# LANGUAGE StandaloneDeriving #-}
{-# LANGUAGE TypeFamilies #-}

module Storage
    ( Reference(..)
    , User(..)
    , InitFailure(..)
    , Handle(..)
    , withSqlStorage
    , currentSchema
    , upgradeSchema
    ) where

import Control.Exception
import Control.Monad
import Data.Int
import Data.Maybe
import qualified Data.Text as Text
import qualified Logger
import Sql.Query
import qualified Sql.Database as Db
import Storage.Schema
import Tuple

data Reference a
    = ReferenceSalted !Int64 !Int64
    | ReferenceAny !Int64
    deriving (Show, Eq)

data User = User
    { userName :: !Text.Text
    , userSurname :: !Text.Text
    }
    deriving (Show, Eq)

data Handle = Handle
    { spawnUser :: User -> IO (Maybe (Reference User))
    , getUser :: Reference User -> IO (Maybe (Reference User, User))
    , setUser :: Reference User -> User -> IO (Maybe ())
    , deleteUser :: Reference User -> IO (Maybe ())
    , listUsers :: Int64 -> Int64 -> IO [(Reference User, User)]
    }

withSqlStorage :: Logger.Handle -> Db.Handle -> (InitFailure -> IO r) -> (Handle -> IO r) -> IO r
withSqlStorage logger db onFail onSuccess = do
    matchCurrentSchema logger db onFail $ onSuccess $ Handle
        { spawnUser = \user -> do
            mret <- Db.queryMaybe db $
                InsertReturning "sn_users" (fUser :* E) (Val user :* E) (fUserRef :* E)
            case mret of
                Just (Val ref :* E) -> return $ Just $ ref
                _ -> return Nothing
        , getUser = \ref -> do
            mret <- Db.queryMaybe db $
                Select "sn_users" (fUserRef :* fUser :* E)
                    (referenceCondition fUserRef ref)
                    Nothing
                    (Just $ RowRange 0 1)
            case mret of
                Just [Val ref :* Val user :* E] -> return $ Just $ (ref, user)
                _ -> return Nothing
        , setUser = \ref user -> do
            Db.queryMaybe db $
                Update "sn_users" (fUser :* E) (Val user :* E)
                    (referenceCondition fUserRef ref)
        , deleteUser = \ref -> do
            Db.queryMaybe db $
                Delete "sn_users"
                    (referenceCondition fUserRef ref)
        , listUsers = \offset limit -> do
            mret <- Db.queryMaybe db $
                Select "sn_users" (fUserRef :* fUser :* E)
                    Nothing
                    (Just "user_id")
                    (Just (RowRange offset limit))
            case mret of
                Just rets -> return $ mapMaybe
                    (\row -> do
                        Val ref :* Val user :* E <- Just row
                        Just (ref, user))
                    rets
                Nothing -> return []
        }

referenceCondition :: Field (Reference a) -> Reference a -> Maybe Condition
referenceCondition (Field (FInt oidName :* FInt saltName :* E)) reference =
    case reference of
        ReferenceSalted oid salt ->
            Just $ Condition
                (oidName ++ " = ? AND " ++ saltName ++ " = ?")
                (Val oid :* Val salt :* E)
        ReferenceAny oid ->
            Just $ Condition
                (oidName ++ " = ?")
                (Val oid :* E)

instance IsValue (Reference a) where
    type Prims (Reference a) = '[Int64, Int64]
    primDecode (VInt oid :* VInt salt :* rest) cont = cont (Just (ReferenceSalted oid salt)) rest
    primDecode (VInt oid :* _ :* rest) cont = cont (Just (ReferenceAny oid)) rest
    primDecode (_ :* _ :* rest) cont = cont Nothing rest
    primEncode (Just (ReferenceSalted oid salt)) rest = VInt oid :* VInt salt :* rest
    primEncode (Just (ReferenceAny oid)) rest = VInt oid :* VNull :* rest
    primEncode Nothing rest = VNull :* VNull :* rest

fReference :: FieldName -> FieldName -> Field (Reference a)
fReference oidName saltName = Field (FInt oidName :* FInt saltName :* E)

instance IsValue User where
    type Prims User = '[Text.Text, Text.Text]
    primDecode (VText name :* VText surname :* rest) cont = cont (Just (User name surname)) rest
    primDecode (_ :* _ :* rest) cont = cont Nothing rest
    primEncode (Just (User name surname)) rest = VText name :* VText surname :* rest
    primEncode Nothing rest = VNull :* VNull :* rest

fUser :: Field User
fUser = Field (FText "user_name" :* FText "user_surname" :* E)

fUserRef :: Field (Reference User)
fUserRef = fReference "user_id" "user_salt"
