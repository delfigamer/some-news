{-# LANGUAGE StandaloneDeriving #-}
{-# LANGUAGE TypeFamilies #-}

module Storage
    ( Reference
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

data Reference a = Reference !Int64
    deriving (Show, Eq)

data User = User
    { userName :: !Text.Text
    , userSurname :: !Text.Text
    }
    deriving (Show, Eq)

data Handle = Handle
    { spawnUser :: User -> IO (Maybe (Reference User))
    , getUser :: Reference User -> IO (Maybe User)
    , setUser :: Reference User -> User -> IO (Maybe ())
    , deleteUser :: Reference User -> IO (Maybe ())
    , listUsers :: Int64 -> Int64 -> IO [(Reference User, User)]
    }

withSqlStorage :: Logger.Handle -> Db.Handle -> (InitFailure -> IO r) -> (Handle -> IO r) -> IO r
withSqlStorage logger db onFail onSuccess = do
    matchCurrentSchema logger db onFail $ onSuccess $ Handle
        { spawnUser = \user -> do
            mret <- Db.queryMaybe db $
                InsertReturning "sn_users" (fUser :* E) (Val user :* E) (fReference "user_id" :* E)
            case mret of
                Just (Val ref :* E) -> return $ Just $ ref
                _ -> return Nothing
        , getUser = \ref -> do
            mret <- Db.queryMaybe db $
                Select "sn_users" (fUser :* E)
                    (Just $ Condition ("user_id = ?") $ Val ref :* E)
                    Nothing
                    (Just $ RowRange 0 1)
            case mret of
                Just [Val user :* E] -> return $ Just $ user
                _ -> return Nothing
        , setUser = \ref user -> do
            Db.queryMaybe db $
                Update "sn_users" (fUser :* E) (Val user :* E)
                    (Just $ Condition ("user_id = ?") $ Val ref :* E)
        , deleteUser = \ref -> do
            Db.queryMaybe db $
                Delete "sn_users"
                    (Just $ Condition ("user_id = ?") $ Val ref :* E)
        , listUsers = \offset limit -> do
            mret <- Db.queryMaybe db $
                Select "sn_users" (fReference "user_id" :* fUser :* E)
                    Nothing
                    (Just "user_id")
                    (guard (offset >= 0 && limit > 0) >> Just (RowRange offset limit))
            case mret of
                Just rets -> return $ mapMaybe
                    (\row -> do
                        Val oid :* Val user :* E <- Just row
                        Just (oid, user))
                    rets
                Nothing -> return []
        }

instance IsValue (Reference a) where
    type Prims (Reference a) = '[Int64]
    primDecode (VInt oid :* rest) cont = cont (Just (Reference oid)) rest
    primDecode (_ :* rest) cont = cont Nothing rest
    primEncode (Just (Reference oid)) rest = VInt oid :* rest
    primEncode Nothing rest = VNull :* rest

fReference :: FieldName -> Field (Reference a)
fReference name = Field (FInt name :* E)

instance IsValue User where
    type Prims User = '[Text.Text, Text.Text]
    primDecode (VText name :* VText surname :* rest) cont = cont (Just (User name surname)) rest
    primDecode (_ :* _ :* rest) cont = cont Nothing rest
    primEncode (Just (User name surname)) rest = VText name :* VText surname :* rest
    primEncode Nothing rest = VNull :* VNull :* rest

fUser :: Field User
fUser = Field (FText "user_name" :* FText "user_surname" :* E)
