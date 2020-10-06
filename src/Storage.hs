{-# LANGUAGE StandaloneDeriving #-}

module Storage
    ( ObjectTag(..)
    , Reference
    , User(..)
    , RefBox(..)
    , InitFailure(..)
    , Handle(..)
    , withStorage
    , currentSchema
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
    , enumObjects :: forall a. Integer -> Integer -> IO [RefBox a]
    }

withStorage :: Logger.Handle -> Db.Handle -> (InitFailure -> IO r) -> (Handle -> IO r) -> IO r
withStorage logger db onFail onSuccess = do
    matchCurrentSchema logger db onFail $ do
        onSuccess $ Handle
            { spawnObject = undefined
            , getObject = undefined
            , setObject = undefined
            , deleteObject = undefined
            , enumObjects = undefined
            }
