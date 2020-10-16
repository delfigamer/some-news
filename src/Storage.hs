{-# LANGUAGE StandaloneDeriving #-}
{-# LANGUAGE TypeFamilies #-}

module Storage
    ( Reference(..)
    , User(..)
    , AccessKey(..)
    , AccessKeyInfo(..)
    , Author(..)
    , PublicationStatus(..)
    , Article(..)
    , InitFailure(..)
    , Handle(..)
    , withSqlStorage
    , currentSchema
    , upgradeSchema
    , accessKeyId
    ) where

import Control.Exception
import Control.Monad
import qualified Crypto.Hash as CHash
import qualified Crypto.Random as CRand
import Data.Bits
import qualified Data.ByteArray as BA
import qualified Data.ByteString as BS
import Data.IORef
import Data.Int
import Data.Maybe
import Data.Proxy
import qualified Data.Text as Text
import Data.Time.Clock
import Data.Word
import GHC.Generics (Generic)
import qualified Logger
import Sql.Query
import qualified Sql.Database as Db
import Storage.Schema
import Tuple

data Reference a
    = Reference !BS.ByteString
    deriving (Show, Eq, Ord)

data User = User
    { userName :: !Text.Text
    , userSurname :: !Text.Text
    , userJoinDate :: !UTCTime
    , userIsAdmin :: !Bool
    }
    deriving (Show, Eq, Ord)

data AccessKey = AccessKey !BS.ByteString !BS.ByteString
    deriving (Show, Eq, Ord)

data AccessKeyInfo = AccessKeyInfo
    { accessKeyHash :: !BS.ByteString
    , accessKeyUser :: !(Reference User)
    }
    deriving (Show, Eq, Ord)

data Author = Author
    { authorName :: !Text.Text
    , authorDescription :: !Text.Text
    }
    deriving (Show, Eq, Ord)

data PublicationStatus
    = PublishAt !UTCTime
    | NonPublished
    deriving (Show, Eq, Ord)

data Article = Article
    { articleAuthor :: !(Reference Author)
    , articleName :: !Text.Text
    , articleText :: !Text.Text
    , articlePublicationStatus :: !PublicationStatus
    }
    deriving (Show, Eq, Ord)

data Handle = Handle
    { spawnUser :: User -> IO (Maybe (Reference User))
    , getUser :: Reference User -> IO (Maybe User)
    , setUser :: Reference User -> User -> IO (Maybe ())
    , deleteUser :: Reference User -> IO (Maybe ())
    , listUsers :: Int64 -> Int64 -> IO [(Reference User, User)]
    , spawnAccessKey :: Reference User -> IO (Maybe AccessKey)
    , lookupAccessKey :: AccessKey -> IO (Maybe (Reference User))
    , deleteAccessKey :: Reference AccessKeyInfo -> IO (Maybe ())
    , listAccessKeysOf :: Reference User -> IO (Maybe [Reference AccessKeyInfo])
    }

withSqlStorage :: Logger.Handle -> Db.Handle -> (InitFailure -> IO r) -> (Handle -> IO r) -> IO r
withSqlStorage logger db onFail onSuccess = do
    matchCurrentSchema logger db onFail $ do
        pgen <- newIORef =<< CRand.drgNew
        onSuccess $ Handle
            { spawnUser = \user -> do
                userRef <- generateRef pgen
                mret <- Db.queryMaybe db $
                    Insert_ "sn_users" (fReference "user_id" :/ fUser :/ E) (Just userRef :/ Just user :/ E)
                case mret of
                    Just () -> return $ Just $ userRef
                    _ -> return Nothing
            , getUser = \userRef -> do
                mret <- Db.queryMaybe db $
                    Select ["sn_users"] (fUser :/ E)
                        [Where "user_id = ?" $ Just userRef :/ E]
                        []
                        (RowRange 0 1)
                case mret of
                    Just [Just user :/ E] -> return $ Just user
                    _ -> return Nothing
            , setUser = \userRef user -> do
                Db.queryMaybe db $
                    Update "sn_users" (fUser :/ E) (Just user :/ E)
                        [Where "user_id = ?" $ Just userRef :/ E]
            , deleteUser = \userRef -> do
                Db.queryMaybe db $
                    Delete "sn_users"
                        [Where "user_id = ?" $ Just userRef :/ E]
            , listUsers = \offset limit -> do
                mret <- Db.queryMaybe db $
                    Select ["sn_users"] (fReference "user_id" :/ fUser :/ E)
                        []
                        [Asc "user_id"]
                        (RowRange offset limit)
                case mret of
                    Just rets -> return $ mapMaybe
                        (\row -> case row of
                            Just userRef :/ Just user :/ E -> Just (userRef, user)
                            _ -> Nothing)
                        rets
                    Nothing -> return []
            , spawnAccessKey = \userRef -> do
                keyBack <- generateAccessKey pgen
                keyRef@(Reference keyFront) <- generateRef pgen
                let key = AccessKey keyFront keyBack
                let keyHash = hashAccessKey key
                let keyInfo = AccessKeyInfo keyHash userRef
                mret <- Db.queryMaybe db $
                    Insert_ "sn_access_keys" (fReference "access_key_id" :/ fAccessKeyInfo:/ E) (Just keyRef :/ Just keyInfo :/ E)
                case mret of
                    Just () -> return $ Just $ key
                    _ -> return Nothing
            , lookupAccessKey = \key@(AccessKey keyFront keyBack) -> do
                let keyHash = hashAccessKey key
                mret <- Db.queryMaybe db $
                    Select ["sn_access_keys"] (fReference "access_key_user_id" :/ E)
                        [Where "access_key_id = ? AND access_key_hash = ?" $ Just keyFront :/ Just keyHash :/ E]
                        []
                        (RowRange 0 1)
                case mret of
                    Just [Just userRef :/ E] -> return $ Just userRef
                    _ -> return Nothing
            , deleteAccessKey = \keyRef -> do
                Db.queryMaybe db $
                    Delete "sn_access_keys"
                        [Where "access_key_id = ?" $ Just keyRef :/ E]
            , listAccessKeysOf = \userRef -> do
                mret <- Db.queryMaybe db $
                    Select ["sn_access_keys"] (fReference "access_key_id" :/ E)
                        [Where "access_key_user_id = ?" $ Just userRef :/ E]
                        []
                        AllRows
                case mret of
                    Just rets -> return $ Just $ mapMaybe
                        (\row -> case row of
                            Just keyRef :/ E -> Just keyRef
                            _ -> Nothing)
                        rets
                    _ -> return Nothing
            }

accessKeyId :: AccessKey -> Reference AccessKeyInfo
accessKeyId (AccessKey front _) = Reference front

hashAccessKey :: AccessKey -> BS.ByteString
hashAccessKey (AccessKey front back) = do
    let ctx0 = CHash.hashInitWith CHash.SHA3_256
    let ctx1 = CHash.hashUpdate ctx0 back
    let ctx2 = CHash.hashUpdate ctx1 front
    BA.convert $ CHash.hashFinalize ctx2

instance IsValue (Reference a) where
    type Prims (Reference a) = '[ 'TBlob ]
    primDecode = fmap Reference . primDecode
    primEncode (Reference bstr) = primEncode bstr

fReference :: FieldName -> Field (Reference a)
fReference fieldName = Field (FBlob fieldName :/ E)

instance IsValue User where
    type Prims User = '[ 'TText, 'TText, 'TTime, 'TInt ]
    primDecode (VText name :/ VText surname :/ VTime joinDate :/ vIsAdmin :/ E) = do
        isAdmin <- primDecode $ vIsAdmin :/ E
        Just $ User name surname joinDate isAdmin
    primDecode _ = Nothing
    primEncode (User name surname joinDate isAdmin) =
        VText name :/ VText surname :/ VTime joinDate :/ primEncode isAdmin

fUser :: Field User
fUser = Field (FText "user_name" :/ FText "user_surname" :/ FTime "user_join_date" :/ FInt "user_is_admin" :/ E)

instance IsValue AccessKeyInfo where
    type Prims AccessKeyInfo = '[ 'TBlob, 'TBlob ]
    primDecode (VBlob keyHash :/ vUserId :/ E) = do
        userId <- primDecode $ vUserId :/ E
        Just $ AccessKeyInfo keyHash userId
    primDecode _ = Nothing
    primEncode (AccessKeyInfo keyHash userId) =
        VBlob keyHash :/ primEncode userId

fAccessKeyInfo :: Field AccessKeyInfo
fAccessKeyInfo = Field (FBlob "access_key_hash" :/ FBlob "access_key_user_id" :/ E)

instance IsValue Author where
    type Prims Author = '[ 'TText, 'TText ]
    primDecode (VText name :/ VText description :/ E) = do
        Just $ Author name description
    primDecode _ = Nothing
    primEncode (Author name description) =
        VText name :/ VText description :/ E

fAuthor :: Field Author
fAuthor = Field (FText "author_name" :/ FText "author_description" :/ E)

instance IsValue PublicationStatus where
    type Prims PublicationStatus = '[ 'TTime ]
    primDecode (VTime date :/ E) = Just $ PublishAt date
    primDecode (VNull :/ E) = Just $ NonPublished
    primEncode (PublishAt date) = VTime date :/ E
    primEncode NonPublished = VNull :/ E

instance IsValue Article where
    type Prims Article = '[ 'TBlob, 'TText, 'TText, 'TTime ]
    primDecode (vAuthorId :/ VText name :/ VText text :/ vPubDate :/ E) = do
        authorId <- primDecode $ vAuthorId :/ E
        pubStatus <- primDecode $ vPubDate :/ E
        Just $ Article authorId name text pubStatus
    primDecode _ = Nothing
    primEncode (Article authorId name text pubStatus) =
        primEncode authorId ++/ VText name :/ VText text :/ primEncode pubStatus

fArticle :: Field Article
fArticle = Field (FBlob "article_author_id" :/ FText "article_name" :/ FText "article_text" :/ FTime "article_publication_date" :/ E)

generateRef :: IORef CRand.ChaChaDRG -> IO (Reference a)
generateRef pgen = Reference <$> generateByteString pgen 16

generateAccessKey :: IORef CRand.ChaChaDRG -> IO BS.ByteString
generateAccessKey pgen = generateByteString pgen 48

generateByteString :: IORef CRand.ChaChaDRG -> Int -> IO BS.ByteString
generateByteString pgen len = atomicModifyIORef' pgen $ \gen1 ->
    let (value, gen2) = CRand.randomBytesGenerate len gen1
    in (gen2, value)
