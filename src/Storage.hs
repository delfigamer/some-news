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
    , listAccessKeysOf :: Reference User -> Int64 -> Int64 -> IO (Maybe [Reference AccessKeyInfo])
    , spawnAuthor :: Author -> IO (Maybe (Reference Author))
    , getAuthor :: Reference Author -> IO (Maybe Author)
    , setAuthor :: Reference Author -> Author -> IO (Maybe ())
    , deleteAuthor :: Reference Author -> IO (Maybe ())
    , listAuthors :: Int64 -> Int64 -> IO [(Reference Author, Author)]
    , listAuthorsOfUser :: Reference User -> Int64 -> Int64 -> IO (Maybe [(Reference Author, Author)])
    , listUsersOfAuthor :: Reference Author -> Int64 -> Int64 -> IO (Maybe [(Reference User, User)])
    , connectUserAuthor :: Reference User -> Reference Author -> IO (Maybe ())
    , disconnectUserAuthor :: Reference User -> Reference Author -> IO (Maybe ())
    }

withSqlStorage :: Logger.Handle -> Db.Handle -> (InitFailure -> IO r) -> (Handle -> IO r) -> IO r
withSqlStorage logger db onFail onSuccess = do
    matchCurrentSchema logger db onFail $ do
        pgen <- newIORef =<< CRand.drgNew
        onSuccess $ Handle
            { spawnUser = spawnObject logger db pgen "sn_users" "user_id" fUser
            , getUser = getObject logger db pgen "sn_users" "user_id" fUser
            , setUser = setObject logger db pgen "sn_users" "user_id" fUser
            , deleteUser = deleteObject logger db pgen "sn_users" "user_id" fUser
            , listUsers = listObjects logger db pgen "sn_users" "user_id" fUser
            , spawnAccessKey = \userRef -> do
                keyBack <- generateAccessKey pgen
                keyRef@(Reference keyFront) <- generateRef pgen
                let key = AccessKey keyFront keyBack
                let keyHash = hashAccessKey key
                let keyInfo = AccessKeyInfo keyHash userRef
                mret <- Db.queryMaybe db $
                    Insert_ "sn_access_keys"
                        (fReference "access_key_id" :/ fAccessKeyInfo:/ E)
                        (Just keyRef :/ Just keyInfo :/ E)
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
            , listAccessKeysOf = \userRef offset limit -> do
                mret <- Db.queryMaybe db $
                    Select ["sn_access_keys"] (fReference "access_key_id" :/ E)
                        [Where "access_key_user_id = ?" $ Just userRef :/ E]
                        [Asc "access_key_id"]
                        (RowRange offset limit)
                case mret of
                    Just rets -> return $ Just $ mapMaybe
                        (\row -> case row of
                            Just keyRef :/ E -> Just keyRef
                            _ -> Nothing)
                        rets
                    _ -> return Nothing
            , spawnAuthor = spawnObject logger db pgen "sn_authors" "author_id" fAuthor
            , getAuthor = getObject logger db pgen "sn_authors" "author_id" fAuthor
            , setAuthor = setObject logger db pgen "sn_authors" "author_id" fAuthor
            , deleteAuthor = deleteObject logger db pgen "sn_authors" "author_id" fAuthor
            , listAuthors = listObjects logger db pgen "sn_authors" "author_id" fAuthor
            , listAuthorsOfUser = \userRef offset limit -> do
                mret <- Db.queryMaybe db $
                    Select ["sn_author2user", "sn_authors"] (fReference "author_id" :/ fAuthor :/ E)
                        [Where "a2u_user_id = ? AND a2u_author_id = author_id" $ Just userRef :/ E]
                        [Asc "author_id"]
                        (RowRange offset limit)
                case mret of
                    Just rets -> return $ Just $ mapMaybe
                        (\row -> case row of
                            Just authorRef :/ Just author :/ E -> Just (authorRef, author)
                            _ -> Nothing)
                        rets
                    _ -> return Nothing
            , listUsersOfAuthor = \authorRef offset limit -> do
                mret <- Db.queryMaybe db $
                    Select ["sn_author2user", "sn_users"] (fReference "user_id" :/ fUser :/ E)
                        [Where "a2u_author_id = ? AND a2u_user_id = user_id" $ Just authorRef :/ E]
                        [Asc "user_id"]
                        (RowRange offset limit)
                case mret of
                    Just rets -> return $ Just $ mapMaybe
                        (\row -> case row of
                            Just userRef :/ Just user :/ E -> Just (userRef, user)
                            _ -> Nothing)
                        rets
                    _ -> return Nothing
            , connectUserAuthor = \userRef authorRef -> do
                Db.queryMaybe db $
                    Insert_ "sn_author2user"
                        (fReference "a2u_user_id" :/ fReference "a2u_author_id" :/ E)
                        (Just userRef :/ Just authorRef :/ E)
            , disconnectUserAuthor = \userRef authorRef -> do
                Db.queryMaybe db $
                    Delete "sn_author2user"
                        [Where "a2u_user_id = ? AND a2u_author_id = ?" $ Just userRef :/ Just authorRef :/ E]
            }

accessKeyId :: AccessKey -> Reference AccessKeyInfo
accessKeyId (AccessKey front _) = Reference front

hashAccessKey :: AccessKey -> BS.ByteString
hashAccessKey (AccessKey front back) = do
    let ctx0 = CHash.hashInitWith CHash.SHA3_256
    let ctx1 = CHash.hashUpdate ctx0 back
    let ctx2 = CHash.hashUpdate ctx1 front
    BA.convert $ CHash.hashFinalize ctx2

spawnObject
    :: IsValue obj => Logger.Handle -> Db.Handle -> IORef CRand.ChaChaDRG
    -> TableName -> FieldName -> Field obj
    -> obj -> IO (Maybe (Reference obj))
spawnObject logger db pgen tableName refFieldName objectField object = do
    ref <- generateRef pgen
    mret <- Db.queryMaybe db $
        Insert_ tableName
            (fReference refFieldName :/ objectField :/ E)
            (Just ref :/ Just object :/ E)
    case mret of
        Just () -> return $ Just ref
        _ -> return Nothing

getObject
    :: IsValue obj => Logger.Handle -> Db.Handle -> IORef CRand.ChaChaDRG
    -> TableName -> FieldName -> Field obj
    -> Reference obj -> IO (Maybe obj)
getObject logger db _pgen tableName (FieldName refFieldStr) objectField ref = do
    mret <- Db.queryMaybe db $
        Select [tableName] (objectField :/ E)
            [Where (refFieldStr ++ " = ?") $ Just ref :/ E]
            []
            (RowRange 0 1)
    case mret of
        Just [Just object :/ E] -> return $ Just object
        _ -> return Nothing

setObject
    :: IsValue obj => Logger.Handle -> Db.Handle -> IORef CRand.ChaChaDRG
    -> TableName -> FieldName -> Field obj
    -> Reference obj -> obj -> IO (Maybe ())
setObject logger db _pgen tableName (FieldName refFieldStr) objectField ref object = do
    Db.queryMaybe db $
        Update tableName (objectField :/ E) (Just object :/ E)
            [Where (refFieldStr ++ " = ?") $ Just ref :/ E]

deleteObject
    :: IsValue obj => Logger.Handle -> Db.Handle -> IORef CRand.ChaChaDRG
    -> TableName -> FieldName -> Field obj
    -> Reference obj -> IO (Maybe ())
deleteObject logger db _pgen tableName (FieldName refFieldStr) objectField ref = do
    Db.queryMaybe db $
        Delete tableName
            [Where (refFieldStr ++ " = ?") $ Just ref :/ E]

listObjects
    :: IsValue obj => Logger.Handle -> Db.Handle -> IORef CRand.ChaChaDRG
    -> TableName -> FieldName -> Field obj
    -> Int64 -> Int64 -> IO [(Reference obj, obj)]
listObjects logger db _pgen tableName refFieldName objectField offset limit = do
    mret <- Db.queryMaybe db $
        Select [tableName] (fReference refFieldName :/ objectField :/ E)
            []
            [Asc refFieldName]
            (RowRange offset limit)
    case mret of
        Just rets -> return $ mapMaybe
            (\row -> case row of
                Just ref :/ Just object :/ E -> Just (ref, object)
                _ -> Nothing)
            rets
        _ -> return []

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
