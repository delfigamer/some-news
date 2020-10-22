{-# LANGUAGE LambdaCase #-}
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
    , Version(..)
    , Result(..)
    , InitFailure(..)
    , Handle
    , withSqlStorage
    , currentSchema
    , upgradeSchema
    , spawnUser
    , getUser
    , setUser
    , deleteUser
    , listUsers
    , spawnAccessKey
    , lookupAccessKey
    , deleteAccessKey
    , listAccessKeysOf
    , spawnAuthor
    , getAuthor
    , setAuthor
    , deleteAuthor
    , listAuthors
    , listAuthorsOfUser
    , listUsersOfAuthor
    , connectUserAuthor
    , disconnectUserAuthor
    , spawnArticle
    , getArticle
    , setArticle
    , deleteArticle
    , listArticles
    , listArticlesOfAuthor
    , listArticlesOfUser
    , accessKeyId
    ) where

import Control.Applicative
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
    deriving (Show, Eq)

instance Ord PublicationStatus where
    NonPublished `compare` NonPublished = EQ
    NonPublished `compare` PublishAt _ = LT
    PublishAt _  `compare` NonPublished = GT
    PublishAt a  `compare` PublishAt b = b `compare` a

data Article = Article
    { articleAuthor :: !(Reference Author)
    , articleName :: !Text.Text
    , articleText :: !Text.Text
    , articlePublicationStatus :: !PublicationStatus
    }
    deriving (Show, Eq, Ord)

data Version a = Version !BS.ByteString
    deriving (Show, Eq, Ord)

data Result a
    = Ok !a
    | NotFoundError
    | InternalError
    deriving (Show, Eq, Ord)

data Handle = Handle
    { hLogger :: Logger.Handle
    , hDb :: Db.Handle
    , hGen :: IORef CRand.ChaChaDRG
    }

withSqlStorage :: Logger.Handle -> Db.Handle -> (InitFailure -> IO r) -> (Handle -> IO r) -> IO r
withSqlStorage logger db onFail onSuccess = do
    matchCurrentSchema logger db onFail $ do
        pgen <- newIORef =<< CRand.drgNew
        onSuccess $ Handle
            { hLogger = logger
            , hDb = db
            , hGen = pgen
            }

spawnUser :: Handle -> User -> IO (Result (Reference User))
spawnUser = spawnObject "sn_users" "user_id" fUser

getUser :: Handle -> Reference User -> IO (Result User)
getUser = getObject "sn_users" "user_id" fUser

setUser :: Handle -> Reference User -> User -> IO (Result ())
setUser = setObject "sn_users" "user_id" fUser

deleteUser :: Handle -> Reference User -> IO (Result ())
deleteUser = deleteObject "sn_users" "user_id" fUser

listUsers :: Handle -> Int64 -> Int64 -> IO (Result [(Reference User, User)])
listUsers = listObjects "sn_users" "user_id" fUser

spawnAccessKey :: Handle -> Reference User -> IO (Result AccessKey)
spawnAccessKey handle userRef = do
    keyBack <- generateAccessKey handle
    keyRef@(Reference keyFront) <- generateRef handle
    let key = AccessKey keyFront keyBack
    let keyHash = hashAccessKey key
    let keyInfo = AccessKeyInfo keyHash userRef
    makeQuery handle
        (Insert "sn_access_keys"
            (fReference "access_key_id" :/ fAccessKeyInfo:/ E)
            (Just keyRef :/ Just keyInfo :/ E)
            E)
        (\case
            Just (Just E) -> Ok key
            _ -> InternalError)

lookupAccessKey :: Handle -> AccessKey -> IO (Result (Reference User))
lookupAccessKey handle key@(AccessKey keyFront keyBack) = do
    let keyHash = hashAccessKey key
    makeQuery handle
        (Select ["sn_access_keys"] (fReference "access_key_user_id" :/ E)
            [Where "access_key_id = ? AND access_key_hash = ?" $ Just keyFront :/ Just keyHash :/ E]
            []
            (RowRange 0 1))
        (\case
            Just [Just userRef :/ E] -> Ok userRef
            Just [] -> NotFoundError
            _ -> InternalError)

deleteAccessKey :: Handle -> Reference User -> Reference AccessKeyInfo -> IO (Result ())
deleteAccessKey handle userRef keyRef = do
    makeQuery handle
        (Delete "sn_access_keys"
            [Where "access_key_user_id = ? AND access_key_id = ?" $ Just userRef :/ Just keyRef :/ E])
        (\case
            Just 1 -> Ok ()
            Just 0 -> NotFoundError
            _ -> InternalError)

listAccessKeysOf :: Handle -> Reference User -> Int64 -> Int64 -> IO (Result [Reference AccessKeyInfo])
listAccessKeysOf handle userRef offset limit = do
    makeQuery handle
        (Select ["sn_access_keys"] (fReference "access_key_id" :/ E)
            [Where "access_key_user_id = ?" $ Just userRef :/ E]
            [Asc "access_key_id"]
            (RowRange offset limit))
        (\case
            Just rs -> Ok $ flip mapMaybe rs $ \case
                Just keyRef :/ E -> Just keyRef
                _ -> Nothing
            _ -> InternalError)

spawnAuthor :: Handle -> Author -> IO (Result (Reference Author))
spawnAuthor = spawnObject "sn_authors" "author_id" fAuthor

getAuthor :: Handle -> Reference Author -> IO (Result Author)
getAuthor = getObject "sn_authors" "author_id" fAuthor

setAuthor :: Handle -> Reference Author -> Author -> IO (Result ())
setAuthor = setObject "sn_authors" "author_id" fAuthor

deleteAuthor :: Handle -> Reference Author -> IO (Result ())
deleteAuthor handle authorRef = do
    tres <- deleteObject "sn_authors" "author_id" fAuthor handle authorRef
    case tres of
        Ok () -> do
            makeQuery handle
                (Delete "sn_articles"
                    [Where "article_author_id IS NULL AND article_publication_date = ?" $ Just NonPublished :/ E])
                (const ())
        _ -> return ()
    return tres

listAuthors :: Handle -> Int64 -> Int64 -> IO (Result [(Reference Author, Author)])
listAuthors = listObjects "sn_authors" "author_id" fAuthor

listAuthorsOfUser :: Handle -> Reference User -> Int64 -> Int64 -> IO (Result [(Reference Author, Author)])
listAuthorsOfUser handle userRef offset limit = do
    makeQuery handle
        (Select ["sn_author2user", "sn_authors"] (fReference "author_id" :/ fAuthor :/ E)
            [Where "a2u_user_id = ? AND a2u_author_id = author_id" $ Just userRef :/ E]
            [Asc "author_id"]
            (RowRange offset limit))
        (\case
            Just rs -> Ok $ flip mapMaybe rs $ \case
                Just authorRef :/ Just author :/ E -> Just (authorRef, author)
                _ -> Nothing
            _ -> InternalError)

listUsersOfAuthor :: Handle -> Reference Author -> Int64 -> Int64 -> IO (Result [(Reference User, User)])
listUsersOfAuthor handle authorRef offset limit = do
    makeQuery handle
        (Select ["sn_author2user", "sn_users"] (fReference "user_id" :/ fUser :/ E)
            [Where "a2u_author_id = ? AND a2u_user_id = user_id" $ Just authorRef :/ E]
            [Asc "user_id"]
            (RowRange offset limit))
        (\case
            Just rs -> Ok $ flip mapMaybe rs $ \case
                Just userRef :/ Just user :/ E -> Just (userRef, user)
                _ -> Nothing
            _ -> InternalError)

connectUserAuthor :: Handle -> Reference User -> Reference Author -> IO (Result ())
connectUserAuthor handle userRef authorRef = do
    makeQuery handle
        (Insert "sn_author2user"
            (fReference "a2u_user_id" :/ fReference "a2u_author_id" :/ E)
            (Just userRef :/ Just authorRef :/ E)
            E)
        (\case
            Just _ -> Ok ()
            _ -> InternalError)

disconnectUserAuthor :: Handle -> Reference User -> Reference Author -> IO (Result ())
disconnectUserAuthor handle userRef authorRef = do
    makeQuery handle
        (Delete "sn_author2user"
            [Where "a2u_user_id = ? AND a2u_author_id = ?" $ Just userRef :/ Just authorRef :/ E])
        (\case
            Just _ -> Ok ()
            _ -> InternalError)

spawnArticle :: Handle -> Article -> IO (Result (Reference Article, Version Article))
spawnArticle handle article = do
    articleRef <- generateRef handle
    articleVersion <- generateVersion handle
    makeQuery handle
        (Insert "sn_articles"
            (fReference "article_id" :/ fVersion "article_version" :/ fArticle :/ E)
            (Just articleRef :/ Just articleVersion :/ Just article :/ E)
            E)
        (\case
            Just (Just E) -> Ok (articleRef, articleVersion)
            _ -> InternalError)

getArticle :: Handle -> Reference Article -> IO (Result (Article, Version Article))
getArticle handle articleRef = do
    makeQuery handle
        (Select ["sn_articles"]
            (fVersion "article_version" :/ fArticle :/ E)
            [Where "article_id = ?" $ Just articleRef :/ E]
            []
            (RowRange 0 1))
        (\case
            Just [Just articleVersion :/ Just article :/ E] -> Ok (article, articleVersion)
            Just [] -> NotFoundError
            _ -> InternalError)

setArticle :: Handle -> Reference Article -> Article -> Version Article -> IO (Result (Version Article))
setArticle handle articleRef article oldVersion = do
    newVersion <- generateVersion handle
    makeQuery handle
        (Update "sn_articles"
            (fVersion "article_version" :/ fArticle :/ E)
            (Just newVersion :/ Just article :/ E)
            [Where "article_id = ? AND article_version = ?" $ Just articleRef :/ Just oldVersion :/ E])
        (\case
            Just 1 -> Ok newVersion
            Just 0 -> NotFoundError
            _ -> InternalError)

deleteArticle :: Handle -> Reference Article -> IO (Result ())
deleteArticle handle articleRef = do
    makeQuery handle
        (Delete "sn_articles"
            [Where "article_id = ?" $ Just articleRef :/ E])
        (\case
            Just 1 -> Ok ()
            Just 0 -> NotFoundError
            _ -> InternalError)

listArticles :: Handle -> Bool -> Int64 -> Int64 -> IO (Result [(Reference Article, Article)])
listArticles handle hideDrafts offset limit = do
    now <- getCurrentTime
    makeQuery handle
        (Select ["sn_articles"]
            (fReference "article_id" :/ fArticle :/ E)
            [Where "article_publication_date <= ?" $ Just now :/ E | hideDrafts]
            [Desc "article_publication_date", Asc "article_id"]
            (RowRange offset limit))
        (\case
            Just rs -> Ok $ flip mapMaybe rs $ \case
                Just articleRef :/ Just article :/ E -> Just (articleRef, article)
                _ -> Nothing
            _ -> InternalError)

listArticlesOfAuthor :: Handle -> Bool -> Reference Author -> Int64 -> Int64 -> IO (Result [(Reference Article, Article)])
listArticlesOfAuthor handle hideDrafts authorRef offset limit = do
    now <- getCurrentTime
    makeQuery handle
        (Select ["sn_articles"]
            (fReference "article_id" :/ fArticle :/ E)
            ([Where "article_author_id = ?" $ Just authorRef :/ E]
                ++ [Where "article_publication_date <= ?" $ Just now :/ E | hideDrafts])
            [Desc "article_publication_date", Asc "article_id"]
            (RowRange offset limit))
        (\case
            Just rs -> Ok $ flip mapMaybe rs $ \case
                Just articleRef :/ Just article :/ E -> Just (articleRef, article)
                _ -> Nothing
            _ -> InternalError)

listArticlesOfUser :: Handle -> Bool -> Reference User -> Int64 -> Int64 -> IO (Result [(Reference Article, Article)])
listArticlesOfUser handle hideDrafts userRef offset limit = do
    now <- getCurrentTime
    makeQuery handle
        (Select ["sn_author2user", "sn_articles"]
            (fReference "article_id" :/ fArticle :/ E)
            ([Where "a2u_user_id = ? AND article_author_id = a2u_author_id" $ Just userRef :/ E]
                ++ [Where "article_publication_date <= ?" $ Just now :/ E | hideDrafts])
            [Desc "article_publication_date", Asc "article_id"]
            (RowRange offset limit))
        (\case
            Just rs -> Ok $ flip mapMaybe rs $ \case
                Just articleRef :/ Just article :/ E -> Just (articleRef, article)
                _ -> Nothing
            _ -> InternalError)

accessKeyId :: AccessKey -> Reference AccessKeyInfo
accessKeyId (AccessKey front _) = Reference front

hashAccessKey :: AccessKey -> BS.ByteString
hashAccessKey (AccessKey front back) = do
    let ctx0 = CHash.hashInitWith CHash.SHA3_256
    let ctx1 = CHash.hashUpdate ctx0 back
    let ctx2 = CHash.hashUpdate ctx1 front
    BA.convert $ CHash.hashFinalize ctx2

spawnObject
    :: IsValue obj => TableName -> FieldName -> Field obj
    -> Handle -> obj -> IO (Result (Reference obj))
spawnObject tableName refFieldName objectField handle object = do
    ref <- generateRef handle
    makeQuery handle
        (Insert tableName
            (fReference refFieldName :/ objectField :/ E)
            (Just ref :/ Just object :/ E)
            E)
        (\case
            Just (Just E) -> Ok ref
            _ -> InternalError)

getObject
    :: IsValue obj => TableName -> FieldName -> Field obj
    -> Handle -> Reference obj -> IO (Result obj)
getObject tableName (FieldName refFieldStr) objectField handle ref = do
    makeQuery handle
        (Select [tableName] (objectField :/ E)
            [Where (refFieldStr ++ " = ?") $ Just ref :/ E]
            []
            (RowRange 0 1))
        (\case
            Just [Just object :/ E] -> Ok object
            Just [] -> NotFoundError
            _ -> InternalError)

setObject
    :: IsValue obj => TableName -> FieldName -> Field obj
    -> Handle -> Reference obj -> obj -> IO (Result ())
setObject tableName (FieldName refFieldStr) objectField handle ref object = do
    makeQuery handle
        (Update tableName (objectField :/ E) (Just object :/ E)
            [Where (refFieldStr ++ " = ?") $ Just ref :/ E])
        (\case
            Just 1 -> Ok ()
            Just 0 -> NotFoundError
            _ -> InternalError)

deleteObject
    :: IsValue obj => TableName -> FieldName -> Field obj
    -> Handle -> Reference obj -> IO (Result ())
deleteObject tableName (FieldName refFieldStr) objectField handle ref = do
    makeQuery handle
        (Delete tableName
            [Where (refFieldStr ++ " = ?") $ Just ref :/ E])
        (\case
            Just 1 -> Ok ()
            Just 0 -> NotFoundError
            _ -> InternalError)

listObjects
    :: IsValue obj => TableName -> FieldName -> Field obj
    -> Handle -> Int64 -> Int64 -> IO (Result [(Reference obj, obj)])
listObjects tableName refFieldName objectField handle offset limit = do
    makeQuery handle
        (Select [tableName] (fReference refFieldName :/ objectField :/ E)
            []
            [Asc refFieldName]
            (RowRange offset limit))
        (\case
            Just rs -> Ok $ flip mapMaybe rs $ \case
                Just ref :/ Just object :/ E -> Just (ref, object)
                _ -> Nothing
            _ -> InternalError)

generateRef :: Handle -> IO (Reference a)
generateRef handle = Reference <$> generateByteString handle 16

generateVersion :: Handle -> IO (Version a)
generateVersion handle = Version <$> generateByteString handle 16

generateAccessKey :: Handle -> IO BS.ByteString
generateAccessKey handle = generateByteString handle 48

generateByteString :: Handle -> Int -> IO BS.ByteString
generateByteString handle len = atomicModifyIORef' (hGen handle) $ \gen1 ->
    let (value, gen2) = CRand.randomBytesGenerate len gen1
    in (gen2, value)

makeQuery :: Handle -> Query qr -> (Maybe qr -> r) -> IO r
makeQuery handle query mapf = do
    mret <- Db.queryMaybe (hDb handle) query
    return $ mapf mret

instance IsValue (Reference a) where
    type Prims (Reference a) = '[ 'TBlob ]
    primDecode (VBlob x :/ E) = Just $ Reference x
    primDecode (VNull :/ E) = Just $ Reference ""
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
    primDecode (_ :/ E) = Just $ NonPublished
    primEncode (PublishAt date) = VTime date :/ E
    primEncode NonPublished = VTPosInf :/ E

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

instance IsValue (Version a) where
    type Prims (Version a) = '[ 'TBlob ]
    primDecode (VBlob version :/ E) = Just $ Version version
    primDecode _ = Nothing
    primEncode (Version version) = VBlob version :/ E

fVersion :: FieldName -> Field (Version a)
fVersion fieldName = Field (FBlob fieldName :/ E)
