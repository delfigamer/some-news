{-# LANGUAGE EmptyCase #-}
{-# LANGUAGE EmptyDataDeriving #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE StandaloneDeriving #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE UndecidableInstances #-}

module Storage
    ( Reference(..)
    , Version(..)
    , User(..)
    , AccessKey(..)
    , Author(..)
    , Category(..)
    , PublicationStatus(..)
    , Article(..)
    , Tag(..)
    , Comment(..)
    , InitFailure(..)
    , Handle
    , withSqlStorage
    , currentSchema
    , upgradeSchema
    , Action(..)
    , StorageError(..)
    , ListView(..)
    , ViewFilter(..)
    , ViewOrder(..)
    , OrderDirection(..)
    , perform
    ) where

import Control.Applicative
import Control.Exception
import Control.Monad
import Control.Monad.Except
import Control.Monad.Reader
import Data.Bits
import Data.IORef
import Data.Int
import Data.List
import Data.Maybe
import Data.Proxy
import Data.Semigroup
import Data.Time.Clock
import Data.Word
import GHC.Generics (Generic)
import qualified Crypto.Hash as CHash
import qualified Crypto.Random as CRand
import qualified Data.ByteArray as BA
import qualified Data.ByteString as BS
import qualified Data.Text as Text
import Sql.Query
import Storage.Schema
import Tuple
import qualified Logger
import qualified Sql.Database as Db

data Reference a = Reference !BS.ByteString
    deriving (Eq, Ord)

instance Show (Reference a) where
    showsPrec _ (Reference x) = showBlob "ref" x

data Version a = Version !BS.ByteString
    deriving (Eq, Ord)

instance Show (Version a) where
    showsPrec _ (Version x) = showBlob "ver" x

data User = User
    { userId :: !(Reference User)
    , userName :: !Text.Text
    , userSurname :: !Text.Text
    , userJoinDate :: !UTCTime
    , userIsAdmin :: !Bool
    }
    deriving (Show, Eq, Ord)

data AccessKey = AccessKey
    { accessKeyId :: !(Reference AccessKey)
    , accessKeyToken :: !BS.ByteString
    }
    deriving (Show, Eq, Ord)

data Author = Author
    { authorId :: !(Reference Author)
    , authorName :: !Text.Text
    , authorDescription :: !Text.Text
    }
    deriving (Show, Eq, Ord)

data Category = Category
    { categoryId :: !(Reference Category)
    , categoryName :: !Text.Text
    , categoryParent :: !(Reference Category)
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
    { articleId :: !(Reference Article)
    , articleVersion :: !(Version Article)
    , articleAuthor :: !(Reference Author)
    , articleName :: !Text.Text
    , articleText :: !Text.Text
    , articlePublicationStatus :: !PublicationStatus
    , articleCategory :: !(Reference Category)
    }
    deriving (Show, Eq, Ord)

data Tag = Tag
    { tagId :: !(Reference Tag)
    , tagName :: !Text.Text
    }
    deriving (Show, Eq, Ord)

data Comment = Comment
    { commentId :: !(Reference Comment)
    , commentArticle :: !(Reference Article)
    , commentUser :: !(Reference User)
    , commentText :: !Text.Text
    , commentDate :: !UTCTime
    , commentEditDate :: !(Maybe UTCTime)
    }

data Handle = Handle
    { perform :: forall a. Action a -> IO (Either StorageError a)
    }

data SqlStorage = SqlStorage
    { storageLogger :: Logger.Handle
    , storageDb :: Db.Handle
    , storageGen :: IORef CRand.ChaChaDRG
    }

withSqlStorage :: Logger.Handle -> Db.Handle -> (InitFailure -> IO r) -> (Handle -> IO r) -> IO r
withSqlStorage logger db onFail onSuccess = do
    matchCurrentSchema logger db onFail $ do
        pgen <- newIORef =<< CRand.drgNew
        let storage = SqlStorage logger db pgen
        onSuccess $ Handle
            { perform = \action -> performStorage action storage
            }

data Action a where
    UserCreate :: Text.Text -> Text.Text -> Action User
    UserSetName :: Reference User -> Text.Text -> Text.Text -> Action ()
    UserSetIsAdmin :: Reference User -> Bool -> Action ()
    UserDelete :: Reference User -> Action ()
    UserList :: ListView User -> Action [User]

    AccessKeyCreate :: Reference User -> Action AccessKey
    AccessKeyDelete :: Reference User -> Reference AccessKey -> Action ()
    AccessKeyList :: Reference User -> ListView AccessKey -> Action [Reference AccessKey]
    AccessKeyLookup :: AccessKey -> Action (Reference User)

    AuthorCreate :: Text.Text -> Text.Text -> Action Author
    AuthorSetName :: Reference Author -> Text.Text -> Action ()
    AuthorSetDescription :: Reference Author -> Text.Text -> Action ()
    AuthorDelete :: Reference Author -> Action ()
    AuthorList :: ListView Author -> Action [Author]
    AuthorSetOwnership :: Reference Author -> Reference User -> Bool -> Action ()

    CategoryCreate :: Text.Text -> Reference Category -> Action Category
    CategorySetName :: Reference Category -> Text.Text -> Action ()
    CategorySetParent :: Reference Category -> Reference Category -> Action ()
    CategoryDelete :: Reference Category -> Action ()
    CategoryList :: ListView Category -> Action [Category]

    ArticleCreate :: Reference Author -> Action Article
    ArticleSetAuthor :: Reference Article -> Reference Author -> Action ()
    ArticleSetName :: Reference Article -> Version Article -> Text.Text -> Action (Version Article)
    ArticleSetText :: Reference Article -> Version Article -> Text.Text -> Action (Version Article)
    ArticleSetCategory :: Reference Article -> Reference Category -> Action ()
    ArticleSetPublicationStatus :: Reference Article -> PublicationStatus -> Action ()
    ArticleDelete :: Reference Article -> Action ()
    ArticleList :: Bool -> ListView Article -> Action [Article]

    TagCreate :: Text.Text -> Action Tag
    TagSetName :: Reference Tag -> Text.Text -> Action ()
    TagDelete :: Reference Tag -> Action ()
    TagList :: ListView Tag -> Action [Tag]

    CommentCreate :: Reference Article -> Reference User -> Text.Text -> Action Comment
    CommentSetText :: Reference Comment -> Text.Text -> Action ()
    CommentDelete :: Reference Comment -> Action ()
    CommentList :: ListView Comment -> Action [Comment]
deriving instance Show (Action a)
deriving instance Eq (Action a)

data ListView a = ListView
    { viewOffset :: Int64
    , viewLimit :: Int64
    , viewFilter :: [ViewFilter a]
    , viewOrder :: [(ViewOrder a, OrderDirection)]
    }
deriving instance (Show (ViewFilter a), Show (ViewOrder a)) => Show (ListView a)
deriving instance (Eq (ViewFilter a), Eq (ViewOrder a)) => Eq (ListView a)

data OrderDirection
    = Ascending
    | Descending
    deriving (Show, Eq)

data family ViewFilter a
data family ViewOrder a

data instance ViewFilter User
    = FilterUserId (Reference User)
    | FilterUserIsAdmin Bool
    | FilterUserAuthorId (Reference Author)
    deriving (Show, Eq)

data instance ViewOrder User
    = OrderUserName
    | OrderUserSurname
    | OrderUserJoinDate
    | OrderUserIsAdmin
    deriving (Show, Eq)

data instance ViewFilter AccessKey
    deriving (Show, Eq)

data instance ViewOrder AccessKey
    deriving (Show, Eq)

data instance ViewFilter Author
    = FilterAuthorId (Reference Author)
    | FilterAuthorUserId (Reference User)
    deriving (Show, Eq)

data instance ViewOrder Author
    = OrderAuthorName
    deriving (Show, Eq)

data instance ViewFilter Category
    = FilterCategoryId (Reference Category)
    | FilterCategoryParentId (Reference Category)
    | FilterCategoryTransitiveParentId (Reference Category)
    deriving (Show, Eq)

data instance ViewOrder Category
    = OrderCategoryName
    deriving (Show, Eq)

data instance ViewFilter Article
    = FilterArticleId (Reference Article)
    | FilterArticleAuthorId (Reference Author)
    | FilterArticleUserId (Reference User)
    | FilterArticleCategoryId (Reference Category)
    | FilterArticleTransitiveCategoryId (Reference Category)
    | FilterArticlePublishedBefore UTCTime
    | FilterArticlePublishedAfter UTCTime
    | FilterArticleTagId [Reference Tag]
    deriving (Show, Eq)

data instance ViewOrder Article
    = OrderArticleName
    | OrderArticleDate
    | OrderArticleAuthorName
    | OrderArticleCategoryName
    deriving (Show, Eq)

data instance ViewFilter Tag
    = FilterTagId (Reference Tag)
    | FilterTagArticleId (Reference Article)
    deriving (Show, Eq)

data instance ViewOrder Tag
    = OrderTagName
    deriving (Show, Eq)

data instance ViewFilter Comment
    = FilterCommentId (Reference Comment)
    | FilterCommentArticleId (Reference Article)
    | FilterCommentUserId (Reference User)
    deriving (Show, Eq)

data instance ViewOrder Comment
    = OrderCommentDate
    deriving (Show, Eq)

data StorageError
    = NotFoundError
    | VersionError
    | InvalidRequestError
    | InternalError
    deriving (Show, Eq, Ord)

performStorage :: Action a -> SqlStorage -> IO (Either StorageError a)
performStorage (UserCreate name surname) = runTransactionRelaxed $ do
    ref <- Reference <$> generateBytes 16
    joinDate <- currentTime
    doQuery
        (Insert "sn_users"
            (fReference "user_id" :/ fText "user_name" :/ fText "user_surname" :/ fTime "user_join_date" :/ fBool "user_is_admin" :/ E)
            (Value ref :/ Value name :/ Value surname :/ Value joinDate :/ Value False :/ E)
            E)
        (parseInsert $ User ref name surname joinDate False)
performStorage (UserSetName userRef name surname) = runTransactionRelaxed $ do
    doQuery
        (Update "sn_users"
            (fText "user_name" :/ fText "user_surname" :/ E)
            (Value name :/ Value surname :/ E)
            [WhereIs userRef "user_id"])
        parseCount
performStorage (UserSetIsAdmin userRef isAdmin) = runTransactionRelaxed $ do
    doQuery
        (Update "sn_users"
            (fBool "user_is_admin" :/ E)
            (Value isAdmin :/ E)
            [WhereIs userRef "user_id"])
        parseCount
performStorage (UserDelete userRef) = runTransactionRelaxed $ do
    doQuery
        (Delete "sn_users"
            [WhereIs userRef "user_id"])
        parseCount
performStorage (UserList view) = runTransactionRelaxed $ do
    withView view $ \tables filter order range _ -> do
        doQuery
            (Select ("sn_users" : tables)
                (fReference "user_id" :/ fText "user_name" :/ fText "user_surname" :/ fTime "user_join_date" :/ fBool "user_is_admin" :/ E)
                filter
                (order "user_id")
                range)
            (parseList $ parseOne User)

performStorage (AccessKeyCreate userRef) = runTransactionRelaxed $ do
    keyBack <- generateBytes 48
    keyFront <- Reference <$> generateBytes 16
    let key = AccessKey keyFront keyBack
    let keyHash = hashAccessKey key
    doQuery
        (Insert "sn_access_keys"
            (fReference "access_key_id" :/ fBlob "access_key_hash" :/ fReference "access_key_user_id" :/ E)
            (Value keyFront :/ Value keyHash :/ Value userRef :/ E)
            E)
        (parseInsert key)
performStorage (AccessKeyDelete userRef keyRef) = runTransactionRelaxed $ do
    doQuery
        (Delete "sn_access_keys"
            [WhereIs userRef "access_key_user_id", WhereIs keyRef "access_key_id"])
        parseCount
performStorage (AccessKeyList userRef view) = runTransactionRelaxed $ do
    withView view $ \tables filter order range _ -> do
        doQuery
            (Select ("sn_access_keys" : tables)
                (fReference "access_key_id" :/ E)
                (WhereIs userRef "access_key_user_id" : filter)
                (order "access_key_id")
                range)
            (parseList $ parseOne id)
performStorage (AccessKeyLookup key@(AccessKey keyFront keyBack)) = runTransactionRelaxed $ do
    let keyHash = hashAccessKey key
    doQuery
        (Select ["sn_access_keys"]
            (fReference "access_key_user_id" :/ E)
            [WhereIs keyFront "access_key_id", WhereWith keyHash "access_key_hash = ?"]
            []
            (RowRange 0 1))
        (parseHead $ parseOne id)

performStorage (AuthorCreate name description) = runTransactionRelaxed $ do
    ref <- Reference <$> generateBytes 16
    doQuery
        (Insert "sn_authors"
            (fReference "author_id" :/ fText "author_name" :/ fText "author_description" :/ E)
            (Value ref :/ Value name :/ Value description :/ E)
            E)
        (parseInsert $ Author ref name description)
performStorage (AuthorSetName authorRef name) = runTransactionRelaxed $ do
    doQuery
        (Update "sn_authors"
            (fText "author_name" :/ E)
            (Value name :/ E)
            [WhereIs authorRef "author_id"])
        parseCount
performStorage (AuthorSetDescription authorRef description) = runTransactionRelaxed $ do
    doQuery
        (Update "sn_authors"
            (fText "author_description" :/ E)
            (Value description :/ E)
            [WhereIs authorRef "author_id"])
        parseCount
performStorage (AuthorDelete authorRef) = runTransactionRelaxed $ do
    doQuery_
        (Delete "sn_articles"
            [WhereIs authorRef "article_author_id", WhereWith NonPublished "article_publication_date = ?"])
    doQuery
        (Delete "sn_authors"
            [WhereIs authorRef "author_id"])
        parseCount
performStorage (AuthorList view) = runTransactionRelaxed $ do
    withView view $ \tables filter order range _ -> do
        doQuery
            (Select ("sn_authors" : tables)
                (fReference "author_id" :/ fText "author_name" :/ fText "author_description" :/ E)
                filter
                (order "author_id")
                range)
            (parseList $ parseOne Author)
performStorage (AuthorSetOwnership authorRef userRef True) = runTransactionRelaxed $ do
    doQuery_
        (Insert "sn_author2user"
            (fReference "a2u_user_id" :/ fReference "a2u_author_id" :/ E)
            (Value userRef :/ Value authorRef :/ E)
            E)
performStorage (AuthorSetOwnership authorRef userRef False) = runTransactionRelaxed $ do
    doQuery_
        (Delete "sn_author2user"
            [WhereIs userRef "a2u_user_id", WhereIs authorRef "a2u_author_id"])

performStorage (CategoryCreate name parentRef) = runTransactionRelaxed $ do
    ref <- Reference <$> generateBytes 2
    doQuery
        (Insert "sn_categories"
            (fReference "category_id" :/ fText "category_name" :/ fReference "category_parent_id" :/ E)
            (Value ref :/ Value name :/ Value parentRef :/ E)
            E)
        (parseInsert $ Category ref name parentRef)
performStorage (CategorySetName categoryRef name) = runTransactionRelaxed $ do
    doQuery
        (Update "sn_categories"
            (fText "category_name" :/ E)
            (Value name :/ E)
            [WhereIs categoryRef "category_id"])
        parseCount
performStorage (CategorySetParent categoryRef parentRef) = runTransactionSerial $ do
    doQuery
        (Select
            [ RecursiveSource "ancestors"
                (fReference "ancestor_category_id" :/ E)
                (Select ["sn_categories"]
                    (fReference "category_id" :/ E)
                    [WhereIs parentRef "category_id"]
                    []
                    AllRows)
                (Select ["sn_categories", "ancestors"]
                    (fReference "category_parent_id" :/ E)
                    [Where "category_id = ancestor_category_id"]
                    []
                    AllRows)
            ]
            (fReference "ancestor_category_id" :/ E)
            [WhereIs categoryRef "ancestor_category_id", Where "ancestor_category_id IS NOT NULL"]
            []
            (RowRange 0 1))
        (\case
            [] -> Right ()
            _ -> Left InvalidRequestError)
    doQuery
        (Update "sn_categories"
            (fReference "category_parent_id" :/ E)
            (Value parentRef :/ E)
            [WhereIs categoryRef "category_id"])
        parseCount
performStorage (CategoryDelete categoryRef) = runTransactionSerial $ do
    parent <- doQuery
        (Select ["sn_categories"]
            (fReference "category_parent_id" :/ E)
            [WhereIs categoryRef "category_id"]
            []
            (RowRange 0 1))
        (parseHead $ parseOne id)
    doQuery_
        (Update "sn_articles"
            (fReference "article_category_id" :/ E)
            (Value parent :/ E)
            [WhereIs categoryRef "article_category_id"])
    doQuery_
        (Update "sn_categories"
            (fReference "category_parent_id" :/ E)
            (Value parent :/ E)
            [WhereIs categoryRef "category_parent_id"])
    doQuery_
        (Delete "sn_categories"
            [WhereIs categoryRef "category_id"])
performStorage (CategoryList view) = runTransactionRelaxed $ do
    withView view $ \tables filter order range _ -> do
        doQuery
            (Select ("sn_categories" : tables)
                (fReference "category_id" :/ fText "category_name" :/ fReference "category_parent_id" :/ E)
                filter
                (order "category_id")
                range)
            (parseList $ parseOne Category)

performStorage (ArticleCreate authorRef) = runTransactionRelaxed $ do
    articleRef <- Reference <$> generateBytes 16
    articleVersion <- Version <$> generateBytes 4
    doQuery
        (Insert "sn_articles"
            (  fReference "article_id"
            :/ fVersion "article_version"
            :/ fReference "article_author_id"
            :/ fText "article_name"
            :/ fText "article_text"
            :/ fPublicationStatus "article_publication_date"
            :/ fReference "article_category_id"
            :/ E)
            (  Value articleRef
            :/ Value articleVersion
            :/ Value authorRef
            :/ Value ""
            :/ Value ""
            :/ Value NonPublished
            :/ Value (Reference "")
            :/ E)
            E)
        (parseInsert $ Article articleRef articleVersion authorRef "" "" NonPublished (Reference ""))
performStorage (ArticleSetAuthor articleRef authorRef) = runTransactionRelaxed $ do
    doQuery
        (Update "sn_articles"
            (fReference "article_author_id" :/ E)
            (Value authorRef :/ E)
            [WhereIs articleRef "article_id"])
        parseCount
performStorage (ArticleSetName articleRef oldVersion name) = runTransactionSerial $ do
    doQuery
        (Select ["sn_articles"]
            (fVersion "article_version" :/ E)
            [WhereIs articleRef "article_id"]
            []
            (RowRange 0 1))
        (\case
            [Just version :/ E]
                | version == oldVersion -> Right ()
                | otherwise -> Left VersionError
            [] -> Left NotFoundError
            _ -> Left InternalError)
    newVersion <- Version <$> generateBytes 4
    doQuery_
        (Update "sn_articles"
            (fVersion "article_version" :/ fText "article_name" :/ E)
            (Value newVersion :/ Value name :/ E)
            [WhereIs articleRef "article_id"])
    return newVersion
performStorage (ArticleSetText articleRef oldVersion text) = runTransactionSerial $ do
    doQuery
        (Select ["sn_articles"]
            (fVersion "article_version" :/ E)
            [WhereIs articleRef "article_id"]
            []
            (RowRange 0 1))
        (\case
            [Just version :/ E]
                | version == oldVersion -> Right ()
                | otherwise -> Left VersionError
            [] -> Left NotFoundError
            _ -> Left InternalError)
    newVersion <- Version <$> generateBytes 4
    doQuery_
        (Update "sn_articles"
            (fVersion "article_version" :/ fText "article_text" :/ E)
            (Value newVersion :/ Value text :/ E)
            [WhereIs articleRef "article_id"])
    return newVersion
performStorage (ArticleSetCategory articleRef categoryRef) = runTransactionRelaxed $ do
    doQuery
        (Update "sn_articles"
            (fReference "article_category_id" :/ E)
            (Value categoryRef :/ E)
            [WhereIs articleRef "article_id"])
        parseCount
performStorage (ArticleSetPublicationStatus articleRef pubStatus) = runTransactionRelaxed $ do
    doQuery
        (Update "sn_articles"
            (fPublicationStatus "article_publication_date" :/ E)
            (Value pubStatus :/ E)
            [WhereIs articleRef "article_id"])
        parseCount
performStorage (ArticleDelete articleRef) = runTransactionRelaxed $ do
    doQuery
        (Delete "sn_articles"
            [WhereIs articleRef "article_id"])
        parseCount
performStorage (ArticleList showDrafts view) = runTransactionRelaxed $ do
    withView view $ \tables filter order range time -> do
        doQuery
            (Select ("sn_articles" : tables)
                (  fReference "article_id"
                :/ fVersion "article_version"
                :/ fReference "article_author_id"
                :/ fText "article_name"
                :/ fText "article_text"
                :/ fPublicationStatus "article_publication_date"
                :/ fReference "article_category_id"
                :/ E)
                ([WhereWith time "article_publication_date <= ?" | not showDrafts] ++ filter)
                (order "article_id")
                range)
            (parseList $ parseOne Article)

performStorage (TagCreate name) = runTransactionRelaxed $ do
    tagRef <- Reference <$> generateBytes 4
    doQuery
        (Insert "sn_tags"
            (fReference "tag_id" :/ fText "tag_name" :/ E)
            (Value tagRef :/ Value name :/ E)
            E)
        (parseInsert $ Tag tagRef name)
performStorage (TagSetName tagRef name) = runTransactionRelaxed $ do
    doQuery
        (Update "sn_tags"
            (fText "tag_name" :/ E)
            (Value name :/ E)
            [WhereIs tagRef "tag_id"])
        parseCount
performStorage (TagDelete tagRef) = runTransactionRelaxed $ do
    doQuery
        (Delete "sn_tags"
            [WhereIs tagRef "tag_id"])
        parseCount
performStorage (TagList view) = runTransactionRelaxed $ do
    withView view $ \tables filter order range time -> do
        doQuery
            (Select ("sn_tags" : tables)
                (fReference "tag_id" :/ fText "tag_name" :/ E)
                filter
                (order "tag_id")
                range)
            (parseList $ parseOne Tag)

performStorage (CommentCreate articleRef userRef text) = runTransactionRelaxed $ do
    commentRef <- Reference <$> generateBytes 16
    commentDate <- currentTime
    doQuery
        (Insert "sn_comments"
            (  fReference "comment_id"
            :/ fReference "comment_article_id"
            :/ fReference "comment_user_id"
            :/ fText "comment_text"
            :/ fTime "comment_date"
            :/ E)
            (  Value commentRef
            :/ Value articleRef
            :/ Value userRef
            :/ Value text
            :/ Value commentDate
            :/ E)
            E)
        (parseInsert $ Comment commentRef articleRef userRef text commentDate Nothing)
performStorage (CommentSetText commentRef text) = runTransactionRelaxed $ do
    editDate <- currentTime
    doQuery
        (Update "sn_comments"
            (fText "comment_text" :/ fTime "comment_date" :/ E)
            (Value text :/ Value editDate :/ E)
            [WhereIs commentRef "comment_id"])
        parseCount
performStorage (CommentDelete commentRef) = runTransactionRelaxed $ do
    doQuery
        (Delete "sn_comments"
            [WhereIs commentRef "comment_id"])
        parseCount
performStorage (CommentList view) = runTransactionRelaxed $ do
    withView view $ \tables filter order range time -> do
        doQuery
            (Select ("sn_comments" : tables)
                (  fReference "comment_id"
                :/ fReference "comment_article_id"
                :/ fReference "comment_user_id"
                :/ fText "comment_text"
                :/ fTime "comment_date"
                :/ fTime "comment_edit_date"
                :/ E)
                filter
                (order "comment_id")
                range)
            (parseList $ \case
                    Just commentRef :/ Just articleRef :/ Just userRef :/ Just text :/ Just commentDate :/ maybeEditDate :/ E ->
                        Just $ Comment commentRef articleRef userRef text commentDate maybeEditDate
                    _ -> Nothing)

hashAccessKey :: AccessKey -> BS.ByteString
hashAccessKey (AccessKey (Reference front) back) = do
    let ctx0 = CHash.hashInitWith CHash.SHA3_256
    let ctx1 = CHash.hashUpdate ctx0 back
    let ctx2 = CHash.hashUpdate ctx1 front
    BA.convert $ CHash.hashFinalize ctx2

type Transaction = ReaderT SqlStorage (ExceptT StorageError (ExceptT Db.QueryError IO))
{- internal: SqlStorage -> IO (Either Db.QueryError (Either StorageError r)) -}

generateBytes :: Int -> Transaction BS.ByteString
generateBytes len = ReaderT $ \SqlStorage {storageGen = tgen} -> do
    liftIO $ atomicModifyIORef' tgen $ \gen1 ->
        let (value, gen2) = CRand.randomBytesGenerate len gen1
        in (gen2, value)

doQuery :: Query qr -> (qr -> Either StorageError r) -> Transaction r
doQuery query mapf = ReaderT $ \SqlStorage {storageDb = db} -> do
    ExceptT $ fmap mapf $ ExceptT $ Db.makeQuery db query

doQuery_ :: Query qr -> Transaction ()
doQuery_ query = doQuery query (const $ Right ())

runTransaction :: Db.TransactionLevel -> Transaction r -> SqlStorage -> IO (Either StorageError r)
runTransaction level body storage = doTry 100
  where
    doTry 0 = return $ Left InternalError
    doTry n = do
        tret <- Db.withTransaction (storageDb storage) level $ \db2 -> do
            runExceptT $ runExceptT $ runReaderT body $ storage {storageDb = db2}
        case tret of
            Right bret -> return bret
            Left Db.QueryError -> return $ Left InternalError
            Left Db.SerializationError -> doTry (n-1)

runTransactionRelaxed :: Transaction r -> SqlStorage -> IO (Either StorageError r)
runTransactionRelaxed = runTransaction Db.ReadCommited

runTransactionSerial :: Transaction r -> SqlStorage -> IO (Either StorageError r)
runTransactionSerial = runTransaction Db.Serializable

parseInsert :: a -> Maybe (HList Maybe '[]) -> Either StorageError a
parseInsert x (Just E) = Right x
parseInsert _ Nothing = Left InternalError

parseCount :: Int64 -> Either StorageError ()
parseCount 1 = Right ()
parseCount 0 = Left NotFoundError
parseCount _ = Left InternalError

parseList :: (HList Maybe ts -> Maybe b) -> [HList Maybe ts] -> Either StorageError [b]
parseList func rs = Right $ mapMaybe func rs

parseHead :: (HList Maybe ts -> Maybe b) -> [HList Maybe ts] -> Either StorageError b
parseHead func [r] = case func r of
    Just x -> Right x
    Nothing -> Left InternalError
parseHead _ [] = Left NotFoundError
parseHead _ _ = Left InternalError

currentTime :: MonadIO m => m UTCTime
currentTime = do
    UTCTime uDay uTime <- liftIO getCurrentTime
    let tpsec1 = diffTimeToPicoseconds uTime
    let tpsec2 = (tpsec1 `div` rounding) * rounding
    return $ UTCTime uDay (picosecondsToDiffTime tpsec2)
  where
    rounding = 1000000000 -- 1 millisecond

type family Returning r ts where
    Returning r '[] = r
    Returning r (t ': ts) = t -> Returning r ts

parseOne :: Returning r ts -> HList Maybe ts -> Maybe r
parseOne f (Just x :/ xs) = parseOne (f x) xs
parseOne _ (Nothing :/ xs) = Nothing
parseOne x E = Just x

instance IsValue (Reference a) where
    type Prims (Reference a) = '[ 'TBlob ]
    primDecode (VBlob x :/ E) = Just $ Reference x
    primDecode (VNull :/ E) = Just $ Reference ""
    primEncode (Reference "") = VNull :/ E
    primEncode (Reference bstr) = VBlob bstr :/ E

fReference :: FieldName -> Field (Reference a)
fReference fieldName = Field (FBlob fieldName :/ E)

instance IsValue (Version a) where
    type Prims (Version a) = '[ 'TBlob ]
    primDecode (VBlob version :/ E) = Just $ Version version
    primDecode _ = Nothing
    primEncode (Version version) = VBlob version :/ E

fVersion :: FieldName -> Field (Version a)
fVersion fieldName = Field (FBlob fieldName :/ E)

instance IsValue PublicationStatus where
    type Prims PublicationStatus = '[ 'TTime ]
    primDecode (VTime date :/ E) = Just $ PublishAt date
    primDecode (_ :/ E) = Just $ NonPublished
    primEncode (PublishAt date) = VTime date :/ E
    primEncode NonPublished = VTPosInf :/ E

fPublicationStatus :: FieldName -> Field PublicationStatus
fPublicationStatus fieldName = Field (FTime fieldName :/ E)

class ListableObject a where
    applyFilter :: ViewFilter a -> QueryDemand
    applyOrder :: ViewOrder a -> OrderDirection -> QueryDemand

instance ListableObject User where
    applyFilter (FilterUserId ref) = demandCondition (WhereIs ref "user_id")
    applyFilter (FilterUserIsAdmin True) = demandCondition (Where "user_is_admin")
    applyFilter (FilterUserIsAdmin False) = demandCondition (Where "NOT user_is_admin")
    applyFilter (FilterUserAuthorId ref) = demandCondition (WhereIs ref "a2u_author_id")
        <> demandJoin "sn_author2user" (Where "a2u_user_id = user_id")
    applyOrder OrderUserName = demandOrder "user_name"
    applyOrder OrderUserSurname = demandOrder "user_surname"
    applyOrder OrderUserJoinDate = demandOrder "user_join_date" . inverseDirection
    applyOrder OrderUserIsAdmin = demandOrder "user_is_admin" . inverseDirection

instance ListableObject AccessKey where
    applyFilter = \case {}
    applyOrder = \case {}

instance ListableObject Author where
    applyFilter (FilterAuthorId ref) = demandCondition (WhereIs ref "author_id")
    applyFilter (FilterAuthorUserId ref) = demandCondition (WhereIs ref "a2u_user_id")
        <> demandJoin "sn_author2user" (Where "a2u_author_id = author_id")
    applyOrder OrderAuthorName = demandOrder "author_name"

instance ListableObject Category where
    applyFilter (FilterCategoryId ref) = demandCondition (WhereIs ref "category_id")
    applyFilter (FilterCategoryParentId ref) = demandCondition (WhereIs ref "category_parent_id")
    applyFilter (FilterCategoryTransitiveParentId ref) = demandJoin (subcategoriesSource ref) (Where "category_id = subcategory_id")
    applyOrder OrderCategoryName = demandOrder "category_name"

instance ListableObject Article where
    applyFilter (FilterArticleId ref) = demandCondition (WhereIs ref "article_id")
    applyFilter (FilterArticleAuthorId ref) = demandCondition (WhereIs ref "article_author_id")
    applyFilter (FilterArticleUserId ref) = demandCondition (WhereIs ref "a2u_user_id")
        <> demandJoin "sn_author2user" (Where "a2u_author_id = article_author_id")
    applyFilter (FilterArticleCategoryId ref) = demandCondition (WhereIs ref "article_category_id")
    applyFilter (FilterArticleTransitiveCategoryId ref) = demandJoin (subcategoriesSource ref) (Where "article_category_id = subcategory_id")
    applyFilter (FilterArticlePublishedBefore end) = demandCondition (WhereWith end "article_publication_date < ?")
    applyFilter (FilterArticlePublishedAfter begin) = demandCondition (WhereWith begin "article_publication_date >= ?")
    applyFilter (FilterArticleTagId []) = mempty
    applyFilter (FilterArticleTagId [ref]) = demandCondition (WhereIs ref "a2t_tag_id")
        <> demandJoin "sn_article2tag" (Where "a2t_article_id = article_id")
    applyFilter (FilterArticleTagId tagRefs) = demandCondition (WhereWithList "EXISTS (SELECT * FROM sn_article2tag WHERE a2t_article_id = article_id AND a2t_tag_id IN " tagRefs ")")
    applyOrder OrderArticleName = demandOrder "article_name"
    applyOrder OrderArticleDate = demandOrder "article_publication_date"
    applyOrder OrderArticleAuthorName = demandOrder "author_name"
        <> pure (demandJoin "sn_authors" (Where "author_id = article_author_id"))
    applyOrder OrderArticleCategoryName = demandOrder "category_name"
        <> pure (demandJoin "sn_categories" (Where "category_id = article_category_id"))

instance ListableObject Tag where
    applyFilter (FilterTagId ref) = demandCondition (WhereIs ref "tag_id")
    applyFilter (FilterTagArticleId ref) = demandCondition (WhereIs ref "a2t_article_id")
        <> demandJoin "sn_article2tag" (Where "a2t_tag_id = tag_id")
    applyOrder OrderTagName = demandOrder "tag_name"

instance ListableObject Comment where
    applyFilter (FilterCommentId ref) = demandCondition (WhereIs ref "comment_id")
    applyFilter (FilterCommentArticleId ref) = demandCondition (WhereIs ref "comment_article_id")
    applyFilter (FilterCommentUserId ref) = demandCondition (WhereIs ref "comment_user_id")
    applyOrder OrderCommentDate = demandOrder "comment_date" . inverseDirection

subcategoriesSource :: Reference Category -> RowSource
subcategoriesSource ref = RecursiveSource "subcategories"
    (fReference "subcategory_id" :/ E)
    (Select ["sn_categories"] (fReference "category_id" :/ E) [WhereIs ref "category_id"] [] AllRows)
    (Select ["sn_categories", "subcategories"] (fReference "category_id" :/ E) [Where "category_parent_id = subcategory_id"] [] AllRows)

data JoinDemand = JoinDemand RowSource (Endo [Condition])

instance Eq JoinDemand where
    JoinDemand (TableSource t1) _ == JoinDemand (TableSource t2) _ = t1 == t2
    JoinDemand (RecursiveSource t1 _ _ _) _ == JoinDemand (RecursiveSource t2 _ _ _) _ = t1 == t2
    _ == _ = False

newtype QueryDemand = QueryDemand
    { withQueryDemand :: forall r. UTCTime
        -> (Endo [JoinDemand] -> Endo [Condition] -> Endo [RowOrder] -> r)
        -> r
    }

instance Semigroup QueryDemand where
    QueryDemand b1 <> QueryDemand b2 = QueryDemand $ \time cont ->
        b1 time $ \j1 c1 o1 ->
            b2 time $ \j2 c2 o2 ->
                cont (j1 <> j2) (c1 <> c2) (o1 <> o2)

instance Monoid QueryDemand where
    mempty = QueryDemand $ \_ cont -> cont mempty mempty mempty

demandJoin :: RowSource -> Condition -> QueryDemand
demandJoin source cond = QueryDemand $ \_ cont -> do
    let demand = JoinDemand source (Endo (cond :))
    cont (Endo (demand :)) mempty mempty

demandCondition :: Condition -> QueryDemand
demandCondition cond = QueryDemand $ \_ cont ->
    cont mempty (Endo (cond :)) mempty

demandOrder :: FieldName -> OrderDirection -> QueryDemand
demandOrder field dir = QueryDemand $ \_ cont ->
    cont mempty mempty (Endo (order :))
  where
    order = case dir of
        Ascending -> Asc field
        Descending -> Desc field

inverseDirection :: OrderDirection -> OrderDirection
inverseDirection Ascending = Descending
inverseDirection Descending = Ascending

withView
    :: (ListableObject a, MonadIO m)
    => ListView a
    -> ([RowSource] -> [Condition] -> (FieldName -> [RowOrder]) -> RowRange -> UTCTime -> m r)
    -> m r
withView view cont = do
    let demand = foldMap applyFilter (viewFilter view) <> foldMap (uncurry applyOrder) (viewOrder view)
    time <- currentTime
    withQueryDemand demand time $ \joinEndo filterEndo orderEndo -> do
        let joins = nub (appEndo joinEndo [])
        let (joinSources, joinFilters) = unzip $ map (\(JoinDemand source filter) -> (source, filter)) joins
        cont
            joinSources
            (appEndo (mconcat joinFilters <> filterEndo) [])
            (\fieldName -> appEndo orderEndo [Asc fieldName])
            (RowRange (viewOffset view) (viewLimit view))
            time
