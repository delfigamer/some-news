{-# LANGUAGE EmptyCase #-}
{-# LANGUAGE EmptyDataDeriving #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE StandaloneDeriving #-}
{-# LANGUAGE TemplateHaskell #-}
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
    , FileInfo(..)
    , Upload(..)
    , InitFailure(..)
    , Storage(..)
    , withSqlStorage
    , currentSchema
    , upgradeSchema
    , Action(..)
    , StorageError(..)
    , ListView(..)
    , ViewFilter(..)
    , ViewOrder(..)
    , OrderDirection(..)
    ) where

import Control.Applicative
import Control.Exception
import Control.Monad
import Control.Monad.Except
import Control.Monad.Reader
import Data.Bits
import Data.Hashable
import Data.IORef
import Data.Int
import Data.List
import Data.Maybe
import Data.Proxy
import Data.Semigroup
import Data.String
import Data.Time.Clock
import Data.Word
import GHC.Generics (Generic)
import qualified Crypto.Hash as CHash
import qualified Crypto.Random as CRand
import qualified Data.ByteArray as BA
import qualified Data.ByteString as BS
import qualified Data.Text as Text
import HEq1
import Logger
import Sql.Query
import Storage.Schema
import Tuple
import qualified Sql.Database as Db

newtype Reference a = Reference
    { getReference :: BS.ByteString
    }
    deriving (Eq, Ord, IsString, Hashable)

instance Show (Reference a) where
    showsPrec _ (Reference x) = showBlob "ref" x

newtype Version a = Version
    { getVersion :: BS.ByteString
    }
    deriving (Eq, Ord, IsString, Hashable)

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
    deriving (Eq, Ord)

instance Show AccessKey where
    showsPrec d ak@(AccessKey ref _) = showParen (d > 10)
        $ showString "AccessKey "
        . showsPrec 11 ref
        . showString " "
        . showBlob "hash" (hashAccessKey ak)

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
    deriving (Show, Eq, Ord)

data FileInfo = FileInfo
    { fileId :: !(Reference FileInfo)
    , fileName :: !Text.Text
    , fileMimeType :: !Text.Text
    , fileUploadDate :: !UTCTime
    , fileArticle :: !(Reference Article)
    , fileIndex :: !Int64
    , fileUser :: !(Reference User)
    }
    deriving (Show, Eq, Ord)

data Upload r
    = UploadAbort !r
    | UploadFinish !r
    | UploadChunk !BS.ByteString (IO (Upload r))

data Storage = Storage
    { storagePerform :: forall a. Action a -> IO (Either StorageError a)
    , storageGenerateBytes :: Int -> IO BS.ByteString
    , storageUpload :: forall r.
           Text.Text
        -> Text.Text
        -> Reference Article
        -> Reference User
        -> (FileInfo -> IO (Upload r))
        -> IO (Either StorageError r)
    , storageDownload :: forall r b.
           Reference FileInfo
        -> (StorageError -> IO r)
        -> (Int64 -> ((BS.ByteString -> IO ()) -> IO ()) -> IO r)
        -> IO r
    }

data SqlStorage = SqlStorage
    { storageLogger :: Logger
    , storageDb :: Db.Database
    , storageGen :: IORef CRand.ChaChaDRG
    }

withSqlStorage :: Logger -> Db.Database -> (InitFailure -> IO r) -> (Storage -> IO r) -> IO r
withSqlStorage logger db onFail onSuccess = do
    matchCurrentSchema logger db onFail $ do
        pgen <- newIORef =<< CRand.drgNew
        let storage = SqlStorage logger db pgen
        onSuccess $ Storage
            { storagePerform = \action -> do
                logDebug logger $ "SqlStorage: Request: " <<| action
                sqlStoragePerform action storage $ \result -> do
                    {- use CPS to bring (Show r) into scope -}
                    logSend logger (resultLogLevel result) $
                        "SqlStorage: Request finished: " <<| action << " -> " <<| result
                    return result
            , storageGenerateBytes = \count -> do
                generateBytes count `runReaderT` storage
            , storageUpload = sqlStorageUpload storage
            , storageDownload = sqlStorageDownload storage
            }
  where
    resultLogLevel (Left InternalError) = LevelWarn
    resultLogLevel _ = LevelDebug

data Action a where
    UserCreate :: Text.Text -> Text.Text -> Action User
    UserSetName :: Reference User -> Text.Text -> Text.Text -> Action ()
    UserSetIsAdmin :: Reference User -> Bool -> Action ()
    UserDelete :: Reference User -> Action ()
    UserList :: ListView User -> Action [User]

    AccessKeyCreate :: Reference User -> Action AccessKey
    AccessKeyDelete :: Reference User -> Reference AccessKey -> Action ()
    AccessKeyList :: Reference User -> ListView AccessKey -> Action [Reference AccessKey]

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
    ArticleGetText :: Reference Article -> Action Text.Text
    ArticleSetAuthor :: Reference Article -> Reference Author -> Action ()
    ArticleSetName :: Reference Article -> Text.Text -> Action ()
    ArticleSetText :: Reference Article -> Version Article -> Text.Text -> Action (Version Article)
    ArticleSetCategory :: Reference Article -> Reference Category -> Action ()
    ArticleSetPublicationStatus :: Reference Article -> PublicationStatus -> Action ()
    ArticleDelete :: Reference Article -> Action ()
    ArticleList :: Bool -> ListView Article -> Action [Article]
    ArticleSetTag :: Reference Article -> Reference Tag -> Bool -> Action ()

    TagCreate :: Text.Text -> Action Tag
    TagSetName :: Reference Tag -> Text.Text -> Action ()
    TagDelete :: Reference Tag -> Action ()
    TagList :: ListView Tag -> Action [Tag]

    CommentCreate :: Reference Article -> Reference User -> Text.Text -> Action Comment
    CommentSetText :: Reference Comment -> Text.Text -> Action ()
    CommentDelete :: Reference Comment -> Action ()
    CommentList :: ListView Comment -> Action [Comment]

    FileSetName :: Reference FileInfo -> Text.Text -> Action ()
    FileSetIndex :: Reference FileInfo -> Int64 -> Action ()
    FileDelete :: Reference FileInfo -> Action ()
    FileList :: ListView FileInfo -> Action [FileInfo]
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
    | FilterUserAccessKey AccessKey
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
    | FilterArticleTagIds [Reference Tag]
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

data instance ViewFilter FileInfo
    = FilterFileId (Reference FileInfo)
    | FilterFileArticleId (Reference Article)
    | FilterFileUserId (Reference User)
    deriving (Show, Eq)

data instance ViewOrder FileInfo
    = OrderFileName
    | OrderFileMimeType
    | OrderFileUploadDate
    | OrderFileIndex
    deriving (Show, Eq)

data StorageError
    = NotFoundError
    | VersionError
    | InvalidRequestError
    | InternalError
    deriving (Show, Eq, Ord)

sqlStoragePerform :: Action a -> SqlStorage -> (Show a => Either StorageError a -> IO r) -> IO r
sqlStoragePerform (UserCreate name surname) = runTransaction Db.ReadCommited $ do
    ref <- Reference <$> generateBytes 16
    joinDate <- currentTime
    doQuery
        (Insert "sn_users"
            (fReference "user_id" :/ fText "user_name" :/ fText "user_surname" :/ fTime "user_join_date" :/ fBool "user_is_admin" :/ E)
            (Value ref :/ Value name :/ Value surname :/ Value joinDate :/ Value False :/ E))
        (parseInsert $ User ref name surname joinDate False)
sqlStoragePerform (UserSetName userRef name surname) = runTransaction Db.ReadCommited $ do
    doQuery
        (Update "sn_users"
            (fText "user_name" :/ fText "user_surname" :/ E)
            (Value name :/ Value surname :/ E)
            [WhereIs userRef "user_id"])
        parseCount
sqlStoragePerform (UserSetIsAdmin userRef isAdmin) = runTransaction Db.ReadCommited $ do
    doQuery
        (Update "sn_users"
            (fBool "user_is_admin" :/ E)
            (Value isAdmin :/ E)
            [WhereIs userRef "user_id"])
        parseCount
sqlStoragePerform (UserDelete userRef) = runTransaction Db.ReadCommited $ do
    doQuery
        (Delete "sn_users"
            [WhereIs userRef "user_id"])
        parseCount
sqlStoragePerform (UserList view) = runTransaction Db.ReadCommited $ do
    withView view $ \tables filter order range _ -> do
        doQuery
            (Select ("sn_users" : tables)
                (fReference "user_id" :/ fText "user_name" :/ fText "user_surname" :/ fTime "user_join_date" :/ fBool "user_is_admin" :/ E)
                filter
                (order "user_id")
                range)
            (parseList $ parseOne User)

sqlStoragePerform (AccessKeyCreate userRef) = runTransaction Db.ReadCommited $ do
    keyBack <- generateBytes 48
    keyFront <- Reference <$> generateBytes 16
    let key = AccessKey keyFront keyBack
    let keyHash = hashAccessKey key
    doQuery
        (Insert "sn_access_keys"
            (fReference "access_key_id" :/ fBlob "access_key_hash" :/ fReference "access_key_user_id" :/ E)
            (Value keyFront :/ Value keyHash :/ Value userRef :/ E))
        (parseInsert key)
sqlStoragePerform (AccessKeyDelete userRef keyRef) = runTransaction Db.ReadCommited $ do
    doQuery
        (Delete "sn_access_keys"
            [WhereIs userRef "access_key_user_id", WhereIs keyRef "access_key_id"])
        parseCount
sqlStoragePerform (AccessKeyList userRef view) = runTransaction Db.ReadCommited $ do
    withView view $ \tables filter order range _ -> do
        doQuery
            (Select ("sn_access_keys" : tables)
                (fReference "access_key_id" :/ E)
                (WhereIs userRef "access_key_user_id" : filter)
                (order "access_key_id")
                range)
            (parseList $ parseOne id)

sqlStoragePerform (AuthorCreate name description) = runTransaction Db.ReadCommited $ do
    ref <- Reference <$> generateBytes 16
    doQuery
        (Insert "sn_authors"
            (fReference "author_id" :/ fText "author_name" :/ fText "author_description" :/ E)
            (Value ref :/ Value name :/ Value description :/ E))
        (parseInsert $ Author ref name description)
sqlStoragePerform (AuthorSetName authorRef name) = runTransaction Db.ReadCommited $ do
    doQuery
        (Update "sn_authors"
            (fText "author_name" :/ E)
            (Value name :/ E)
            [WhereIs authorRef "author_id"])
        parseCount
sqlStoragePerform (AuthorSetDescription authorRef description) = runTransaction Db.ReadCommited $ do
    doQuery
        (Update "sn_authors"
            (fText "author_description" :/ E)
            (Value description :/ E)
            [WhereIs authorRef "author_id"])
        parseCount
sqlStoragePerform (AuthorDelete authorRef) = runTransaction Db.ReadCommited $ do
    doQuery_
        (Delete "sn_articles"
            [WhereIs authorRef "article_author_id", WhereWith NonPublished "article_publication_date = ?"])
    doQuery
        (Delete "sn_authors"
            [WhereIs authorRef "author_id"])
        parseCount
sqlStoragePerform (AuthorList view) = runTransaction Db.ReadCommited $ do
    withView view $ \tables filter order range _ -> do
        doQuery
            (Select ("sn_authors" : tables)
                (fReference "author_id" :/ fText "author_name" :/ fText "author_description" :/ E)
                filter
                (order "author_id")
                range)
            (parseList $ parseOne Author)
sqlStoragePerform (AuthorSetOwnership authorRef userRef True) = runTransaction Db.ReadCommited $ do
    doQuery_
        (Insert "sn_author2user"
            (fReference "a2u_user_id" :/ fReference "a2u_author_id" :/ E)
            (Value userRef :/ Value authorRef :/ E))
sqlStoragePerform (AuthorSetOwnership authorRef userRef False) = runTransaction Db.ReadCommited $ do
    doQuery_
        (Delete "sn_author2user"
            [WhereIs userRef "a2u_user_id", WhereIs authorRef "a2u_author_id"])

sqlStoragePerform (CategoryCreate name parentRef) = runTransaction Db.ReadCommited $ do
    ref <- Reference <$> generateBytes 4
    doQuery
        (Insert "sn_categories"
            (fReference "category_id" :/ fText "category_name" :/ fReference "category_parent_id" :/ E)
            (Value ref :/ Value name :/ Value parentRef :/ E))
        (parseInsert $ Category ref name parentRef)
sqlStoragePerform (CategorySetName categoryRef name) = runTransaction Db.ReadCommited $ do
    doQuery
        (Update "sn_categories"
            (fText "category_name" :/ E)
            (Value name :/ E)
            [WhereIs categoryRef "category_id"])
        parseCount
sqlStoragePerform (CategorySetParent categoryRef parentRef) = runTransaction Db.Serializable $ do
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
sqlStoragePerform (CategoryDelete categoryRef) = runTransaction Db.Serializable $ do
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
sqlStoragePerform (CategoryList view) = runTransaction Db.ReadCommited $ do
    withView view $ \tables filter order range _ -> do
        doQuery
            (Select ("sn_categories" : tables)
                (fReference "category_id" :/ fText "category_name" :/ fReference "category_parent_id" :/ E)
                filter
                (order "category_id")
                range)
            (parseList $ parseOne Category)

sqlStoragePerform (ArticleCreate authorRef) = runTransaction Db.ReadCommited $ do
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
            :/ E))
        (parseInsert $ Article articleRef articleVersion authorRef "" NonPublished (Reference ""))
sqlStoragePerform (ArticleGetText articleRef) = runTransaction Db.ReadCommited $ do
    doQuery
        (Select ["sn_articles"]
            (fText "article_text" :/ E)
            [WhereIs articleRef "article_id"]
            []
            (RowRange 0 1))
        (parseHead $ parseOne id)
sqlStoragePerform (ArticleSetAuthor articleRef authorRef) = runTransaction Db.ReadCommited $ do
    doQuery
        (Update "sn_articles"
            (fReference "article_author_id" :/ E)
            (Value authorRef :/ E)
            [WhereIs articleRef "article_id"])
        parseCount
sqlStoragePerform (ArticleSetName articleRef name) = runTransaction Db.ReadCommited $ do
    doQuery
        (Update "sn_articles"
            (fText "article_name" :/ E)
            (Value name :/ E)
            [WhereIs articleRef "article_id"])
        parseCount
sqlStoragePerform (ArticleSetText articleRef oldVersion text) = runTransaction Db.Serializable $ do
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
sqlStoragePerform (ArticleSetCategory articleRef categoryRef) = runTransaction Db.ReadCommited $ do
    doQuery
        (Update "sn_articles"
            (fReference "article_category_id" :/ E)
            (Value categoryRef :/ E)
            [WhereIs articleRef "article_id"])
        parseCount
sqlStoragePerform (ArticleSetPublicationStatus articleRef pubStatus) = runTransaction Db.ReadCommited $ do
    doQuery
        (Update "sn_articles"
            (fPublicationStatus "article_publication_date" :/ E)
            (Value pubStatus :/ E)
            [WhereIs articleRef "article_id"])
        parseCount
sqlStoragePerform (ArticleDelete articleRef) = runTransaction Db.ReadCommited $ do
    doQuery
        (Delete "sn_articles"
            [WhereIs articleRef "article_id"])
        parseCount
sqlStoragePerform (ArticleList showDrafts view) = runTransaction Db.ReadCommited $ do
    withView view $ \tables filter order range time -> do
        doQuery
            (Select ("sn_articles" : tables)
                (  fReference "article_id"
                :/ fVersion "article_version"
                :/ fReference "article_author_id"
                :/ fText "article_name"
                :/ fPublicationStatus "article_publication_date"
                :/ fReference "article_category_id"
                :/ E)
                ([WhereWith time "article_publication_date <= ?" | not showDrafts] ++ filter)
                (order "article_id")
                range)
            (parseList $ parseOne Article)
sqlStoragePerform (ArticleSetTag articleRef tagRef True) = runTransaction Db.ReadCommited $ do
    doQuery_
        (Insert "sn_article2tag"
            (fReference "a2t_article_id" :/ fReference "a2t_tag_id" :/ E)
            (Value articleRef :/ Value tagRef :/ E))
sqlStoragePerform (ArticleSetTag articleRef tagRef False) = runTransaction Db.ReadCommited $ do
    doQuery_
        (Delete "sn_article2tag"
            [WhereIs articleRef "a2t_article_id", WhereIs tagRef "a2t_tag_id"])

sqlStoragePerform (TagCreate name) = runTransaction Db.ReadCommited $ do
    tagRef <- Reference <$> generateBytes 4
    doQuery
        (Insert "sn_tags"
            (fReference "tag_id" :/ fText "tag_name" :/ E)
            (Value tagRef :/ Value name :/ E))
        (parseInsert $ Tag tagRef name)
sqlStoragePerform (TagSetName tagRef name) = runTransaction Db.ReadCommited $ do
    doQuery
        (Update "sn_tags"
            (fText "tag_name" :/ E)
            (Value name :/ E)
            [WhereIs tagRef "tag_id"])
        parseCount
sqlStoragePerform (TagDelete tagRef) = runTransaction Db.ReadCommited $ do
    doQuery
        (Delete "sn_tags"
            [WhereIs tagRef "tag_id"])
        parseCount
sqlStoragePerform (TagList view) = runTransaction Db.ReadCommited $ do
    withView view $ \tables filter order range time -> do
        doQuery
            (Select ("sn_tags" : tables)
                (fReference "tag_id" :/ fText "tag_name" :/ E)
                filter
                (order "tag_id")
                range)
            (parseList $ parseOne Tag)

sqlStoragePerform (CommentCreate articleRef userRef text) = runTransaction Db.ReadCommited $ do
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
            :/ E))
        (parseInsert $ Comment commentRef articleRef userRef text commentDate Nothing)
sqlStoragePerform (CommentSetText commentRef text) = runTransaction Db.ReadCommited $ do
    editDate <- currentTime
    doQuery
        (Update "sn_comments"
            (fText "comment_text" :/ fTime "comment_edit_date" :/ E)
            (Value text :/ Value editDate :/ E)
            [WhereIs commentRef "comment_id"])
        parseCount
sqlStoragePerform (CommentDelete commentRef) = runTransaction Db.ReadCommited $ do
    doQuery
        (Delete "sn_comments"
            [WhereIs commentRef "comment_id"])
        parseCount
sqlStoragePerform (CommentList view) = runTransaction Db.ReadCommited $ do
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

sqlStoragePerform (FileSetName fileRef name) = runTransaction Db.ReadCommited $ do
    doQuery
        (Update "sn_files"
            (fText "file_name" :/ E)
            (Value name :/ E)
            [WhereIs fileRef "file_id"])
        parseCount
sqlStoragePerform (FileSetIndex fileRef index) = runTransaction Db.ReadCommited $ do
    doQuery
        (Update "sn_files"
            (fInt "file_index" :/ E)
            (Value index :/ E)
            [WhereIs fileRef "file_id"])
        parseCount
sqlStoragePerform (FileDelete fileRef) = runTransaction Db.ReadCommited $ do
    doQuery
        (Delete "sn_files"
            [WhereIs fileRef "file_id"])
        parseCount
sqlStoragePerform (FileList view) = runTransaction Db.ReadCommited $ do
    withView view $ \tables filter order range time -> do
        doQuery
            (Select ("sn_files" : tables)
                (  fReference "file_id"
                :/ fText "file_name"
                :/ fText "file_mimetype"
                :/ fTime "file_upload_date"
                :/ fReference "file_article_id"
                :/ fInt "file_index"
                :/ fReference "file_user_id"
                :/ E)
                filter
                (order "file_id")
                range)
            (parseList $ parseOne FileInfo)

sqlStorageUpload
    :: SqlStorage
    -> Text.Text
    -> Text.Text
    -> Reference Article
    -> Reference User
    -> (FileInfo -> IO (Upload r))
    -> IO (Either StorageError r)
sqlStorageUpload storage name mimeType articleRef userRef uploader = do
    tret <- Db.execute (storageDb storage) Db.ReadCommited $ do
        uploadTime <- currentTime
        fileRef <- (Reference <$> generateBytes 16) `runReaderT` storage
        maxIndexQret <- Db.query
            (Select ["sn_files"]
                (fInt "COALESCE(MAX(file_index), 0)" :/ E)
                [WhereIs articleRef "file_article_id"]
                []
                (RowRange 0 1))
        maxIndex <- case maxIndexQret of
            [Just i :/ E] -> return i
            _ -> Db.abort
        {- since this transaction is relaxed, it's possible for multiple files to end up with the same (`article_id`, `index`) pair -}
        {- this is fine -}
        {- for identification, we use an independent `id` field, which is under a primary key constraint -}
        {- for ordering, we always append an `id ASC` to the end of the "order by" clause, so it's still always deterministic -}
        {- if the user isn't happy with whatever order the files turned out to be, they can always rearrange the indices later -}
        let myIndex = maxIndex + 1
        insertQret <- Db.query
            (Insert "sn_files"
                (  fReference "file_id"
                :/ fText "file_name"
                :/ fText "file_mimetype"
                :/ fTime "file_upload_date"
                :/ fReference "file_article_id"
                :/ fInt "file_index"
                :/ fReference "file_user_id"
                :/ E)
                (  Value fileRef
                :/ Value name
                :/ Value mimeType
                :/ Value uploadTime
                :/ Value articleRef
                :/ Value myIndex
                :/ Value userRef
                :/ E))
        unless insertQret $ Db.abort
        driveUpload fileRef 0 $ uploader $ FileInfo fileRef name mimeType uploadTime articleRef myIndex userRef
    case tret of
        Right bret -> return bret
        Left _ -> return $ Left InternalError
  where
    driveUpload fileRef chunkIndex action = do
        aret <- liftIO action
        case aret of
            UploadAbort result -> do
                Db.rollback $ Right result
            UploadFinish result -> do
                return $ Right result
            UploadChunk chunkData next -> do
                qret <- Db.query
                    (Insert "sn_file_chunks"
                        (fReference "chunk_file_id" :/ fInt "chunk_index" :/ fBlob "chunk_data" :/ E)
                        (Value fileRef :/ Value chunkIndex :/ Value chunkData :/ E))
                unless qret $ Db.abort
                driveUpload fileRef (chunkIndex + 1) next

sqlStorageDownload
    :: SqlStorage
    -> Reference FileInfo
    -> (StorageError -> IO r)
    -> (Int64 -> ((BS.ByteString -> IO ()) -> IO ()) -> IO r)
    -> IO r
sqlStorageDownload storage fileRef onError onStream = do
    pmResult <- newIORef Nothing
    tret <- Db.execute (storageDb storage) Db.RepeatableRead $ do
        filesizeQret <- Db.query
            (Select
                ["sn_file_chunks"]
                (fInt "sum(length(chunk_data))" :/ E)
                [WhereIs fileRef "chunk_file_id"]
                []
                (RowRange 0 1))
        filesize <- case filesizeQret of
            [Just filesize :/ E] -> return filesize
            [Nothing :/ E] -> Db.rollback $ Left NotFoundError
            _ -> Db.abort
        sink <- Db.mshift $ \cont -> do
            pOut <- newIORef $ error "invalid onStream callback"
            result <- onStream filesize $ \sink -> do
                cont sink >>= writeIORef pOut
            writeIORef pmResult $ Just result
            readIORef pOut
        driveDownloader 0 sink
    mResult <- readIORef pmResult
    case mResult of
        Just result -> return result
        Nothing -> case tret of
            Right (Left err) -> onError err
            _ -> onError InternalError
  where
    driveDownloader index sink = do
        chunkQret <- Db.query
            (Select
                ["sn_file_chunks"]
                (fBlob "chunk_data" :/ E)
                [WhereIs fileRef "chunk_file_id"]
                [Asc "chunk_index"]
                (RowRange index 1))
        case chunkQret of
            [] -> return $ Right ()
            [Just chunkData :/ E] -> do
                liftIO $ sink chunkData
                driveDownloader (index + 1) sink
            _ -> Db.abort

hashAccessKey :: AccessKey -> BS.ByteString
hashAccessKey (AccessKey (Reference front) back) = do
    let ctx0 = CHash.hashInitWith CHash.SHA3_256
    let ctx1 = CHash.hashUpdate ctx0 back
    let ctx2 = CHash.hashUpdate ctx1 front
    BA.convert $ CHash.hashFinalize ctx2

type Transaction a = ReaderT SqlStorage (Db.Transaction (Either StorageError a))

generateBytes :: (MonadReader SqlStorage m, MonadIO m) => Int -> m BS.ByteString
generateBytes len = do
    tgen <- storageGen <$> ask
    liftIO $ atomicModifyIORef' tgen $ \gen1 ->
        let (value, gen2) = CRand.randomBytesGenerate len gen1
        in (gen2, value)

doQuery :: Query qr -> (qr -> Either StorageError r) -> Transaction a r
doQuery query parser = ReaderT $ \_ -> do
    qr <- Db.query query
    case parser qr of
        Left ste -> Db.rollback $ Left ste
        Right r -> return r

doQuery_ :: Query qr -> Transaction a ()
doQuery_ query = doQuery query (const $ Right ())

runTransaction :: Show a => Db.TransactionLevel -> Transaction a a -> SqlStorage -> (Show a => Either StorageError a -> IO r) -> IO r
runTransaction level body storage cont = do
    tret <- Db.executeRepeat (storageDb storage) 100 level $ do
        br <- body `runReaderT` storage
        return $ Right br
    case tret of
        Right bret -> cont bret
        Left _ -> cont $ Left InternalError

parseInsert :: a -> Bool -> Either StorageError a
parseInsert x True = Right x
parseInsert _ False = Left InternalError

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
        <> demandJoin "sn_author2user" [Where "a2u_user_id = user_id"]
    applyFilter (FilterUserAccessKey key@(AccessKey keyFront _)) =
        demandCondition (WhereIs keyFront "access_key_id")
            <> demandCondition (WhereIs (hashAccessKey key) "access_key_hash")
            <> demandJoin "sn_access_keys" [Where "access_key_user_id = user_id"]
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
        <> demandJoin "sn_author2user" [Where "a2u_author_id = author_id"]
    applyOrder OrderAuthorName = demandOrder "author_name"

instance ListableObject Category where
    applyFilter (FilterCategoryId ref) = demandCondition (WhereIs ref "category_id")
    applyFilter (FilterCategoryParentId ref) = demandCondition (WhereIs ref "category_parent_id")
    applyFilter (FilterCategoryTransitiveParentId ref) = demandJoin (subcategoriesSource ref) [Where "category_id = subcategory_id"]
    applyOrder OrderCategoryName = demandOrder "category_name"

instance ListableObject Article where
    applyFilter (FilterArticleId ref) = demandCondition (WhereIs ref "article_id")
    applyFilter (FilterArticleAuthorId ref) = demandCondition (WhereIs ref "article_author_id")
    applyFilter (FilterArticleUserId ref) = demandCondition (WhereIs ref "a2u_user_id")
        <> demandJoin (OuterJoinSource "sn_author2user" (WhereFieldIs "a2u_author_id" "article_author_id")) []
    applyFilter (FilterArticleCategoryId ref) = demandCondition (WhereIs ref "article_category_id")
    applyFilter (FilterArticleTransitiveCategoryId (Reference "")) = demandCondition (Where "article_category_id IS NULL")
    applyFilter (FilterArticleTransitiveCategoryId ref) = demandJoin (subcategoriesSource ref) [Where "article_category_id = subcategory_id"]
    applyFilter (FilterArticlePublishedBefore end) = demandCondition (WhereWith end "article_publication_date < ?")
    applyFilter (FilterArticlePublishedAfter begin) = demandCondition (WhereWith begin "article_publication_date >= ?")
    applyFilter (FilterArticleTagIds []) = mempty
    applyFilter (FilterArticleTagIds tagRefs) = demandCondition (WhereWithList "EXISTS (SELECT * FROM sn_article2tag WHERE a2t_article_id = article_id AND a2t_tag_id IN " tagRefs ")")
    applyOrder OrderArticleName = demandOrder "article_name"
    applyOrder OrderArticleDate = demandOrder "article_publication_date" . inverseDirection
    applyOrder OrderArticleAuthorName = demandOrder "COALESCE(author_name, '')"
        <> pure (demandJoin (OuterJoinSource "sn_authors" (WhereFieldIs "author_id" "article_author_id")) [])
    applyOrder OrderArticleCategoryName = demandOrder "COALESCE(category_name, '')"
        <> pure (demandJoin (OuterJoinSource "sn_categories" (WhereFieldIs "category_id" "article_category_id")) [])

instance ListableObject Tag where
    applyFilter (FilterTagId ref) = demandCondition (WhereIs ref "tag_id")
    applyFilter (FilterTagArticleId ref) = demandCondition (WhereIs ref "a2t_article_id")
        <> demandJoin "sn_article2tag" [Where "a2t_tag_id = tag_id"]
    applyOrder OrderTagName = demandOrder "tag_name"

instance ListableObject Comment where
    applyFilter (FilterCommentId ref) = demandCondition (WhereIs ref "comment_id")
    applyFilter (FilterCommentArticleId ref) = demandCondition (WhereIs ref "comment_article_id")
    applyFilter (FilterCommentUserId ref) = demandCondition (WhereIs ref "comment_user_id")
    applyOrder OrderCommentDate = demandOrder "comment_date" . inverseDirection

instance ListableObject FileInfo where
    applyFilter (FilterFileId ref) = demandCondition (WhereIs ref "file_id")
    applyFilter (FilterFileArticleId ref) = demandCondition (WhereIs ref "file_article_id")
    applyFilter (FilterFileUserId ref) = demandCondition (WhereIs ref "file_user_id")
    applyOrder OrderFileName = demandOrder "file_name"
    applyOrder OrderFileMimeType = demandOrder "file_mimetype"
    applyOrder OrderFileUploadDate = demandOrder "file_upload_date" . inverseDirection
    applyOrder OrderFileIndex = demandOrder "file_index"

subcategoriesSource :: Reference Category -> RowSource
subcategoriesSource ref = RecursiveSource "subcategories"
    (fReference "subcategory_id" :/ E)
    (Select ["sn_categories"] (fReference "category_id" :/ E) [WhereIs ref "category_id"] [] AllRows)
    (Select ["sn_categories", "subcategories"] (fReference "category_id" :/ E) [Where "category_parent_id = subcategory_id"] [] AllRows)

data JoinDemand = JoinDemand RowSource (Endo [Condition])

instance Eq JoinDemand where
    JoinDemand (TableSource t1) _ == JoinDemand (TableSource t2) _ = t1 == t2
    JoinDemand (OuterJoinSource t1 _) _ == JoinDemand (OuterJoinSource t2 _) _ = t1 == t2
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

demandJoin :: RowSource -> [Condition] -> QueryDemand
demandJoin source cond = QueryDemand $ \_ cont -> do
    let demand = JoinDemand source (Endo (cond ++))
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

declareHEq1Instance ''Action
