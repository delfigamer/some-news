{-# LANGUAGE StandaloneDeriving #-}
{-# LANGUAGE TemplateHaskell #-}

module SN.Ground.Interface
    ( Reference(..)
    , Version(..)
    , User(..)
    , Password(..)
    , AccessKey(..)
    , Author(..)
    , Category(..)
    , PublicationStatus(..)
    , Article(..)
    , Tag(..)
    , Comment(..)
    , FileInfo(..)
    , Upload(..)
    , Ground(..)
    , Action(..)
    , GroundError(..)
    , ListView(..)
    , OrderDirection(..)
    , ViewFilter(..)
    , ViewOrder(..)
    ) where

import Data.Int
import qualified Data.ByteString as BS
import qualified Data.Text as Text
import SN.Data.HEq1
import SN.Ground.ListView
import SN.Ground.Types

data Upload r
    = UploadAbort !r
    | UploadFinish !r
    | UploadChunk !BS.ByteString (IO (Upload r))

data Ground = Ground
    { groundPerform :: forall a. Action a -> IO (Either GroundError a)
    , groundGenerateBytes :: Int -> IO BS.ByteString
    , groundUpload :: forall r.
           Text.Text
        -> Text.Text
        -> Reference Article
        -> Reference User
        -> (FileInfo -> IO (Upload r))
        -> IO (Either GroundError r)
    , groundDownload :: forall r b.
           Reference FileInfo
        -> (GroundError -> IO r)
        -> (Int64 -> Text.Text -> ((BS.ByteString -> IO ()) -> IO ()) -> IO r)
        -> IO r
    }

data Action a where
    UserCreate :: Text.Text -> Text.Text -> Password -> Action User
    UserCheckPassword :: Reference User -> Password -> Action ()
    UserSetName :: Reference User -> Text.Text -> Text.Text -> Action ()
    UserSetIsAdmin :: Reference User -> Bool -> Action ()
    UserSetPassword :: Reference User -> Password -> Action ()
    UserDelete :: Reference User -> Action ()
    UserList :: ListView User -> Action [User]

    AccessKeyCreate :: Reference User -> Action AccessKey
    AccessKeyClear :: Reference User -> Action ()
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
    ArticleList :: ListView Article -> Action [Article]
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

data GroundError
    = NotFoundError
    | VersionError
    | InvalidRequestError
    | InternalError
    deriving (Show, Eq, Ord)

declareHEq1Instance ''Action
