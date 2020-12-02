{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE TypeFamilies #-}

module JsonInterface.Response
    ( Accept(..)
    , Response(..)
    , ResponseStatus(..)
    , ErrorMessage(..)
    , ResponseBody(..)
    , OkResponseBody(..)
    , okResponse
    , errorResponse
    , Expanded(..)
    , Expandable(..)
    , Retrievable(..)
    , UploadStatus(..)
    , expandFileInfo'
    , expandUploadStatus'
    , toHexText
    ) where

import Control.Monad
import Data.Aeson
import Data.Int
import Data.Time.Clock
import qualified Data.Aeson.Encoding as Encoding
import qualified Data.ByteString as BS
import qualified Data.ByteString.Builder as Builder
import qualified Data.Text as Text
import qualified Data.Text.Encoding as TextEncoding
import Hex
import JsonInterface.ActionTicket
import Storage

data Accept r = Accept
    { acceptResponse :: Response -> IO r
    , acceptStream :: Int64 -> ((BS.ByteString -> IO ()) -> IO ()) -> IO r
    }

data Response = Response ResponseStatus ResponseBody
    deriving (Show, Eq)

data ResponseStatus
    = StatusOk
    | StatusBadRequest
    | StatusForbidden
    | StatusNotFound
    | StatusConflict
    | StatusInternalError
    deriving (Show, Eq)

data ErrorMessage
    = ErrAccessDenied
    | ErrInternal
    | ErrInvalidAccessKey
    | ErrInvalidRequest
    | ErrInvalidRequestMsg Text.Text
    | ErrInvalidParameter Text.Text
    | ErrMissingParameter Text.Text
    | ErrNotFound
    | ErrUnknownRequest
    | ErrVersionConflict
    deriving (Show, Eq)

errorMessageContent :: KeyValue a => ErrorMessage -> [a]
errorMessageContent = \case
    ErrAccessDenied -> ["class" .= String "Access denied"]
    ErrInternal -> ["class" .= String "Internal error"]
    ErrInvalidAccessKey -> ["class" .= String "Invalid access key"]
    ErrInvalidRequest -> ["class" .= String "Invalid request"]
    ErrInvalidRequestMsg msg -> ["class" .= String "Invalid request", "message" .= msg]
    ErrInvalidParameter key -> ["class" .= String "Invalid parameter", "parameterName" .= key]
    ErrMissingParameter key -> ["class" .= String "Missing parameter", "parameterName" .= key]
    ErrNotFound -> ["class" .= String "Not found"]
    ErrUnknownRequest -> ["class" .= String "Unknown request"]
    ErrVersionConflict -> ["class" .= String "Version conflict"]

instance ToJSON ErrorMessage where
    toJSON msg = object $ errorMessageContent msg
    toEncoding msg = pairs $ mconcat $ errorMessageContent msg

data ResponseBody
    = ResponseBodyError ErrorMessage
    | ResponseBodyConfirm (Reference ActionTicket)
    | ResponseBodyFollow Text.Text
    | ResponseBodyOk
    | ResponseBodyOkUser (Expanded User)
    | ResponseBodyOkUserList [Expanded User]
    | ResponseBodyUploadStatusList [Expanded UploadStatus]
    deriving (Show, Eq)

class OkResponseBody a where okResponseBody :: a -> ResponseBody
instance OkResponseBody () where okResponseBody = const ResponseBodyOk
instance OkResponseBody (Expanded User) where okResponseBody = ResponseBodyOkUser
instance OkResponseBody [Expanded User] where okResponseBody = ResponseBodyOkUserList
instance OkResponseBody [Expanded UploadStatus] where okResponseBody = ResponseBodyUploadStatusList

okResponse :: OkResponseBody a => a -> Response
okResponse a = Response StatusOk $ okResponseBody a

instance ToJSON ResponseBody where
    toJSON = \case
        ResponseBodyError msg -> object ["error" .= msg]
        ResponseBodyConfirm ref -> object ["confirm" .= ref]
        ResponseBodyFollow link -> object ["follow" .= link]
        ResponseBodyOk -> object ["ok" .= True]
        ResponseBodyOkUser user -> object ["ok" .= user]
        ResponseBodyOkUserList list -> object ["ok" .= list]
        ResponseBodyUploadStatusList list -> object ["uploads" .= list]
    toEncoding = \case
        ResponseBodyError msg -> pairs $ "error" .= msg
        ResponseBodyConfirm ref -> pairs $ "confirm" .= ref
        ResponseBodyFollow link -> pairs $ "follow" .= link
        ResponseBodyOk -> pairs $ "ok" .= True
        ResponseBodyOkUser user -> pairs $ "ok" .= user
        ResponseBodyOkUserList list -> pairs $ "ok" .= list
        ResponseBodyUploadStatusList list -> pairs $ "uploads" .= list

errorResponse :: ErrorMessage -> Response
errorResponse err = Response (errorStatus err) $ ResponseBodyError err

errorStatus :: ErrorMessage -> ResponseStatus
errorStatus = \case
    ErrAccessDenied -> StatusForbidden
    ErrInternal -> StatusInternalError
    ErrInvalidAccessKey -> StatusForbidden
    ErrInvalidRequest -> StatusBadRequest
    ErrInvalidRequestMsg _ -> StatusBadRequest
    ErrInvalidParameter _ -> StatusBadRequest
    ErrMissingParameter _ -> StatusBadRequest
    ErrNotFound -> StatusNotFound
    ErrUnknownRequest -> StatusNotFound
    ErrVersionConflict -> StatusConflict

data family Expanded a

class Expandable a where
    expand' :: Storage -> a -> IO (Expanded a)

class Retrievable a where
    retrieve :: Storage -> Reference a -> IO (Maybe (Expanded a))

data instance Expanded User = ExUser User
    deriving (Show, Eq)

instance Expandable User where
    expand' _ u = return $ ExUser u

instance Retrievable User where
    retrieve _ "" = return Nothing
    retrieve storage ref = do
        qret <- storagePerform storage $ UserList $
            ListView 0 1 [FilterUserId ref] []
        case qret of
            Right [user] -> return $ Just $ ExUser user
            _ -> return Nothing

instance ToJSON (Expanded User) where
    toJSON (ExUser user) = object
        [ "class" .= String "User"
        , "id" .= userId user
        , "name" .= userName user
        , "surname" .= userSurname user
        , "joinDate" .= userJoinDate user
        , "isAdmin" .= userIsAdmin user
        ]
    toEncoding (ExUser user) = pairs $ mconcat
        [ "class" .= String "User"
        , "id" .= userId user
        , "name" .= userName user
        , "surname" .= userSurname user
        , "joinDate" .= userJoinDate user
        , "isAdmin" .= userIsAdmin user
        ]

instance ToJSON AccessKey where
    toJSON (AccessKey (Reference ref) token) = String $ toHexText ref <> ":" <> toHexText token
    toEncoding (AccessKey (Reference ref) token) = Encoding.unsafeToEncoding $
        "\"" <> Builder.byteStringHex ref <> ":" <> Builder.byteStringHex token <> "\""

data instance Expanded Author = ExAuthor Author
    deriving (Show, Eq)

instance Expandable Author where
    expand' _ u = return $ ExAuthor u

instance Retrievable Author where
    retrieve _ "" = return Nothing
    retrieve storage ref = do
        qret <- storagePerform storage $ AuthorList $
            ListView 0 1 [FilterAuthorId ref] []
        case qret of
            Right [author] -> return $ Just $ ExAuthor author
            _ -> return Nothing

instance ToJSON (Expanded Author) where
    toJSON (ExAuthor author) = object
        [ "class" .= String "Author"
        , "id" .= authorId author
        , "name" .= authorName author
        , "description" .= authorDescription author
        ]
    toEncoding (ExAuthor author) = pairs $ mconcat
        [ "class" .= String "Author"
        , "id" .= authorId author
        , "name" .= authorName author
        , "description" .= authorDescription author
        ]

data instance Expanded Category = ExCategory
    (Reference Category) Text.Text (Maybe (Expanded Category))
    deriving (Show, Eq)

instance Expandable Category where
    expand' storage (Category ref name parentRef) =
        ExCategory ref name <$> retrieve storage parentRef

instance Retrievable Category where
    retrieve storage "" = return Nothing
    retrieve storage ref = do
        qret <- storagePerform storage $ CategoryList $ ListView 0 1 [FilterCategoryId ref] []
        case qret of
            Right [category] -> Just <$> expand' storage category
            _ -> return Nothing

instance ToJSON (Expanded Category) where
    toJSON (ExCategory ref name parent) = object
        [ "class" .= String "Category"
        , "id" .= ref
        , "name" .= name
        , "parent" .= parent
        ]
    toEncoding (ExCategory ref name parent) = pairs $ mconcat
        [ "class" .= String "Category"
        , "id" .= ref
        , "name" .= name
        , "parent" .= parent
        ]

instance ToJSON PublicationStatus where
    toJSON (PublishAt time) = toJSON time
    toJSON NonPublished = Null
    toEncoding (PublishAt time) = toEncoding time
    toEncoding NonPublished = Encoding.null_

data instance Expanded Article = ExArticle
    (Reference Article)
    (Version Article)
    (Maybe (Expanded Author))
    Text.Text
    PublicationStatus
    (Maybe (Expanded Category))
    [Expanded Tag]
    deriving (Show, Eq)

instance Expandable Article where
    expand' storage article = do
        mAuthorEx <- retrieve storage $ articleAuthor article
        mCategoryEx <- retrieve storage $ articleCategory article
        mTags <- storagePerform storage $ TagList $
            ListView 0 maxBound [FilterTagArticleId (articleId article)] [(OrderTagName, Ascending)]
        tagsEx <- case mTags of
            Left _ -> return []
            Right tags -> forM tags $ expand' storage
        return $ ExArticle
            (articleId article)
            (articleVersion article)
            mAuthorEx
            (articleName article)
            (articlePublicationStatus article)
            mCategoryEx
            tagsEx

instance Retrievable Article where
    retrieve storage "" = return Nothing
    retrieve storage ref = do
        qret <- storagePerform storage $ ArticleList True $ ListView 0 1 [FilterArticleId ref] []
        case qret of
            Right [article] -> Just <$> expand' storage article
            _ -> return Nothing

instance ToJSON (Expanded Article) where
    toJSON (ExArticle ref version mAuthor name pubStat mCategory tags) = object
        [ "class" .= String "Article"
        , "id" .= ref
        , "version" .= version
        , "author" .= mAuthor
        , "name" .= name
        , "publicationStatus" .= pubStat
        , "category" .= mCategory
        , "tags" .= tags
        ]
    toEncoding (ExArticle ref version mAuthor name pubStat mCategory tags) = pairs $ mconcat
        [ "class" .= String "Article"
        , "id" .= ref
        , "version" .= version
        , "author" .= mAuthor
        , "name" .= name
        , "publicationStatus" .= pubStat
        , "category" .= mCategory
        , "tags" .= tags
        ]

data instance Expanded Tag = ExTag Tag
    deriving (Show, Eq)

instance Expandable Tag where
    expand' _ u = return $ ExTag u

instance ToJSON (Expanded Tag) where
    toJSON (ExTag tag) = object
        [ "class" .= String "Tag"
        , "id" .= tagId tag
        , "name" .= tagName tag
        ]
    toEncoding (ExTag tag) = pairs $ mconcat
        [ "class" .= String "Tag"
        , "id" .= tagId tag
        , "name" .= tagName tag
        ]

data instance Expanded Comment = ExComment
    (Reference Comment)
    (Maybe (Expanded Article))
    (Maybe (Expanded User))
    Text.Text
    UTCTime
    (Maybe UTCTime)
    deriving (Show, Eq)

instance Expandable Comment where
    expand' storage comment = do
        mArticleEx <- retrieve storage $ commentArticle comment
        mUserEx <- retrieve storage $ commentUser comment
        return $ ExComment
            (commentId comment)
            mArticleEx
            mUserEx
            (commentText comment)
            (commentDate comment)
            (commentEditDate comment)

instance ToJSON (Expanded Comment) where
    toJSON (ExComment ref mArticle mUser text date editDate) = object
        [ "class" .= String "Comment"
        , "id" .= ref
        , "article" .= mArticle
        , "user" .= mUser
        , "text" .= text
        , "date" .= date
        , "editDate" .= editDate
        ]
    toEncoding (ExComment ref mArticle mUser text date editDate) = pairs $ mconcat
        [ "class" .= String "Comment"
        , "id" .= ref
        , "article" .= mArticle
        , "user" .= mUser
        , "text" .= text
        , "date" .= date
        , "editDate" .= editDate
        ]

data instance Expanded FileInfo = ExFileInfo
    (Reference FileInfo)
    Text.Text
    Text.Text
    UTCTime
    (Maybe (Expanded Article))
    Int64
    (Maybe (Expanded User))
    Text.Text
    deriving (Show, Eq)

expandFileInfo' :: Storage -> Text.Text -> FileInfo -> IO (Expanded FileInfo)
expandFileInfo' storage approot finfo = do
    mArticleEx <- retrieve storage $ fileArticle finfo
    mUserEx <- retrieve storage $ fileUser finfo
    return $ ExFileInfo
        (fileId finfo)
        (fileName finfo)
        (fileMimeType finfo)
        (fileUploadDate finfo)
        mArticleEx
        (fileIndex finfo)
        mUserEx
        (expandFileName approot finfo)

instance ToJSON (Expanded FileInfo) where
    toJSON (ExFileInfo ref@(Reference refBytes) name mimeType uploadDate mArticle index mUser link) = object
        [ "class" .= String "File"
        , "id" .= ref
        , "name" .= name
        , "mimeType" .= mimeType
        , "uploadDate" .= uploadDate
        , "article" .= mArticle
        , "index" .= index
        , "user" .= mUser
        , "link" .= link
        ]
    toEncoding (ExFileInfo ref@(Reference refBytes) name mimeType uploadDate mArticle index mUser link) = pairs $ mconcat
        [ "class" .= String "File"
        , "id" .= ref
        , "name" .= name
        , "mimeType" .= mimeType
        , "uploadDate" .= uploadDate
        , "article" .= mArticle
        , "index" .= index
        , "user" .= mUser
        , "link" .= link
        ]

data UploadStatus = UploadStatus Text.Text (Either ErrorMessage FileInfo)

data instance Expanded UploadStatus = ExUploadStatus
    Text.Text (Either ErrorMessage (Expanded FileInfo))
    deriving (Show, Eq)

expandUploadStatus' :: Storage -> Text.Text -> UploadStatus -> IO (Expanded UploadStatus)
expandUploadStatus' storage approot (UploadStatus pname status) = case status of
    Left errm -> return $ ExUploadStatus pname (Left errm)
    Right finfo -> ExUploadStatus pname . Right <$> expandFileInfo' storage approot finfo

instance ToJSON (Expanded UploadStatus) where
    toJSON (ExUploadStatus pname status) = case status of
        Left err -> object
            [ "param" .= pname
            , "error" .= err
            ]
        Right finfo -> object
            [ "param" .= pname
            , "ok" .= finfo
            ]
    toEncoding (ExUploadStatus pname status) = case status of
        Left err -> pairs $ mconcat
            [ "param" .= pname
            , "error" .= err
            ]
        Right finfo -> pairs $ mconcat
            [ "param" .= pname
            , "ok" .= finfo
            ]

expandFileName :: Text.Text -> FileInfo -> Text.Text
expandFileName approot finfo = approot <> "/get/" <> (toHexText $ getReference $ fileId finfo) <> "/" <> fileName finfo

instance ToJSON (Reference a) where
    toJSON (Reference "") = String "-"
    toJSON (Reference refBytes) = String $ toHexText refBytes
    toEncoding (Reference "") = Encoding.text "-"
    toEncoding (Reference refBytes) = Encoding.unsafeToEncoding $
        "\"" <> Builder.byteStringHex refBytes <> "\""

instance ToJSON (Version a) where
    toJSON (Version v) = String $ toHexText v
    toEncoding (Version v) = Encoding.unsafeToEncoding $
        "\"" <> Builder.byteStringHex v <> "\""

toHexText :: BS.ByteString -> Text.Text
toHexText = Text.pack . toHex . BS.unpack
