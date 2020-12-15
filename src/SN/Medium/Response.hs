{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE TypeFamilies #-}

module SN.Medium.Response
    ( Accept(..)
    , Response(..)
    , ResponseStatus(..)
    , ErrorMessage(..)
    , ResponseBody(..)
    , IsResponseBody(..)
    , okResponse
    , errorResponse
    , ExpansionContext
    , newExpansionContext
    , Expanded(..)
    , Expandable(..)
    , Retrievable(..)
    , UploadStatus(..)
    ) where

import Control.Monad
import Control.Monad.Fix
import Data.Aeson
import Data.Coerce
import Data.Either
import Data.IORef
import Data.Int
import Data.Time.Clock
import Data.Typeable
import qualified Data.Aeson.Encoding as Encoding
import qualified Data.ByteString as BS
import qualified Data.ByteString.Builder as Builder
import qualified Data.Text as Text
import qualified Data.Text.Encoding as TextEncoding
import SN.Data.Base64
import SN.Ground.Interface
import SN.Medium.ActionTicket
import qualified SN.Data.PolyMap as PolyMap

data Accept r = Accept
    { acceptResponse :: Response -> IO r
    , acceptStream :: Int64 -> Text.Text -> ((BS.ByteString -> IO ()) -> IO ()) -> IO r
    }

data Response = Response ResponseStatus ResponseBody
    deriving (Show, Eq)

data ResponseStatus
    = StatusOk
    | StatusBadRequest
    | StatusForbidden
    | StatusNotFound
    | StatusConflict
    | StatusPayloadTooLarge
    | StatusInternalError
    deriving (Show, Eq)

data ErrorMessage
    = ErrAccessDenied
    | ErrArticleNotEditable
    | ErrAuthorNotOwned
    | ErrCyclicReference
    | ErrFileTooLarge
    | ErrInternal
    | ErrInvalidAccessKey
    | ErrInvalidRequest
    | ErrInvalidRequestMsg Text.Text
    | ErrInvalidParameter Text.Text
    | ErrLimitExceeded
    | ErrMissingParameter Text.Text
    | ErrNotFound
    | ErrUnknownRequest
    | ErrVersionConflict
    deriving (Show, Eq)

data ResponseBody
    = ResponseBodyError ErrorMessage
    | ResponseBodyConfirm (Reference ActionTicket)
    | ResponseBodyFollow Text.Text
    | ResponseBodyOk
    | ResponseBodyOkUser (Expanded User)
    | ResponseBodyOkUserList [Expanded User]
    | ResponseBodyOkAccessKey AccessKey
    | ResponseBodyOkAccessKeyList [Reference AccessKey]
    | ResponseBodyOkAuthor (Expanded Author)
    | ResponseBodyOkAuthorList [Expanded Author]
    | ResponseBodyOkCategoryList [Expanded Category]
    | ResponseBodyOkCategoryAncestryList [Expanded Category]
    | ResponseBodyOkTag (Expanded Tag)
    | ResponseBodyOkTagList [Expanded Tag]
    | ResponseBodyUploadStatusList [Expanded UploadStatus]
    deriving (Show, Eq)

class IsResponseBody a where toResponseBody :: a -> ResponseBody
instance IsResponseBody ResponseBody where toResponseBody = id
instance IsResponseBody () where toResponseBody = const ResponseBodyOk
instance IsResponseBody (Expanded User) where toResponseBody = ResponseBodyOkUser
instance IsResponseBody [Expanded User] where toResponseBody = ResponseBodyOkUserList
instance IsResponseBody AccessKey where toResponseBody = ResponseBodyOkAccessKey
instance IsResponseBody [Reference AccessKey] where toResponseBody = ResponseBodyOkAccessKeyList
instance IsResponseBody (Expanded Author) where toResponseBody = ResponseBodyOkAuthor
instance IsResponseBody [Expanded Author] where toResponseBody = ResponseBodyOkAuthorList
instance IsResponseBody [Expanded Category] where toResponseBody = ResponseBodyOkCategoryList
instance IsResponseBody (Expanded Tag) where toResponseBody = ResponseBodyOkTag
instance IsResponseBody [Expanded Tag] where toResponseBody = ResponseBodyOkTagList
instance IsResponseBody [Expanded UploadStatus] where toResponseBody = ResponseBodyUploadStatusList

okResponse :: IsResponseBody a => a -> Response
okResponse a = Response StatusOk $ toResponseBody a

errorResponse :: ErrorMessage -> Response
errorResponse err = Response (errorStatus err) $ ResponseBodyError err

errorStatus :: ErrorMessage -> ResponseStatus
errorStatus = \case
    ErrAccessDenied -> StatusForbidden
    ErrArticleNotEditable -> StatusForbidden
    ErrAuthorNotOwned -> StatusForbidden
    ErrCyclicReference -> StatusBadRequest
    ErrFileTooLarge -> StatusPayloadTooLarge
    ErrInternal -> StatusInternalError
    ErrInvalidAccessKey -> StatusForbidden
    ErrInvalidRequest -> StatusBadRequest
    ErrInvalidRequestMsg _ -> StatusBadRequest
    ErrInvalidParameter _ -> StatusBadRequest
    ErrLimitExceeded -> StatusForbidden
    ErrMissingParameter _ -> StatusBadRequest
    ErrNotFound -> StatusNotFound
    ErrUnknownRequest -> StatusNotFound
    ErrVersionConflict -> StatusConflict

data family Expanded a

data ExpansionContext = ExpansionContext
    Ground
    Text.Text
    (IORef (PolyMap.PolyMap Reference))

newExpansionContext :: Ground -> Text.Text -> IO ExpansionContext
newExpansionContext ground approot = do
    cache <- newIORef PolyMap.empty
    return $ ExpansionContext ground approot cache

class Expandable a where
    expand' :: ExpansionContext -> a -> IO (Expanded a)

class Retrievable a where
    doRetrieve :: ExpansionContext -> Reference a -> IO (Maybe (Expanded a))

retrieve :: (Typeable a, Retrievable a) => ExpansionContext -> Reference a -> IO (Maybe (Expanded a))
retrieve _ "" = return Nothing
retrieve ecx@(ExpansionContext _ _ cache) ref = do
    cmap <- readIORef cache
    case PolyMap.lookup (coerce ref) cmap of
        Just mx -> return mx
        Nothing -> mfix $ \mx -> do
            modifyIORef' cache $ PolyMap.insert (coerce ref) mx
            doRetrieve ecx ref

data instance Expanded User = ExUser User
    deriving (Show, Eq)

instance Expandable User where
    expand' _ u = return $ ExUser u

instance Retrievable User where
    doRetrieve (ExpansionContext ground _ _) ref = do
        qret <- groundPerform ground $ UserList $ ListView 0 1 [FilterUserId ref] []
        case qret of
            Right [user] -> return $ Just $ ExUser user
            _ -> return Nothing

data instance Expanded Author = ExAuthor Author
    deriving (Show, Eq)

instance Expandable Author where
    expand' _ u = return $ ExAuthor u

instance Retrievable Author where
    doRetrieve (ExpansionContext ground _ _) ref = do
        qret <- groundPerform ground $ AuthorList $ ListView 0 1 [FilterAuthorId ref] []
        case qret of
            Right [author] -> return $ Just $ ExAuthor author
            _ -> return Nothing

data instance Expanded Category = ExCategory Category
    deriving (Show, Eq)

instance Expandable Category where
    expand' _ u = return $ ExCategory u

instance Retrievable Category where
    doRetrieve (ExpansionContext ground _ _) ref = do
        qret <- groundPerform ground $ CategoryList $ ListView 0 1 [FilterCategoryId ref] []
        case qret of
            Right [category] -> return $ Just $ ExCategory category
            _ -> return Nothing

data instance Expanded Article = ExArticle
    (Reference Article)
    (Version Article)
    (Maybe (Expanded Author))
    Text.Text
    PublicationStatus
    [Expanded Category]
    [Expanded Tag]
    deriving (Show, Eq)

instance Expandable Article where
    expand' cex@(ExpansionContext ground _ _) article = do
        mAuthorEx <- retrieve cex $ articleAuthor article
        categoryList <- fmap (fromRight []) $ groundPerform ground $ CategoryAncestry $ articleCategory article
        mTags <- groundPerform ground $ TagList $
            ListView 0 maxBound [FilterTagArticleId (articleId article)] [(OrderTagName, Ascending)]
        tagsEx <- case mTags of
            Left _ -> return []
            Right tags -> forM tags $ expand' cex
        return $ ExArticle
            (articleId article)
            (articleVersion article)
            mAuthorEx
            (articleName article)
            (articlePublicationStatus article)
            (map ExCategory categoryList)
            tagsEx

instance Retrievable Article where
    doRetrieve cex@(ExpansionContext ground _ _) ref = do
        qret <- groundPerform ground $ ArticleList $ ListView 0 1 [FilterArticleId ref] []
        case qret of
            Right [article] -> Just <$> expand' cex article
            _ -> return Nothing

data instance Expanded Tag = ExTag Tag
    deriving (Show, Eq)

instance Expandable Tag where
    expand' _ u = return $ ExTag u

data instance Expanded Comment = ExComment
    (Reference Comment)
    (Maybe (Expanded Article))
    (Maybe (Expanded User))
    Text.Text
    UTCTime
    (Maybe UTCTime)
    deriving (Show, Eq)

instance Expandable Comment where
    expand' cex comment = do
        mArticleEx <- retrieve cex $ commentArticle comment
        mUserEx <- retrieve cex $ commentUser comment
        return $ ExComment
            (commentId comment)
            mArticleEx
            mUserEx
            (commentText comment)
            (commentDate comment)
            (commentEditDate comment)

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

instance Expandable FileInfo where
    expand' cex@(ExpansionContext _ approot _) finfo = do
        mArticleEx <- retrieve cex $ fileArticle finfo
        mUserEx <- retrieve cex $ fileUser finfo
        return $ ExFileInfo
            (fileId finfo)
            (fileName finfo)
            (fileMimeType finfo)
            (fileUploadDate finfo)
            mArticleEx
            (fileIndex finfo)
            mUserEx
            (expandFileName approot finfo)

data UploadStatus = UploadStatus Text.Text (Either ErrorMessage FileInfo)

data instance Expanded UploadStatus = ExUploadStatus
    Text.Text (Either ErrorMessage (Expanded FileInfo))
    deriving (Show, Eq)

instance Expandable UploadStatus where
    expand' cex@(ExpansionContext _ approot _) (UploadStatus pname status) = case status of
        Left errm -> return $ ExUploadStatus pname (Left errm)
        Right finfo -> ExUploadStatus pname . Right <$> expand' cex finfo

expandFileName :: Text.Text -> FileInfo -> Text.Text
expandFileName approot finfo = approot <> "/get/" <> (toBase64Text $ getReference $ fileId finfo) <> "/" <> fileName finfo
