{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE TypeFamilies #-}

module SN.Surface.EncodeResponse
    ( encodeResponseBody
    ) where

import Data.Aeson
import Data.Aeson.Encoding
import qualified Data.ByteString as BS
import qualified Data.ByteString.Builder as Builder
import SN.Data.Base64
import SN.Ground.Types
import SN.Medium.Response

encodeResponseBody :: ResponseBody -> Builder.Builder
encodeResponseBody = fromEncoding . responseBody

responseBody :: ResponseBody -> Encoding
responseBody (ResponseBodyError errm) = pairs $ pair "error" $ errorMessage errm
responseBody (ResponseBodyConfirm ticketRef) = pairs $ pair "confirm" $ reference ticketRef
responseBody (ResponseBodyFollow uri) = pairs $ pair "follow" $ text uri
responseBody ResponseBodyOk = pairs $ pair "ok" $ bool True
responseBody (ResponseBodyOkUser user) = pairs $ pair "ok" $ exUser user
responseBody (ResponseBodyOkUserList userList) = pairs $ pair "ok" $ list exUser userList
responseBody (ResponseBodyOkAccessKey akey) = pairs $ pair "ok" $ accessKey akey
responseBody (ResponseBodyOkAccessKeyList refList) = pairs $ pair "ok" $ list reference refList
responseBody (ResponseBodyOkAuthor author) = pairs $ pair "ok" $ exAuthor author
responseBody (ResponseBodyOkAuthorList authorList) = pairs $ pair "ok" $ list exAuthor authorList
responseBody (ResponseBodyUploadStatusList uploadStatusList) = pairs $ pair "ok" $ list exUploadStatus uploadStatusList

errorMessage :: ErrorMessage -> Encoding
errorMessage ErrAccessDenied = pairs $ pair "class" $ text "Access denied"
errorMessage ErrArticleNotEditable = pairs $ pair "class" $ text "Article not editable"
errorMessage ErrFileTooLarge = pairs $ pair "class" $ text "File too large"
errorMessage ErrInternal = pairs $ pair "class" $ text "Internal error"
errorMessage ErrInvalidAccessKey = pairs $ pair "class" $ text "Invalid access key"
errorMessage ErrInvalidRequest = pairs $ pair "class" $ text "Invalid request"
errorMessage (ErrInvalidRequestMsg msg) = pairs $ (pair "class" $ text "Invalid request") <> (pair "message" $ text msg)
errorMessage (ErrInvalidParameter paramName) = pairs $ (pair "class" $ text "Invalid parameter") <> (pair "paramName" $ text paramName)
errorMessage ErrLimitExceeded = pairs $ pair "class" $ text "Limit exceeded"
errorMessage (ErrMissingParameter paramName) = pairs $ (pair "class" $ text "Missing parameter") <> (pair "paramName" $ text paramName)
errorMessage ErrNotFound = pairs $ pair "class" $ text "Not found"
errorMessage ErrUnknownRequest = pairs $ pair "class" $ text "Unknown request"
errorMessage ErrVersionConflict = pairs $ pair "class" $ text "Version conflict"

exUser :: Expanded User -> Encoding
exUser (ExUser (User ref name surname joinDate isAdmin)) = pairs $ mconcat
    [ pair "class" $ text "User"
    , pair "id" $ reference ref
    , pair "name" $ text name
    , pair "surname" $ text surname
    , pair "joinDate" $ utcTime joinDate
    , pair "isAdmin" $ bool isAdmin
    ]

accessKey :: AccessKey -> Encoding
accessKey (AccessKey (Reference ref) token) = unsafeToEncoding $
    "\"" <> byteStringBase64 ref <> ":" <> byteStringBase64 token <> "\""

exAuthor :: Expanded Author -> Encoding
exAuthor (ExAuthor (Author ref name description)) = pairs $ mconcat
    [ pair "class" $ text "Author"
    , pair "id" $ reference ref
    , pair "name" $ text name
    , pair "description" $ text description
    ]

exCategory :: Expanded Category -> Encoding
exCategory (ExCategory ref name mParent) = pairs $ mconcat
    [ pair "class" $ text "Category"
    , pair "id" $ reference ref
    , pair "name" $ text name
    , pair "parent" $ maybe null_ exCategory mParent
    ]

publicationStatus :: PublicationStatus -> Encoding
publicationStatus (PublishAt time) = utcTime time
publicationStatus NonPublished = null_

exArticle :: Expanded Article -> Encoding
exArticle (ExArticle ref ver mAuthor name pubStat mCategory tags) = pairs $ mconcat
    [ pair "class" $ text "Article"
    , pair "id" $ reference ref
    , pair "version" $ version ver
    , pair "author" $ maybe null_ exAuthor mAuthor
    , pair "name" $ text name
    , pair "publicationStatus" $ publicationStatus pubStat
    , pair "category" $ maybe null_ exCategory mCategory
    , pair "tags" $ list exTag tags
    ]

exTag :: Expanded Tag -> Encoding
exTag (ExTag (Tag ref name)) = pairs $ mconcat
    [ pair "class" $ text "Tag"
    , pair "id" $ reference ref
    , pair "name" $ text name
    ]

exComment :: Expanded Comment -> Encoding
exComment (ExComment ref mArticle mUser content date mEditDate) = pairs $ mconcat
    [ pair "class" $ text "Comment"
    , pair "id" $ reference ref
    , pair "article" $ maybe null_ exArticle mArticle
    , pair "user" $ maybe null_ exUser mUser
    , pair "text" $ text content
    , pair "date" $ utcTime date
    , pair "editDate" $ maybe null_ utcTime mEditDate
    ]

exFileInfo :: Expanded FileInfo -> Encoding
exFileInfo (ExFileInfo ref name mimeType uploadDate mArticle index mUser uri) = pairs $ mconcat
    [ pair "class" $ text "File"
    , pair "id" $ reference ref
    , pair "name" $ text name
    , pair "mimeType" $ text mimeType
    , pair "uploadDate" $ utcTime uploadDate
    , pair "article" $ maybe null_ exArticle mArticle
    , pair "index" $ int64 index
    , pair "user" $ maybe null_ exUser mUser
    , pair "link" $ text uri
    ]

exUploadStatus :: Expanded UploadStatus -> Encoding
exUploadStatus (ExUploadStatus pname status) = pairs $ mconcat
    [ pair "param" $ text pname
    , case status of
        Left err -> pair "error" $ errorMessage err
        Right finfo -> pair "ok" $ exFileInfo finfo
    ]

reference :: Reference a -> Encoding
reference (Reference "") = unsafeToEncoding "\".\""
reference (Reference refBytes) = unsafeToEncoding $ "\"" <> byteStringBase64 refBytes <> "\""

version :: Version a -> Encoding
version (Version v) = unsafeToEncoding $ "\"" <> byteStringBase64 v <> "\""

byteStringBase64 :: BS.ByteString -> Builder.Builder
byteStringBase64 = Builder.byteString . toBase64
