{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE TypeFamilies #-}

module SN.Ground.Types
    ( Reference(..)
    , fReference
    , Version(..)
    , fVersion
    , User(..)
    , Password(..)
    , hashPassword
    , AccessKey(..)
    , hashAccessKey
    , Author(..)
    , Category(..)
    , PublicationStatus(..)
    , fPublicationStatus
    , Article(..)
    , Tag(..)
    , Comment(..)
    , FileInfo(..)
    ) where

import Data.Hashable
import Data.Int
import Data.String
import Data.Time.Clock
import qualified Crypto.Hash as CHash
import qualified Data.ByteArray as BA
import qualified Data.ByteString as BS
import qualified Data.Text as Text
import SN.Data.Hex
import SN.Sql.Query

newtype Reference a = Reference
    { getReference :: BS.ByteString
    }
    deriving (Eq, Ord, IsString, Hashable)

instance Show (Reference a) where
    showsPrec d (Reference x) = showHex d $ BS.unpack x

instance IsValue (Reference a) where
    type PrimOf (Reference a) = BS.ByteString
    fromPrim (VBlob x) = Just $ Reference x
    fromPrim VNull = Just $ Reference ""
    toPrim (Reference "") = VNull
    toPrim (Reference bstr) = VBlob bstr

fReference :: FieldName -> Field (Reference a)
fReference = Field . FBlob

newtype Version a = Version
    { getVersion :: BS.ByteString
    }
    deriving (Eq, Ord, IsString, Hashable)

instance Show (Version a) where
    showsPrec d (Version x) = showHex d $ BS.unpack x

instance IsValue (Version a) where
    type PrimOf (Version a) = BS.ByteString
    fromPrim (VBlob version) = Just $ Version version
    fromPrim _ = Nothing
    toPrim (Version version) = VBlob version

fVersion :: FieldName -> Field (Version a)
fVersion = Field . FBlob

data User = User
    { userId :: !(Reference User)
    , userName :: !Text.Text
    , userSurname :: !Text.Text
    , userJoinDate :: !UTCTime
    , userIsAdmin :: !Bool
    }
    deriving (Show, Eq, Ord)

newtype Password = Password
    { getPassword :: BS.ByteString
    }
    deriving (Eq, Ord, IsString)

instance Show Password where
    showsPrec d _ = showParen (d > 10)
        $ showString "Password <<token>>"

hashPassword :: BS.ByteString -> Password -> BS.ByteString
hashPassword salt (Password back) = do
    let ctx0 = CHash.hashInitWith CHash.SHA3_256
    let ctx1 = CHash.hashUpdate ctx0 back
    let ctx2 = CHash.hashUpdate ctx1 salt
    BA.convert $ CHash.hashFinalize ctx2

data AccessKey = AccessKey
    { accessKeyId :: !(Reference AccessKey)
    , accessKeyToken :: !BS.ByteString
    }
    deriving (Eq, Ord)

instance Show AccessKey where
    showsPrec d (AccessKey ref _) = showParen (d > 10)
        $ showString "AccessKey "
        . showsPrec 11 ref
        . showString " <<token>>"

hashAccessKey :: AccessKey -> BS.ByteString
hashAccessKey (AccessKey (Reference front) back) = do
    let ctx0 = CHash.hashInitWith CHash.SHA3_256
    let ctx1 = CHash.hashUpdate ctx0 back
    let ctx2 = CHash.hashUpdate ctx1 front
    BA.convert $ CHash.hashFinalize ctx2

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

instance IsValue PublicationStatus where
    type PrimOf PublicationStatus = UTCTime
    fromPrim (VTime date) = Just $ PublishAt date
    fromPrim VTPosInf = Just NonPublished
    fromPrim _ = Nothing
    toPrim (PublishAt date) = VTime date
    toPrim NonPublished = VTPosInf

fPublicationStatus :: FieldName -> Field PublicationStatus
fPublicationStatus = Field . FTime

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
