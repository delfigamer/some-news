{-# LANGUAGE StandaloneDeriving #-}
{-# LANGUAGE TypeFamilies #-}

module Storage
    ( Reference(..)
    , User(..)
    , AccessKey(..)
    , Author(..)
    , PublicationStatus(..)
    , Article(..)
    , InitFailure(..)
    , Handle(..)
    , withSqlStorage
    , currentSchema
    , upgradeSchema
    ) where

import Control.Exception
import Control.Monad
import Data.Bits
import qualified Data.ByteString as BS
import Data.Int
import Data.Maybe
import Data.Proxy
import qualified Data.Text as Text
import Data.Time.Clock
import Data.Word
import qualified Logger
import Sql.Query
import qualified Sql.Database as Db
import qualified Sql.Database.Postgres as Db
import Storage.Schema
import System.Random
import Tuple

data Reference a
    = Reference !Word64 !Word64
    deriving (Show, Eq)

data User = User
    { userName :: !Text.Text
    , userSurname :: !Text.Text
    , userJoinDate :: !UTCTime
    , userIsAdmin :: !Bool
    }
    deriving (Show, Eq)

data AccessKey = AccessKey
    { accessKeyFront :: !BS.ByteString
    , accessKeyBackHash :: !BS.ByteString
    , accessKeyUser :: !(Reference User)
    }
    deriving (Show, Eq)

data Author = Author
    { authorName :: !Text.Text
    , authorDescription :: !Text.Text
    }
    deriving (Show, Eq)

data PublicationStatus
    = PublishAt !UTCTime
    | NonPublished
    deriving (Show, Eq)

data Article = Article
    { articleAuthor :: !(Reference Author)
    , articleName :: !Text.Text
    , articleText :: !Text.Text
    , articlePublicationStatus :: !PublicationStatus
    }
    deriving (Show, Eq)

data Handle = Handle
    { spawnUser :: User -> IO (Maybe (Reference User))
    , getUser :: Reference User -> IO (Maybe User)
    , setUser :: Reference User -> User -> IO (Maybe ())
    , deleteUser :: Reference User -> IO (Maybe ())
    , listUsers :: Int64 -> Int64 -> IO [(Reference User, User)]
    -- , spawnAccessKey :: Reference User
    }

withSqlStorage :: Logger.Handle -> Db.Handle -> (InitFailure -> IO r) -> (Handle -> IO r) -> IO r
withSqlStorage logger db onFail onSuccess = do
    matchCurrentSchema logger db onFail $ onSuccess $ Handle
        { spawnUser = \user -> do
            ref <- generateRef
            mret <- Db.queryMaybe db $
                Insert "sn_users" (fReference "user_id" :* fUser :* E) (Val ref :* Val user :* E) E
            case mret of
                Just E -> return $ Just $ ref
                _ -> return Nothing
        , getUser = \ref -> do
            mret <- Db.queryMaybe db $
                Select ["sn_users"] (fUser :* E)
                    [Where "user_id = ?" $ Val ref :* E]
                    []
                    (RowRange 0 1)
            case mret of
                Just [Val user :* E] -> return $ Just user
                _ -> return Nothing
        , setUser = \ref user -> do
            Db.queryMaybe db $
                Update "sn_users" (fUser :* E) (Val user :* E)
                    [Where "user_id = ?" $ Val ref :* E]
        , deleteUser = \ref -> do
            Db.queryMaybe db $
                Delete "sn_users"
                    [Where "user_id = ?" $ Val ref :* E]
        , listUsers = \offset limit -> do
            mret <- Db.queryMaybe db $
                Select ["sn_users"] (fReference "user_id" :* fUser :* E)
                    []
                    [Asc "user_id"]
                    (RowRange offset limit)
            case mret of
                Just rets -> return $ mapMaybe
                    (\row -> case row of
                        Val ref :* Val user :* E -> Just (ref, user)
                        _ -> Nothing)
                    rets
                Nothing -> return []
        }

-- referenceCondition :: Field (Reference a) -> Reference a -> Maybe Condition
-- referenceCondition (Field (FInt oidName :* FInt saltName :* E)) reference =
    -- case reference of
        -- ReferenceSalted oid salt ->
            -- Just $ Condition
                -- (oidName ++ " = ? AND " ++ saltName ++ " = ?")
                -- (Val oid :* Val salt :* E)
        -- ReferenceAny oid ->
            -- Just $ Condition
                -- (oidName ++ " = ?")
                -- (Val oid :* E)

instance IsValue (Reference a) where
    type Prims (Reference a) = '[BS.ByteString]
    primProxy _ = Proxy :* E
    primDecode (VBlob b :* E) = do
        let bs1 = BS.unpack b
        (x, bs2) <- decodeWord bs1
        (y, []) <- decodeWord bs2
        Just $ Reference x y
    primDecode _ = Nothing
    primEncode (Reference x y) = VBlob (BS.pack $ encodeWord x ++ encodeWord y) :* E

fReference :: FieldName -> Field (Reference a)
fReference fieldName = Field (FBlob fieldName :* E)

generateRef :: IO (Reference a)
generateRef = do
    x <- randomIO
    y <- randomIO
    return $ Reference x y

instance IsValue User where
    type Prims User = '[Text.Text, Text.Text, UTCTime, Int64]
    primProxy _ = Proxy :* Proxy :* Proxy :* Proxy :* E
    primDecode (VText name :* VText surname :* VDateTime joinDate :* vIsAdmin :* E) = do
        isAdmin <- primDecode $ vIsAdmin :* E
        Just $ User name surname joinDate isAdmin
    primDecode _ = Nothing
    primEncode (User name surname joinDate isAdmin) =
        VText name :* VText surname :* VDateTime joinDate :* primEncode isAdmin

fUser :: Field User
fUser = Field (FText "user_name" :* FText "user_surname" :* FDateTime "user_join_date" :* FInt "user_is_admin" :* E)

instance IsValue AccessKey where
    type Prims AccessKey = '[BS.ByteString, BS.ByteString, BS.ByteString]
    primProxy _ = Proxy :* Proxy :* Proxy :* E
    primDecode (VBlob keyFront :* VBlob keyBackHash :* vUserId :* E) = do
        userId <- primDecode $ vUserId :* E
        Just $ AccessKey keyFront keyBackHash userId
    primDecode _ = Nothing
    primEncode (AccessKey keyFront keyBackHash userId) =
        VBlob keyFront :* VBlob keyBackHash :* primEncode userId

fAccessKey :: Field AccessKey
fAccessKey = Field (FBlob "access_key_front" :* FBlob "access_key_back_hash" :* FBlob "access_key_user_id" :* E)

instance IsValue Author where
    type Prims Author = '[Text.Text, Text.Text]
    primProxy _ = Proxy :* Proxy :* E
    primDecode (VText name :* VText description :* E) = do
        Just $ Author name description
    primDecode _ = Nothing
    primEncode (Author name description) =
        VText name :* VText description :* E

fAuthor :: Field Author
fAuthor = Field (FText "author_name" :* FText "author_description" :* E)

instance IsValue PublicationStatus where
    type Prims PublicationStatus = '[UTCTime]
    primProxy _ = Proxy :* E
    primDecode (VDateTime date :* E) = Just $ PublishAt date
    primDecode (VNull :* E) = Just $ NonPublished
    primEncode (PublishAt date) = VDateTime date :* E
    primEncode NonPublished = VNull :* E

instance IsValue Article where
    type Prims Article = '[BS.ByteString, Text.Text, Text.Text, UTCTime]
    primProxy _ = Proxy :* Proxy :* Proxy :* Proxy :* E
    primDecode (vAuthorId :* VText name :* VText text :* vPubDate :* E) = do
        authorId <- primDecode $ vAuthorId :* E
        pubStatus <- primDecode $ vPubDate :* E
        Just $ Article authorId name text pubStatus
    primDecode _ = Nothing
    primEncode (Article authorId name text pubStatus) =
        primEncode authorId >* VText name :* VText text :* primEncode pubStatus

fArticle :: Field Article
fArticle = Field (FBlob "article_author_id" :* FText "article_name" :* FText "article_text" :* FDateTime "article_publication_date" :* E)

encodeWord :: Word64 -> [Word8]
encodeWord x =
    [ fromIntegral x
    , fromIntegral $ x `shiftR` 8
    , fromIntegral $ x `shiftR` 16
    , fromIntegral $ x `shiftR` 24
    , fromIntegral $ x `shiftR` 32
    , fromIntegral $ x `shiftR` 40
    , fromIntegral $ x `shiftR` 48
    , fromIntegral $ x `shiftR` 56
    ]

decodeWord :: [Word8] -> Maybe (Word64, [Word8])
decodeWord (a0:a1:a2:a3:a4:a5:a6:a7:rest) =
    let i = fromIntegral a0
            + fromIntegral a1 `shiftL` 8
            + fromIntegral a2 `shiftL` 16
            + fromIntegral a3 `shiftL` 24
            + fromIntegral a4 `shiftL` 32
            + fromIntegral a5 `shiftL` 40
            + fromIntegral a6 `shiftL` 48
            + fromIntegral a7 `shiftL` 56
    in Just (i, rest)
decodeWord _ = Nothing
