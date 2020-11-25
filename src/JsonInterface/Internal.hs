{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE TupleSections #-}

module JsonInterface.Internal
    ( ResponseStatus(..)
    , ResponseContent(..)
    , Response(..)
    , Context(..)
    , RequestHandler
    , runRequestHandler
    , ErrorMessage(..)
    , requireUserAccess
    , requireAdminAccess
    , getParam
    , withDefault
    , intParser
    , textParser
    , referenceParser
    , sortOrderParser
    , perform
    , performRequire
    , getConfig
    , exitOk
    , exitError
    , errorResponse
    ) where

import Control.Monad.Cont
import Control.Monad.Reader
import Data.Aeson
import Data.Int
import Data.Void
import qualified Data.Aeson.Encoding as Encoding
import qualified Data.ByteString as BS
import qualified Data.ByteString.Char8 as BSChar
import qualified Data.HashMap.Strict as Map
import qualified Data.Text as Text
import qualified Data.Text.Encoding as Text
import qualified Data.Text.Encoding.Error as Text
import Hex
import Logger
import Storage
import qualified JsonInterface.Config as Config

data ResponseStatus
    = StatusOk
    | StatusBadRequest
    | StatusForbidden
    | StatusNotFound
    | StatusConflict
    | StatusInternalError
    deriving (Show, Eq)

data ResponseContent
    = forall j. ToJSON j => JsonResponse j

data Response = Response ResponseStatus ResponseContent

data Context = Context
    { contextConfig :: Config.Config
    , contextLogger :: Logger
    , contextStorage :: Storage
    }

type RequestHandler
    = ReaderT Context
        (ReaderT (Map.HashMap BS.ByteString BS.ByteString)
            (ContT Response IO))

runRequestHandler
    :: Map.HashMap BS.ByteString BS.ByteString
    -> Context
    -> RequestHandler Void
    -> IO Response
runRequestHandler params context handler = do
    handler
        `runReaderT` context
        `runReaderT` params
        `runContT` (return . absurd)

requireUserAccess :: RequestHandler User
requireUserAccess = do
    akey <- getParam "akey" accessKeyParser
    userLookup <- performRequire $ UserList $ ListView 0 1 [FilterUserAccessKey akey] []
    case userLookup of
        [user] -> return user
        [] -> exitError ErrInvalidAccessKey

requireAdminAccess :: RequestHandler ()
requireAdminAccess = do
    maybeAkey <- getParam "akey" $ Just . accessKeyParser
    case maybeAkey of
        Just akey -> do
            eUserLookup <- perform $ UserList $
                ListView 0 1 [FilterUserAccessKey akey, FilterUserIsAdmin True] []
            case eUserLookup of
                Right [_] -> return ()
                _ -> exitError ErrUnknownRequest
        _ -> exitError ErrUnknownRequest

getParam :: BS.ByteString -> (Maybe BS.ByteString -> Maybe r) -> RequestHandler r
getParam key parser = do
    params <- lift ask
    let value = Map.lookup key params
    case (value, parser value) of
        (_, Just result) -> return result
        (Nothing, Nothing) -> exitError $ ErrMissingParameter keyText
        (Just _, Nothing) -> exitError $ ErrInvalidParameter keyText
  where
    keyText = Text.decodeUtf8With Text.lenientDecode key

withDefault :: b -> (Maybe a -> Maybe b) -> Maybe a -> Maybe b
withDefault def parser mbs = case mbs of
    Nothing -> Just def
    _ -> parser mbs

intParser :: Int64 -> Int64 -> Maybe BS.ByteString -> Maybe Int64
intParser _ _ Nothing = Nothing
intParser minv maxv (Just bs) = case reads $ BSChar.unpack bs of
    (i, "") : _
        | i >= minv
        , i <= maxv
            -> Just i
    _ -> Nothing

textParser :: Maybe BS.ByteString -> Maybe Text.Text
textParser Nothing = Nothing
textParser (Just "") = Nothing
textParser (Just bs) = case Text.decodeUtf8' bs of
    Left _ -> Nothing
    Right text -> Just text

referenceParser :: Maybe BS.ByteString -> Maybe (Reference a)
referenceParser Nothing = Nothing
referenceParser (Just "-") = Just $ Reference ""
referenceParser (Just bs) = parseHex (BS.unpack bs) $ \idBytes rest -> do
    case rest of
        [] -> Just $ Reference $ BS.pack idBytes
        _ -> Nothing

accessKeyParser :: Maybe BS.ByteString -> Maybe AccessKey
accessKeyParser Nothing = Nothing
accessKeyParser (Just bs) = parseHex (BS.unpack bs) $ \idBytes rest1 -> do
    case rest1 of
        {- 58 == fromEnum ':' -}
        58 : tokenChars -> parseHex tokenChars $ \tokenBytes rest2 -> do
            case rest2 of
                [] -> Just $ AccessKey (Reference $ BS.pack idBytes) (BS.pack tokenBytes)
                _ -> Nothing
        _ -> Nothing

sortOrderParser
    :: [(BSChar.ByteString, ViewOrder a)]
    -> Maybe BSChar.ByteString
    -> Maybe [(ViewOrder a, OrderDirection)]
sortOrderParser _ Nothing = Just []
sortOrderParser _ (Just "") = Just []
sortOrderParser colNames (Just bs) = parse bs
  where
    parse buf0 = do
        (col, buf1) <- msum $ map
            (\(name, val) -> (val, ) <$> BS.stripPrefix name buf0)
            colNames
        (dir, buf2) <- msum
            [ (Ascending, ) <$> BS.stripPrefix "Asc" buf1
            , (Descending, ) <$> BS.stripPrefix "Desc" buf1
            , Just (Ascending, buf1)
            ]
        case BSChar.uncons buf2 of
            Nothing -> Just [(col, dir)]
            Just ('+', buf3) -> do
                after <- parse buf3
                Just $ (col, dir) : after
            _ -> Nothing

perform :: Action a -> RequestHandler (Either StorageError a)
perform action = do
    storage <- contextStorage <$> ask
    liftIO $ storagePerform storage action

performRequire :: Action a -> RequestHandler a
performRequire action = perform action >>= \case
    Left err -> exitStorageError err
    Right r -> return r

getConfig :: RequestHandler Config.Config
getConfig = contextConfig <$> ask

exitRespond :: Response -> RequestHandler a
exitRespond response = lift $ lift $ ContT $ \_ -> return response

exitOk :: ToJSON j => j -> RequestHandler a
exitOk value = exitRespond $ Response StatusOk $ JsonResponse $ OkWrapper value

exitStorageError :: StorageError -> RequestHandler a
exitStorageError NotFoundError = exitError ErrNotFound
exitStorageError VersionError = exitError ErrVersionConflict
exitStorageError InvalidRequestError = exitError ErrInvalidRequest
exitStorageError InternalError = exitError ErrInternal

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

exitError :: ErrorMessage -> RequestHandler a
exitError = exitRespond . errorResponse

errorResponse :: ErrorMessage -> Response
errorResponse err = Response (errorStatus err) $ JsonResponse err

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
    toJSON msg = object ["error" .= object (errorMessageContent msg)]
    toEncoding msg = pairs $ Encoding.pair "error" $ pairs $ mconcat $ errorMessageContent msg

data OkWrapper = forall j. ToJSON j => OkWrapper j

instance ToJSON OkWrapper where
    toJSON (OkWrapper value) = object ["ok" .= value]
    toEncoding (OkWrapper value) = pairs $ Encoding.pair "ok" $ toEncoding value

instance ToJSON User where
    toJSON user = object
        [ "class" .= String "User"
        , "id" .= userId user
        , "name" .= userName user
        , "surname" .= userSurname user
        , "joinDate" .= userJoinDate user
        , "isAdmin" .= userIsAdmin user
        ]
    toEncoding user = pairs $ mconcat
        [ "class" .= String "User"
        , "id" .= userId user
        , "name" .= userName user
        , "surname" .= userSurname user
        , "joinDate" .= userJoinDate user
        , "isAdmin" .= userIsAdmin user
        ]

instance ToJSON (Reference a) where
    toJSON (Reference "") = String "-"
    toJSON (Reference ref) = String $ Text.pack $ bstrToHex ref
    toEncoding (Reference "") = Encoding.text "-"
    toEncoding (Reference ref) = Encoding.string $ bstrToHex ref

bstrToHex :: BS.ByteString -> [Char]
bstrToHex = toHex . BS.unpack
