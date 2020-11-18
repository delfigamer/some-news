{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE TupleSections #-}

module JsonInterface
    ( SimpleRequest(..)
    , ResponseStatus(..)
    , ResponseContent(..)
    , Response(..)
    , JsonInterface(..)
    , withJsonInterface
    , maxPageLimit
    , defaultPageLimit
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
import Hex
import Logger
import Storage

data SimpleRequest = SimpleRequest Text.Text (Map.HashMap Text.Text BS.ByteString)

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

data JsonInterface = JsonInterface
    { simpleRequest :: SimpleRequest -> IO Response
    }

withJsonInterface :: Logger -> Storage -> (JsonInterface -> IO r) -> IO r
withJsonInterface logger storage body = do
    body $ JsonInterface
        { simpleRequest = \(SimpleRequest rname params) -> do
            logDebug logger $ "JsonInterface: " <<|| rname << paramText params
            let handlerCont =
                    flip runReaderT logger $
                        flip runReaderT params $
                            flip runReaderT storage $
                                handleSimpleRequest rname
            runContT handlerCont $ \_ -> do
                logErr logger $
                    "JsonInterface: request handler fell through"
                return $ Response StatusInternalError $ JsonResponse ErrInternal
        }
  where
    paramText = Map.foldrWithKey
        (\pname pvalue rest -> "\n    " <<|| pname << " = " <<| pvalue << rest)
        ""

maxPageLimit :: Int64
maxPageLimit = 100

defaultPageLimit :: Int64
defaultPageLimit = 20

type RequestHandler
    = ReaderT Storage
        (ReaderT (Map.HashMap Text.Text BS.ByteString)
            (ReaderT Logger
                (ContT Response IO)))

handleSimpleRequest :: Text.Text -> RequestHandler Void
handleSimpleRequest "users/create" = do
    name <- getParam textParser "name"
    surname <- getParam textParser "surname"
    user <- performRequire $ UserCreate name surname
    exitOk user
handleSimpleRequest "users/me" = do
    myUser <- requireUserAccess
    exitOk myUser
handleSimpleRequest "users/setName" = do
    newName <- getParam textParser "name"
    newSurname <- getParam textParser "surname"
    myUser <- requireUserAccess
    performRequire $ UserSetName (userId myUser) newName newSurname
    exitOk True
handleSimpleRequest "users/delete" = do
    ref <- getParam referenceParser "user"
    myUser <- requireUserAccess
    when (ref /= userId myUser) $ do
        exitError $ ErrInvalidParameter "user"
    performRequire $ UserDelete ref
    exitOk True

handleSimpleRequest "users/setName_" = do
    requireAdminAccess
    ref <- getParam referenceParser "user"
    newName <- getParam textParser "name"
    newSurname <- getParam textParser "surname"
    performRequire $ UserSetName ref newName newSurname
    exitOk True
handleSimpleRequest "users/grantAdmin_" = do
    requireAdminAccess
    ref <- getParam referenceParser "user"
    performRequire $ UserSetIsAdmin ref True
    exitOk True
handleSimpleRequest "users/dropAdmin_" = do
    requireAdminAccess
    ref <- getParam referenceParser "user"
    performRequire $ UserSetIsAdmin ref False
    exitOk True
handleSimpleRequest "users/delete_" = do
    requireAdminAccess
    ref <- getParam referenceParser "user"
    performRequire $ UserDelete ref
    exitOk True
handleSimpleRequest "users/list_" = do
    requireAdminAccess
    offset <- getParam (withDefault 0 $ intParser 0 maxBound) "offset"
    limit <- getParam (withDefault defaultPageLimit $ intParser 1 maxPageLimit) "limit"
    order <- getParam userOrderParser "orderBy"
    elems <- performRequire $ UserList $ ListView offset limit [] order
    exitOk elems
  where
    userOrderParser = sortOrderParser
        [ ("name", OrderUserName)
        , ("surname", OrderUserSurname)
        , ("joinDate", OrderUserJoinDate)
        , ("isAdmin", OrderUserIsAdmin)
        ]

handleSimpleRequest _ = do
    exitError $ ErrUnknownRequest

    -- UserCreate :: Text.Text -> Text.Text -> Action User
    -- UserSetName :: Reference User -> Text.Text -> Text.Text -> Action ()
    -- UserSetIsAdmin :: Reference User -> Bool -> Action ()
    -- UserDelete :: Reference User -> Action ()
    -- UserList :: ListView User -> Action [User]

    -- AccessKeyCreate :: Reference User -> Action AccessKey
    -- AccessKeyDelete :: Reference User -> Reference AccessKey -> Action ()
    -- AccessKeyList :: Reference User -> ListView AccessKey -> Action [Reference AccessKey]
    -- AccessKeyLookup :: AccessKey -> Action (Reference User)

    -- AuthorCreate :: Text.Text -> Text.Text -> Action Author
    -- AuthorSetName :: Reference Author -> Text.Text -> Action ()
    -- AuthorSetDescription :: Reference Author -> Text.Text -> Action ()
    -- AuthorDelete :: Reference Author -> Action ()
    -- AuthorList :: ListView Author -> Action [Author]
    -- AuthorSetOwnership :: Reference Author -> Reference User -> Bool -> Action ()

    -- CategoryCreate :: Text.Text -> Reference Category -> Action Category
    -- CategorySetName :: Reference Category -> Text.Text -> Action ()
    -- CategorySetParent :: Reference Category -> Reference Category -> Action ()
    -- CategoryDelete :: Reference Category -> Action ()
    -- CategoryList :: ListView Category -> Action [Category]

    -- ArticleCreate :: Reference Author -> Action Article
    -- ArticleSetAuthor :: Reference Article -> Reference Author -> Action ()
    -- ArticleSetName :: Reference Article -> Version Article -> Text.Text -> Action (Version Article)
    -- ArticleSetText :: Reference Article -> Version Article -> Text.Text -> Action (Version Article)
    -- ArticleSetCategory :: Reference Article -> Reference Category -> Action ()
    -- ArticleSetPublicationStatus :: Reference Article -> PublicationStatus -> Action ()
    -- ArticleDelete :: Reference Article -> Action ()
    -- ArticleList :: Bool -> ListView Article -> Action [Article]
    -- ArticleSetTag :: Reference Article -> Reference Tag -> Bool -> Action ()

    -- TagCreate :: Text.Text -> Action Tag
    -- TagSetName :: Reference Tag -> Text.Text -> Action ()
    -- TagDelete :: Reference Tag -> Action ()
    -- TagList :: ListView Tag -> Action [Tag]

    -- CommentCreate :: Reference Article -> Reference User -> Text.Text -> Action Comment
    -- CommentSetText :: Reference Comment -> Text.Text -> Action ()
    -- CommentDelete :: Reference Comment -> Action ()
    -- CommentList :: ListView Comment -> Action [Comment]

requireUserAccess :: RequestHandler User
requireUserAccess = do
    akey <- getParam accessKeyParser "akey"
    userLookup <- performRequire $ UserList $ ListView 0 1 [FilterUserAccessKey akey] []
    case userLookup of
        [user] -> return user
        [] -> exitError ErrInvalidAccessKey

requireAdminAccess :: RequestHandler ()
requireAdminAccess = do
    maybeAkey <- getParam (Just . accessKeyParser) "akey"
    case maybeAkey of
        Just akey -> do
            eUserLookup <- perform $ UserList $
                ListView 0 1 [FilterUserAccessKey akey, FilterUserIsAdmin True] []
            case eUserLookup of
                Right [_] -> return ()
                _ -> exitError ErrUnknownRequest
        _ -> exitError ErrUnknownRequest

getParam :: (Maybe BS.ByteString -> Maybe r) -> Text.Text -> RequestHandler r
getParam parser key = do
    params <- lift ask
    let value = Map.lookup key params
    case (value, parser value) of
        (_, Just result) -> return result
        (Nothing, Nothing) -> exitError $ ErrMissingParameter key
        (Just _, Nothing) -> exitError $ ErrInvalidParameter key

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
    storage <- ask
    liftIO $ doStorage storage action

performRequire :: Action a -> RequestHandler a
performRequire action = perform action >>= \case
    Left err -> exitStorageError err
    Right r -> return r

exitRespond :: ToJSON j => ResponseStatus -> j -> RequestHandler a
exitRespond status response = lift $ lift $ lift $ ContT $ \_ -> return $ Response status $ JsonResponse response

exitOk :: ToJSON j => j -> RequestHandler a
exitOk value = exitRespond StatusOk (OkWrapper value)

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
    | ErrInvalidParameter Text.Text
    | ErrMissingParameter Text.Text
    | ErrNotFound
    | ErrUnknownRequest
    | ErrVersionConflict

exitError :: ErrorMessage -> RequestHandler a
exitError err = exitRespond (errorStatus err) err

errorStatus :: ErrorMessage -> ResponseStatus
errorStatus = \case
    ErrAccessDenied -> StatusForbidden
    ErrInternal -> StatusInternalError
    ErrInvalidAccessKey -> StatusForbidden
    ErrInvalidRequest -> StatusBadRequest
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
