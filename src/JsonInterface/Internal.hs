{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE FunctionalDependencies #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE UndecidableInstances #-}

module JsonInterface.Internal
    ( Context(..)
    , initializeContext
    , RequestData(..)
    , RequestHandler(..)
    , MonadRequestHandler(..)
    , exitRespond
    , runRequestHandler
    , execRequestHandler
    , requireUserAccess
    , requireAdminAccess
    , assertArticleAccess
    , getParam
    , withDefault
    , intParser
    , textParser
    , referenceParser
    , accessKeyParser
    , sortOrderParser
    , createActionTicket
    , lookupActionTicket
    , withStorage
    , perform
    , performRequire
    , performRequireConfirm
    , expand
    , expandFileInfo
    , expandUploadStatus
    , storageError
    , exitOk
    , exitError
    , module Cont
    , module JsonInterface.ActionTicket
    , module JsonInterface.Response
    ) where

import Control.Monad.Reader
import Data.Int
import Data.Void
import qualified Data.ByteString as BS
import qualified Data.ByteString.Char8 as BSChar
import qualified Data.HashMap.Strict as Map
import qualified Data.Text as Text
import qualified Data.Text.Encoding as Text
import qualified Data.Text.Encoding.Error as Text
import Cont
import Hex
import JsonInterface.ActionTicket
import JsonInterface.Response
import Logger
import Storage
import qualified JsonInterface.Config as Config
import qualified TData.TimedHashmap as TimedHashmap

data Context = Context
    { contextConfig :: Config.Config
    , contextLogger :: Logger
    , contextStorage :: Storage
    , contextTicketMap :: TimedHashmap.TimedHashmap (Reference ActionTicket) ActionTicket
    }

initializeContext :: Config.Config -> Logger -> Storage -> IO Context
initializeContext config logger storage = do
    ticketMap <- TimedHashmap.new
    return $ Context config logger storage ticketMap

data RequestData = RequestData
    { rqdataParams :: Map.HashMap BS.ByteString BS.ByteString
    , rqdataApproot :: Text.Text
    }

newtype RequestHandler f a = RequestHandler
    { unwrapRequestHandler :: ReaderT Context (ReaderT (Accept f) (ContT f IO)) a
    }
    deriving
        ( Functor
        , Applicative
        , Monad
        , MonadIO
        , MonadContPoly
        , MonadShift IO
        )

instance MonadEscape (Accept f -> IO f) (RequestHandler f) where
    mescape body = RequestHandler $
        ReaderT $ \_context ->
            ReaderT $ \accept ->
                mescape $ body accept

class
        ( MonadIO m
        , MonadContPoly m
        , MonadShift IO m
        , MonadEscape (Accept f -> IO f) m
        ) => MonadRequestHandler f m | m -> f where
    askContext :: m Context

instance MonadRequestHandler f (RequestHandler f) where
    askContext = RequestHandler $ ask

instance MonadRequestHandler f m => MonadRequestHandler f (ReaderT r m) where
    askContext = lift askContext

exitRespond :: MonadRequestHandler f m => Response -> m void
exitRespond resp = mescape $ \accept -> acceptResponse accept resp

runRequestHandler :: RequestHandler f a -> Context -> Accept f -> (a -> IO f) -> IO f
runRequestHandler handler context accept cont = do
    unwrapRequestHandler handler `runReaderT` context `runReaderT` accept `runContT` cont

execRequestHandler
    :: (forall void. RequestHandler f void)
    -> Context
    -> Accept f
    -> IO f
execRequestHandler handler context accept = do
    runRequestHandler handler context accept (return . absurd)

requireUserAccess :: (MonadRequestHandler f m, MonadReader RequestData m) => m User
requireUserAccess = do
    akey <- getParam "akey" accessKeyParser
    userLookup <- performRequire $ UserList $ ListView 0 1 [FilterUserAccessKey akey] []
    case userLookup of
        [user] -> return user
        [] -> exitError ErrInvalidAccessKey

requireAdminAccess :: (MonadRequestHandler f m, MonadReader RequestData m) => m User
requireAdminAccess = do
    maybeAkey <- getParam "akey" $ Just . accessKeyParser
    case maybeAkey of
        Just akey -> do
            eUserLookup <- perform $ UserList $
                ListView 0 1 [FilterUserAccessKey akey, FilterUserIsAdmin True] []
            case eUserLookup of
                Right [user] -> return user
                _ -> exitError ErrUnknownRequest
        _ -> exitError ErrUnknownRequest

assertArticleAccess :: MonadRequestHandler f m => Reference User -> Reference Article -> m Article
assertArticleAccess userRef articleRef = do
    qret <- performRequire $ ArticleList True $ ListView 0 1 [FilterArticleId articleRef, FilterArticleUserId userRef] []
    case qret of
        [article] -> return article
        _ -> exitError ErrAccessDenied

getParam :: (MonadRequestHandler f m, MonadReader RequestData m) => BS.ByteString -> (Maybe BS.ByteString -> Maybe r) -> m r
getParam key parser = do
    params <- rqdataParams <$> ask
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

createActionTicket :: MonadRequestHandler f m => ActionTicket -> m (Reference ActionTicket)
createActionTicket ticket = do
    Context
        { contextConfig = Config.Config
            { Config.ticketLength = ticketLength
            , Config.ticketLifetime = ticketLifetime
            }
        , contextStorage = storage
        , contextTicketMap = ticketMap
        } <- askContext
    ref <- liftIO $ Reference <$> storageGenerateBytes storage ticketLength
    liftIO $ TimedHashmap.insert ticketMap ref ticket ticketLifetime
    return ref

lookupActionTicket :: MonadRequestHandler f m => Reference ActionTicket -> m (Maybe ActionTicket)
lookupActionTicket ref = do
    ticketMap <- contextTicketMap <$> askContext
    liftIO $ TimedHashmap.lookup ticketMap ref

withStorage :: MonadRequestHandler f m => (Storage -> IO a) -> m a
withStorage f = do
    storage <- contextStorage <$> askContext
    liftIO $ f storage

perform :: MonadRequestHandler f m => Action a -> m (Either StorageError a)
perform action = withStorage $ \storage -> storagePerform storage action

performRequire :: MonadRequestHandler f m => Action a -> m a
performRequire action = perform action >>= \case
    Left err -> exitRespond $ errorResponse $ storageError err
    Right r -> return r

performRequireConfirm :: (MonadRequestHandler f m, MonadReader RequestData m) => Action a -> m a
performRequireConfirm action = do
    confirmParam <- getParam "confirm" $ withDefault "" referenceParser
    case confirmParam of
        "" -> recreateTicket
        _ -> do
            mTicket2 <- lookupActionTicket confirmParam
            if mTicket2 == Just ticket
                then return ()
                else recreateTicket
    performRequire action
  where
    recreateTicket = do
        ref <- createActionTicket ticket
        exitRespond $ Response StatusOk $ ResponseBodyConfirm ref
    ticket = ActionTicket action

expand :: (MonadRequestHandler f m, Expandable a) => a -> m (Expanded a)
expand x = withStorage $ \storage -> expand' storage x

expandFileInfo :: (MonadRequestHandler f m, MonadReader RequestData m) => FileInfo -> m (Expanded FileInfo)
expandFileInfo x = do
    approot <- rqdataApproot <$> ask
    withStorage $ \storage -> expandFileInfo' storage approot x

expandUploadStatus :: (MonadRequestHandler f m, MonadReader RequestData m) => UploadStatus -> m (Expanded UploadStatus)
expandUploadStatus x = do
    approot <- rqdataApproot <$> ask
    withStorage $ \storage -> expandUploadStatus' storage approot x

storageError :: StorageError -> ErrorMessage
storageError = \case
    NotFoundError -> ErrNotFound
    VersionError -> ErrVersionConflict
    InvalidRequestError -> ErrInvalidRequest
    InternalError -> ErrInternal

exitOk :: (MonadRequestHandler f m, OkResponseBody a) => a -> m void
exitOk = exitRespond . okResponse

exitError :: MonadRequestHandler f m => ErrorMessage -> m void
exitError = exitRespond . errorResponse
