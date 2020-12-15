{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE FunctionalDependencies #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE UndecidableInstances #-}

module SN.Medium.Internal
    ( Context(..)
    , initializeContext
    , RequestData(..)
    , RequestHandler(..)
    , MonadRequestHandler(..)
    , exitRespond
    , runRequestHandler
    , execRequestHandler
    , requireUserPassword
    , requireUserAccess
    , requireAdminAccess
    , assertAuthorAccess
    , assertArticleAccess
    , getParam
    , withDefault
    , listOptional
    , optionParser
    , intParser
    , textParser
    , passwordParser
    , referenceParser
    , accessKeyParser
    , sortOrderParser
    , getListView
    , getUserListView
    , getAuthorListView
    , getCategoryListView
    , getTagListView
    , createActionTicket
    , lookupActionTicket
    , withGround
    , perform
    , performRequire
    , performRequireConfirm
    , expand
    , expandList
    , groundError
    , exitOk
    , exitError
    , module SN.Control.Monad.Cont
    , module SN.Medium.ActionTicket
    , module SN.Medium.Config
    , module SN.Medium.Response
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
import SN.Control.Monad.Cont
import SN.Data.Base64
import SN.Ground.Interface
import SN.Logger
import SN.Medium.ActionTicket
import SN.Medium.Config
import SN.Medium.Response
import qualified SN.Data.TimedHashmap as TimedHashmap

data Context = Context
    { contextConfig :: MediumConfig
    , contextLogger :: Logger
    , contextGround :: Ground
    , contextTicketMap :: TimedHashmap.TimedHashmap (Reference ActionTicket) ActionTicket
    }

initializeContext :: MediumConfig -> Logger -> Ground -> IO Context
initializeContext config logger ground = do
    ticketMap <- TimedHashmap.new
    return $ Context config logger ground ticketMap

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

requireUserPassword :: (MonadRequestHandler f m, MonadReader RequestData m) => m (Reference User)
requireUserPassword = do
    userRef <- getParam "user" referenceParser
    password <- getParam "password" passwordParser
    check <- perform $ UserCheckPassword userRef password
    case check of
        Right () -> return userRef
        _ -> exitError ErrInvalidAccessKey

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

assertAuthorAccess :: MonadRequestHandler f m => Reference User -> Reference Author -> m Author
assertAuthorAccess userRef authorRef = do
    qret <- performRequire $ AuthorList $ ListView 0 1 [FilterAuthorId authorRef, FilterAuthorUserId userRef] []
    case qret of
        [author] -> return author
        _ -> exitError ErrAuthorNotOwned

assertArticleAccess :: MonadRequestHandler f m => Reference User -> Reference Article -> m Article
assertArticleAccess userRef articleRef = do
    qret <- performRequire $ ArticleList $ ListView 0 1 [FilterArticleId articleRef, FilterArticleUserId userRef] []
    case qret of
        [article] -> return article
        _ -> exitError ErrArticleNotEditable

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

listOptional :: (Maybe a -> Maybe b) -> Maybe a -> Maybe [b]
listOptional parser mbs = case mbs of
    Nothing -> Just []
    _ -> (\x -> [x]) <$> parser mbs

optionParser :: Maybe BS.ByteString -> Maybe Bool
optionParser Nothing = Just False
optionParser (Just _) = Just True

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

passwordParser :: Maybe BS.ByteString -> Maybe Password
passwordParser = fmap Password

referenceParser :: Maybe BS.ByteString -> Maybe (Reference a)
referenceParser Nothing = Nothing
referenceParser (Just ".") = Just $ Reference ""
referenceParser (Just bs) = Reference <$> fromBase64 bs

accessKeyParser :: Maybe BS.ByteString -> Maybe AccessKey
accessKeyParser Nothing = Nothing
accessKeyParser (Just bs) = do
    {- 58 == fromEnum ':' -}
    let (left, mid) = BS.break (== 58) bs
    (58, right) <- BS.uncons mid
    idBytes <- fromBase64 left
    tokenBytes <- fromBase64 right
    Just $ AccessKey (Reference idBytes) tokenBytes

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

getListView
    :: (MonadRequestHandler f m, MonadReader RequestData m)
    => [(BSChar.ByteString, ViewOrder a)]
    -> m ([ViewFilter a] -> ListView a)
getListView orderCols = do
    config <- contextConfig <$> askContext
    offset <- getParam "offset" $
        withDefault 0 $ intParser 0 maxBound
    limit <- getParam "limit" $
        withDefault (mediumConfigDefaultPageLimit config) $ intParser 1 (mediumConfigMaxPageLimit config)
    order <- getParam "order" $ sortOrderParser orderCols
    return $ \filters -> ListView offset limit filters order

getUserListView
    :: (MonadRequestHandler f m, MonadReader RequestData m)
    => m ([ViewFilter User] -> ListView User)
getUserListView = getListView
    [ ("name", OrderUserName)
    , ("surname", OrderUserSurname)
    , ("joinDate", OrderUserJoinDate)
    , ("isAdmin", OrderUserIsAdmin)
    ]

getAuthorListView
    :: (MonadRequestHandler f m, MonadReader RequestData m)
    => m ([ViewFilter Author] -> ListView Author)
getAuthorListView = getListView
    [ ("name", OrderAuthorName)
    ]

getCategoryListView
    :: (MonadRequestHandler f m, MonadReader RequestData m)
    => m ([ViewFilter Category] -> ListView Category)
getCategoryListView = getListView
    [ ("name", OrderCategoryName)
    ]

getTagListView
    :: (MonadRequestHandler f m, MonadReader RequestData m)
    => m ([ViewFilter Tag] -> ListView Tag)
getTagListView = getListView
    [ ("name", OrderTagName)
    ]

createActionTicket :: MonadRequestHandler f m => ActionTicket -> m (Reference ActionTicket)
createActionTicket ticket = do
    Context
        { contextConfig = MediumConfig
            { mediumConfigTicketLength = ticketLength
            , mediumConfigTicketLifetime = ticketLifetime
            }
        , contextGround = ground
        , contextTicketMap = ticketMap
        } <- askContext
    ref <- liftIO $ Reference <$> groundGenerateBytes ground ticketLength
    liftIO $ TimedHashmap.insert ticketMap ref ticket ticketLifetime
    return ref

lookupActionTicket :: MonadRequestHandler f m => Reference ActionTicket -> m (Maybe ActionTicket)
lookupActionTicket ref = do
    ticketMap <- contextTicketMap <$> askContext
    liftIO $ TimedHashmap.lookup ticketMap ref

withGround :: MonadRequestHandler f m => (Ground -> IO a) -> m a
withGround f = do
    ground <- contextGround <$> askContext
    liftIO $ f ground

perform :: MonadRequestHandler f m => Action a -> m (Either GroundError a)
perform action = withGround $ \ground -> groundPerform ground action

performRequire :: MonadRequestHandler f m => Action a -> m a
performRequire action = perform action >>= \case
    Left err -> exitRespond $ errorResponse $ groundError err
    Right r -> return r

performRequireConfirm :: (MonadRequestHandler f m, MonadReader RequestData m) => Action a -> m a
performRequireConfirm action = do
    mConfirmParam <- getParam "confirm" $ Just . referenceParser
    case mConfirmParam of
        Just confirmParam | confirmParam /= "" -> do
            mTicket2 <- lookupActionTicket confirmParam
            if mTicket2 == Just ticket
                then return ()
                else recreateTicket
        _ -> recreateTicket
    performRequire action
  where
    recreateTicket = do
        ref <- createActionTicket ticket
        exitRespond $ Response StatusOk $ ResponseBodyConfirm ref
    ticket = ActionTicket action

expand :: (MonadRequestHandler f m, MonadReader RequestData m, Expandable a) => a -> m (Expanded a)
expand x = do
    approot <- rqdataApproot <$> ask
    withGround $ \ground -> do
        cex <- newExpansionContext ground approot
        expand' cex x

expandList :: (MonadRequestHandler f m, MonadReader RequestData m, Expandable a) => [a] -> m [Expanded a]
expandList xs = do
    approot <- rqdataApproot <$> ask
    withGround $ \ground -> do
        cex <- newExpansionContext ground approot
        mapM (expand' cex) xs

groundError :: GroundError -> ErrorMessage
groundError = \case
    NotFoundError -> ErrNotFound
    VersionError -> ErrVersionConflict
    CyclicReferenceError -> ErrCyclicReference
    InvalidRequestError -> ErrInvalidRequest
    InternalError -> ErrInternal

exitOk :: (MonadRequestHandler f m, IsResponseBody a) => a -> m void
exitOk = exitRespond . okResponse

exitError :: MonadRequestHandler f m => ErrorMessage -> m void
exitError = exitRespond . errorResponse
