{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE LambdaCase #-}

module JsonInterface
    ( Accept(..)
    , Response(..)
    , ResponseStatus(..)
    , ErrorMessage(..)
    , ResponseBody(..)
    , Expanded(..)
    , UploadStatus
    , Request(..)
    , RequestData(..)
    , JsonInterface(..)
    , withJsonInterface
    ) where

import Control.Monad.Reader
import Data.IORef
import Data.Int
import qualified Data.ByteString as BS
import qualified Data.ByteString.Lazy as LBS
import qualified Data.HashMap.Strict as Map
import qualified Data.Text as Text
import qualified Data.Text.Encoding as Text
import qualified Data.Text.Encoding.Error as Text
import Cont
import Hex
import JsonInterface.Internal
import Logger
import Storage
import qualified JsonInterface.Config as Config

data Request
    = SimpleRequest RequestData
    | UploadRequest
        ((BS.ByteString -> BS.ByteString -> BS.ByteString -> IO BS.ByteString -> IO ()) -> IO RequestData)

newtype JsonInterface = JsonInterface
    { executeRequest :: forall r. [Text.Text] -> Accept r -> Request -> IO r
    }

type DList a = [a] -> [a]

withJsonInterface :: Config.Config -> Logger -> Storage -> (JsonInterface -> IO r) -> IO r
withJsonInterface config logger storage body = do
    context <- initializeContext config logger storage
    counter <- newIORef 0
    let newName = atomicModifyIORef' counter $ \n -> (n+1, n)
    body $ JsonInterface
        { executeRequest = jintExecuteRequest context newName
        }

jintExecuteRequest :: Context -> IO Int -> [Text.Text] -> Accept r -> Request -> IO r
jintExecuteRequest context newName rname (Accept onResponse onStream) request = do
    i <- newName
    logDebug (contextLogger context) $ "JsonInterface: " << rnameText rname << " (" <<| i << ")" << requestLog request
    execRequestHandler (requestDispatch rname request) context $ Accept
        (\response -> do
            logDebug (contextLogger context) $ "JsonInterface: " << rnameText rname << " (" <<| i << ") response:\n    " <<| response
            onResponse response)
        (\fsize ftype inner -> do
            logDebug (contextLogger context) $ "JsonInterface: " << rnameText rname << " (" <<| i << ") download: " <<|| ftype << ", " <<| fsize << " bytes"
            onStream fsize ftype inner)
  where
    rnameText (a : b : bs) = toLog a << "/" << rnameText (b : bs)
    rnameText [a] = toLog a
    rnameText [] = ""
    requestLog (SimpleRequest rqdata) = ":" << paramText (rqdataParams rqdata)
    requestLog (UploadRequest _) = " upload request"
    paramText = Map.foldrWithKey paramTextIter ""
    paramTextIter "akey" _ rest = "\n    \"akey\" = ..." << rest
    paramTextIter "password" _ rest = "\n    \"password\" = ..." << rest
    paramTextIter "newPassword" _ rest = "\n    \"newPassword\" = ..." << rest
    paramTextIter pname pvalue rest = "\n    " <<| pname << " = " <<| pvalue << rest

simpleRequest :: ReaderT RequestData (RequestHandler r) void -> Request -> RequestHandler r void
simpleRequest handler (SimpleRequest rqdata) = handler `runReaderT` rqdata
simpleRequest handler _ = exitError ErrInvalidRequest

requestDispatch :: [Text.Text] -> Request -> RequestHandler r void
requestDispatch ["users", "create"] = simpleRequest $ do
    name <- getParam "name" textParser
    surname <- getParam "surname" textParser
    newPassword <- getParam "newPassword" passwordParser
    minPasswordLength <- Config.minPasswordLength . contextConfig <$> askContext
    when (BS.length (getPassword newPassword) < minPasswordLength) $
        exitError $ ErrInvalidParameter "newPassword"
    user <- performRequire $ UserCreate name surname newPassword
    exitOk =<< expand user
requestDispatch ["users", "me"] = simpleRequest $ do
    myUser <- requireUserAccess
    exitOk =<< expand myUser
requestDispatch ["users", "setName"] = simpleRequest $ do
    newName <- getParam "name" textParser
    newSurname <- getParam "surname" textParser
    myUser <- requireUserAccess
    performRequire $ UserSetName (userId myUser) newName newSurname
    exitOk ()
requestDispatch ["users", "setPassword"] = simpleRequest $ do
    userRef <- getParam "user" referenceParser
    oldPassword <- getParam "password" passwordParser
    newPassword <- getParam "newPassword" passwordParser
    minPasswordLength <- Config.minPasswordLength . contextConfig <$> askContext
    when (BS.length (getPassword newPassword) < minPasswordLength) $
        exitError $ ErrInvalidParameter "newPassword"
    checkUserPassword userRef oldPassword
    performRequire $ UserSetPassword userRef newPassword
    exitOk ()
requestDispatch ["users", "delete"] = simpleRequest $ do
    myUser <- requireUserAccess
    performRequireConfirm $ UserDelete (userId myUser)
    exitOk ()

requestDispatch ["users", "setName_"] = simpleRequest $ do
    void requireAdminAccess
    ref <- getParam "user" referenceParser
    newName <- getParam "name" textParser
    newSurname <- getParam "surname" textParser
    performRequire $ UserSetName ref newName newSurname
    exitOk ()
requestDispatch ["users", "grantAdmin_"] = simpleRequest $ do
    void requireAdminAccess
    ref <- getParam "user" referenceParser
    performRequire $ UserSetIsAdmin ref True
    exitOk ()
requestDispatch ["users", "revokeAdmin_"] = simpleRequest $ do
    myUser <- requireAdminAccess
    ref <- getParam "user" referenceParser
    if ref == userId myUser
        then performRequireConfirm $ UserSetIsAdmin ref False
        else performRequire $ UserSetIsAdmin ref False
    exitOk ()
requestDispatch ["users", "delete_"] = simpleRequest $ do
    requireAdminAccess
    ref <- getParam "user" referenceParser
    performRequireConfirm $ UserDelete ref
    exitOk ()
requestDispatch ["users", "list_"] = simpleRequest $ do
    requireAdminAccess
    config <- contextConfig <$> askContext
    offset <- getParam "offset" $
        withDefault 0 $ intParser 0 maxBound
    limit <- getParam "limit" $
        withDefault (Config.defaultPageLimit config) $ intParser 1 (Config.maxPageLimit config)
    order <- getParam "orderBy" $
        sortOrderParser
            [ ("name", OrderUserName)
            , ("surname", OrderUserSurname)
            , ("joinDate", OrderUserJoinDate)
            , ("isAdmin", OrderUserIsAdmin)
            ]
    elems <- performRequire $ UserList $ ListView offset limit [] order
    exitOk =<< mapM expand elems

requestDispatch ["files", "upload"] = simpleRequest $ do
    articleRef <- getParam "article" referenceParser
    myUser <- requireUserAccess
    void $ assertArticleAccess (userId myUser) articleRef
    Reference ticketKey <- createActionTicket $ UploadTicket articleRef (userId myUser)
    approot <- rqdataApproot <$> ask
    let uri = approot <> "/upload/" <> toHexText ticketKey
    exitRespond $ Response StatusOk $ ResponseBodyFollow uri

requestDispatch ["upload", ticketParam] = \case
    SimpleRequest _ -> exitError ErrInvalidRequest
    UploadRequest body -> do
        ticketRef <- parseHex (Text.unpack ticketParam) $ \idBytes rest -> case rest of
            [] -> return $ Reference $ BS.pack idBytes
            _ -> exitInvalidTicket
        mTicket <- lookupActionTicket ticketRef
        case mTicket of
            Just (UploadTicket articleRef userRef) -> do
                Context
                    { contextConfig = Config.Config
                        { Config.fileChunkSize = fileChunkSize
                        , Config.maxFileSize = maxFileSize
                        }
                    } <- askContext
                pUploadStatusList <- liftIO $ newIORef []
                rqdata <- withStorage $ \storage -> body $
                    handleFileUpload
                        storage fileChunkSize maxFileSize
                        articleRef userRef pUploadStatusList
                uploadStatusList <- liftIO $ readIORef pUploadStatusList
                flip runReaderT rqdata $ do
                    statusList <- expandList $ reverse $ uploadStatusList
                    exitOk statusList
            _ -> exitInvalidTicket
  where
    exitInvalidTicket = exitError $ ErrInvalidRequestMsg "Upload ticket is invalid or has expired"

requestDispatch ["get", fileIdBytes, _] = simpleRequest $ do
    fileRef <- case referenceParser $ Just $ Text.encodeUtf8 fileIdBytes of
        Nothing -> exitError ErrNotFound
        Just fileRef -> return fileRef
    storage <- contextStorage <$> askContext
    mescape $ \accept -> do
        storageDownload storage fileRef
            (\err -> acceptResponse accept $ errorResponse $ storageError err)
            (acceptStream accept)

requestDispatch _ = do
    const $ exitError ErrUnknownRequest

handleFileUpload
    :: Storage
    -> Int64
    -> Int64
    -> Reference Article
    -> Reference User
    -> IORef [UploadStatus]
    -> BS.ByteString
    -> BS.ByteString
    -> BS.ByteString
    -> IO BS.ByteString
    -> IO ()
handleFileUpload
        storage fileChunkSize maxFileSize
        articleRef userRef pUploadStatusList
        paramNameBS fileNameBS mimeTypeBS getChunk = do
    uploadStatus <- execContT $ do
        let decodeText bs status = case Text.decodeUtf8' bs of
                Left _ -> mescape $ return status
                Right text -> return text
        let paramNameLenient = Text.decodeUtf8With Text.lenientDecode paramNameBS
        paramName <- decodeText paramNameBS $
            UploadStatus paramNameLenient $ Left ErrInvalidRequest
        fileName <- decodeText fileNameBS $
            UploadStatus paramName $ Left $ ErrInvalidParameter "fileName"
        mimeType <- decodeText mimeTypeBS $
            UploadStatus paramName $ Left $ ErrInvalidParameter "mimeType"
        saveResult <- liftIO $ storageUpload storage
            fileName mimeType articleRef userRef
            (uploader 0 "")
        case saveResult of
            Left serr -> return $ UploadStatus paramName $ Left $ storageError serr
            Right result -> return $ UploadStatus paramName result
    case uploadStatus of
        UploadStatus _ (Left _) -> drain
        _ -> return ()
    modifyIORef pUploadStatusList (uploadStatus :)
  where
    uploader totalSize pending finfo
        | LBS.length pending > fileChunkSize = do
            let (pl, pr) = LBS.splitAt fileChunkSize pending
            return $ UploadChunk (LBS.toStrict pl) $ uploader totalSize pr finfo
        | otherwise = do
            chunk <- getChunk
            if BS.null chunk
                then uploaderFinal pending finfo
                else do
                    let newTotalSize = totalSize + fromIntegral (BS.length chunk)
                    if newTotalSize > maxFileSize
                        then return $ UploadAbort $ Left ErrFileTooLarge
                        else uploader newTotalSize (pending <> LBS.fromStrict chunk) finfo
    uploaderFinal pending finfo
        | LBS.null pending = return $ UploadFinish $ Right finfo
        | otherwise = return $ UploadChunk (LBS.toStrict pending) $ do
            return $ UploadFinish $ Right finfo
    drain = do
        chunk <- getChunk
        unless (BS.null chunk) $ do
            drain
