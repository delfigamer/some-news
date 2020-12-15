{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE LambdaCase #-}

module SN.Medium
    ( Accept(..)
    , Response(..)
    , ResponseStatus(..)
    , ErrorMessage(..)
    , ResponseBody(..)
    , Expanded(..)
    , UploadStatus
    , Request(..)
    , RequestData(..)
    , MediumConfig(..)
    , defaultMediumConfig
    , Medium(..)
    , withMedium
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
import SN.Data.Base64
import SN.Ground.Interface
import SN.Logger
import SN.Medium.Internal

data Request
    = SimpleRequest RequestData
    | UploadRequest
        ((BS.ByteString -> BS.ByteString -> BS.ByteString -> IO BS.ByteString -> IO ()) -> IO RequestData)

newtype Medium = Medium
    { executeRequest :: forall r. [Text.Text] -> Accept r -> Request -> IO r
    }

type DList a = [a] -> [a]

withMedium :: MediumConfig -> Logger -> Ground -> (Medium -> IO r) -> IO r
withMedium config logger ground body = do
    context <- initializeContext config logger ground
    counter <- newIORef 0
    let newName = atomicModifyIORef' counter $ \n -> (n+1, n)
    body $ Medium
        { executeRequest = mediumExecuteRequest context newName
        }

mediumExecuteRequest :: Context -> IO Int -> [Text.Text] -> Accept r -> Request -> IO r
mediumExecuteRequest context newName rname (Accept onResponse onStream) request = do
    i <- newName
    logDebug (contextLogger context) $ "Medium: " << rnameText rname << " (" <<| i << ")" << requestLog request
    execRequestHandler (requestDispatch rname request) context $ Accept
        (\response -> do
            logDebug (contextLogger context) $ "Medium: " << rnameText rname << " (" <<| i << ") response:\n    " <<| response
            onResponse response)
        (\fsize ftype inner -> do
            logDebug (contextLogger context) $ "Medium: " << rnameText rname << " (" <<| i << ") download: " <<|| ftype << ", " <<| fsize << " bytes"
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
    minPasswordLength <- mediumConfigMinPasswordLength . contextConfig <$> askContext
    when (BS.length (getPassword newPassword) < minPasswordLength) $
        exitError $ ErrInvalidParameter "newPassword"
    user <- performRequire $ UserCreate name surname newPassword False
    exitOk =<< expand user
requestDispatch ["users", "info"] = simpleRequest $ do
    userRef <- getParam "user" referenceParser
    ret <- performRequire $ UserList $ ListView 0 1 [FilterUserId userRef] []
    case ret of
        [user] -> exitOk =<< expand user
        _ -> exitError ErrNotFound
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
    newPassword <- getParam "newPassword" passwordParser
    minPasswordLength <- mediumConfigMinPasswordLength . contextConfig <$> askContext
    when (BS.length (getPassword newPassword) < minPasswordLength) $
        exitError $ ErrInvalidParameter "newPassword"
    userRef <- requireUserPassword
    performRequire $ UserSetPassword userRef newPassword
    exitOk ()
requestDispatch ["users", "delete"] = simpleRequest $ do
    myUser <- requireUserAccess
    performRequireConfirm $ UserDelete (userId myUser)
    exitOk ()
requestDispatch ["users", "createAccessKey"] = simpleRequest $ do
    userRef <- requireUserPassword
    akeyList <- performRequire $ AccessKeyList userRef $ ListView 0 maxBound [] []
    maxAccessKeyCount <- mediumConfigMaxAccessKeyCount . contextConfig <$> askContext
    when (length akeyList >= maxAccessKeyCount) $
        exitError ErrLimitExceeded
    akey <- performRequire $ AccessKeyCreate userRef
    exitOk akey
requestDispatch ["users", "listAccessKeys"] = simpleRequest $ do
    userRef <- requireUserPassword
    akeyList <- performRequire $ AccessKeyList userRef $ ListView 0 maxBound [] []
    exitOk akeyList
requestDispatch ["users", "clearAccessKeys"] = simpleRequest $ do
    userRef <- requireUserPassword
    performRequire $ AccessKeyClear userRef
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
    void requireAdminAccess
    ref <- getParam "user" referenceParser
    performRequireConfirm $ UserDelete ref
    exitOk ()
requestDispatch ["users", "list_"] = simpleRequest $ do
    void requireAdminAccess
    mlAuthorRef <- getParam "author" $ listOptional referenceParser
    lview <- getUserListView
    elems <- performRequire $ UserList $ lview $ map FilterUserAuthorId mlAuthorRef
    exitOk =<< expandList elems

requestDispatch ["authors", "info"] = simpleRequest $ do
    authorRef <- getParam "author" referenceParser
    ret <- performRequire $ AuthorList $ ListView 0 1 [FilterAuthorId authorRef] []
    case ret of
        [author] -> exitOk =<< expand author
        _ -> exitError ErrNotFound
requestDispatch ["authors", "mine"] = simpleRequest $ do
    lview <- getAuthorListView
    myUser <- requireUserAccess
    elems <- performRequire $ AuthorList $ lview [FilterAuthorUserId $ userId myUser]
    exitOk =<< expandList elems
requestDispatch ["authors", "owners"] = simpleRequest $ do
    authorRef <- getParam "author" referenceParser
    lview <- getUserListView
    myUser <- requireUserAccess
    void $ assertAuthorAccess (userId myUser) authorRef
    elems <- performRequire $ UserList $ lview [FilterUserAuthorId authorRef]
    exitOk =<< expandList elems
requestDispatch ["authors", "create_"] = simpleRequest $ do
    void requireAdminAccess
    name <- getParam "name" textParser
    description <- getParam "description" textParser
    author <- performRequire $ AuthorCreate name description
    exitOk =<< expand author
requestDispatch ["authors", "setName_"] = simpleRequest $ do
    void requireAdminAccess
    authorRef <- getParam "author" referenceParser
    name <- getParam "name" textParser
    performRequire $ AuthorSetName authorRef name
    exitOk ()
requestDispatch ["authors", "setDescription_"] = simpleRequest $ do
    void requireAdminAccess
    authorRef <- getParam "author" referenceParser
    description <- getParam "description" textParser
    performRequire $ AuthorSetDescription authorRef description
    exitOk ()
requestDispatch ["authors", "delete_"] = simpleRequest $ do
    void requireAdminAccess
    authorRef <- getParam "author" referenceParser
    performRequireConfirm $ AuthorDelete authorRef
    exitOk ()
requestDispatch ["authors", "list_"] = simpleRequest $ do
    void requireAdminAccess
    mlUserRef <- getParam "user" $ listOptional referenceParser
    lview <- getAuthorListView
    elems <- performRequire $ AuthorList $ lview $ map FilterAuthorUserId mlUserRef
    exitOk =<< expandList elems
requestDispatch ["authors", "addOwner_"] = simpleRequest $ do
    void requireAdminAccess
    authorRef <- getParam "author" referenceParser
    userRef <- getParam "user" referenceParser
    performRequire $ AuthorSetOwnership authorRef userRef True
    exitOk ()
requestDispatch ["authors", "removeOwner_"] = simpleRequest $ do
    void requireAdminAccess
    authorRef <- getParam "author" referenceParser
    userRef <- getParam "user" referenceParser
    performRequire $ AuthorSetOwnership authorRef userRef False
    exitOk ()

requestDispatch ["categories", "info"] = simpleRequest $ do
    categoryRef <- getParam "category" referenceParser
    elems <- performRequire $ CategoryAncestry categoryRef
    case elems of
        [] -> exitError ErrNotFound
        _ -> exitOk . ResponseBodyOkCategoryAncestryList =<< expandList elems
requestDispatch ["categories", "list"] = simpleRequest $ do
    mlParentRef <- getParam "parent" $ listOptional referenceParser
    isStrict <- getParam "strict" optionParser
    lview <- getCategoryListView
    let filter = if isStrict
            then map FilterCategoryParentId mlParentRef
            else map FilterCategoryTransitiveParentId mlParentRef
    elems <- performRequire $ CategoryList $ lview filter
    exitOk =<< expandList elems
requestDispatch ["categories", "create_"] = simpleRequest $ do
    void requireAdminAccess
    name <- getParam "name" textParser
    parentRef <- getParam "parent" referenceParser
    parentAncestry <- case parentRef of
        "" -> return []
        _ -> do
            anc <- performRequire $ CategoryAncestry parentRef
            when (null anc) $
                exitError $ ErrInvalidParameter "parent"
            return anc
    category <- performRequire $ CategoryCreate name parentRef
    exitOk . ResponseBodyOkCategoryAncestryList =<< expandList (category : parentAncestry)
requestDispatch ["categories", "setName_"] = simpleRequest $ do
    void requireAdminAccess
    categoryRef <- getParam "category" referenceParser
    name <- getParam "name" textParser
    performRequire $ CategorySetName categoryRef name
    exitOk ()
requestDispatch ["categories", "setParent_"] = simpleRequest $ do
    void requireAdminAccess
    categoryRef <- getParam "category" referenceParser
    parentRef <- getParam "parent" referenceParser
    performRequire $ CategorySetParent categoryRef parentRef
    exitOk ()
requestDispatch ["categories", "delete_"] = simpleRequest $ do
    void requireAdminAccess
    categoryRef <- getParam "category" referenceParser
    performRequireConfirm $ CategoryDelete categoryRef
    exitOk ()

requestDispatch ["tags", "info"] = simpleRequest $ do
    tagRef <- getParam "tag" referenceParser
    elems <- performRequire $ TagList $ ListView 0 1 [FilterTagId tagRef] []
    case elems of
        [tag] -> exitOk . ResponseBodyOkTag =<< expand tag
        _ -> exitError ErrNotFound
requestDispatch ["tags", "list"] = simpleRequest $ do
    lview <- getTagListView
    elems <- performRequire $ TagList $ lview []
    exitOk =<< expandList elems
requestDispatch ["tags", "create_"] = simpleRequest $ do
    void requireAdminAccess
    name <- getParam "name" textParser
    tag <- performRequire $ TagCreate name
    exitOk =<< expand tag
requestDispatch ["tags", "setName_"] = simpleRequest $ do
    void requireAdminAccess
    tagRef <- getParam "tag" referenceParser
    name <- getParam "name" textParser
    performRequire $ TagSetName tagRef name
    exitOk ()
requestDispatch ["tags", "delete_"] = simpleRequest $ do
    void requireAdminAccess
    tagRef <- getParam "tag" referenceParser
    performRequireConfirm $ TagDelete tagRef
    exitOk ()

requestDispatch ["files", "upload"] = simpleRequest $ do
    articleRef <- getParam "article" referenceParser
    myUser <- requireUserAccess
    void $ assertArticleAccess (userId myUser) articleRef
    Reference ticketKey <- createActionTicket $ UploadTicket articleRef (userId myUser)
    approot <- rqdataApproot <$> ask
    let uri = approot <> "/upload/" <> toBase64Text ticketKey
    exitRespond $ Response StatusOk $ ResponseBodyFollow uri

requestDispatch ["upload", ticketParam] = \case
    SimpleRequest _ -> exitError ErrInvalidRequest
    UploadRequest body -> do
        ticketRef <- case fromBase64Text ticketParam of
            Just idBytes -> return $ Reference idBytes
            Nothing -> exitInvalidTicket
        mTicket <- lookupActionTicket ticketRef
        case mTicket of
            Just (UploadTicket articleRef userRef) -> do
                Context
                    { contextConfig = MediumConfig
                        { mediumConfigFileChunkSize = fileChunkSize
                        , mediumConfigMaxFileSize = maxFileSize
                        }
                    } <- askContext
                pUploadStatusList <- liftIO $ newIORef []
                rqdata <- withGround $ \ground -> body $
                    handleFileUpload
                        ground fileChunkSize maxFileSize
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
    ground <- contextGround <$> askContext
    mescape $ \accept -> do
        groundDownload ground fileRef
            (\err -> acceptResponse accept $ errorResponse $ groundError err)
            (acceptStream accept)

requestDispatch _ = do
    const $ exitError ErrUnknownRequest

handleFileUpload
    :: Ground
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
        ground fileChunkSize maxFileSize
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
        saveResult <- liftIO $ groundUpload ground
            fileName mimeType articleRef userRef
            (uploader 0 "")
        case saveResult of
            Left serr -> return $ UploadStatus paramName $ Left $ groundError serr
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
