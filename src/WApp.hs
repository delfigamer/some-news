{-# LANGUAGE ScopedTypeVariables #-}

module WApp
    (
    ) where

import Control.Exception
import Data.Aeson
import Data.IORef
import Data.Int
import Data.Maybe
import qualified Data.ByteString as BS
import qualified Data.ByteString.Builder as Builder
import qualified Data.ByteString.Char8 as BSChar
import qualified Data.ByteString.Lazy as LBS
import qualified Data.HashMap.Strict as Map
import qualified Data.Text as Text
import qualified Data.Text.Encoding as Text
import qualified Data.Text.Encoding.Error as Text
import qualified Network.HTTP.Types as Http
import qualified Network.Wai as Wai
import qualified Network.Wai.Middleware.Approot as Approot
import qualified Network.Wai.Handler.Warp as Warp
import qualified Network.Wai.Parse as Parse
import Logger
import qualified JsonInterface as JI
import qualified JsonInterface.Response as JI

wApplication :: Logger -> JI.JsonInterface -> Wai.Application
wApplication logger jint = Approot.fromRequest $ \request respond -> do
    wApplicationInner logger jint request respond
        `catches`
        [ Handler $ \(ex :: SomeException) -> do
            logErr logger $ "WApp: exception in application: " <<|| displayException ex
            respond $ transformResponse $ JI.errorResponse $ JI.ErrInternal
        ]

wApplicationInner :: Logger -> JI.JsonInterface -> Wai.Application
wApplicationInner logger jint request respond = do
    case Parse.getRequestBodyType request of
        Just Parse.UrlEncoded -> do
            let parseOptions =
                    Parse.setMaxRequestParmsSize (64 * 1024) $
                    Parse.defaultParseRequestBodyOptions
            let fileBackend _ _ _ = error "Files are not allowed"
            eBodyParse <- try $ Parse.parseRequestBodyEx
                parseOptions fileBackend request
            case eBodyParse of
                Left (ErrorCall msg) -> respond $ transformResponse $
                    JI.errorResponse $ JI.ErrInvalidRequestMsg $ Text.pack msg
                Right (body, _) -> do
                    let params = foldr (\(k,v) -> Map.insert k v) uriParams body
                    JI.executeRequest jint rname (wAccept respond) $ JI.SimpleRequest $
                        JI.RequestData params approot
        Just (Parse.Multipart _) -> do
            JI.executeRequest jint rname (wAccept respond) $ JI.UploadRequest $ \handleUpload -> do
                let parseOptions =
                        Parse.setMaxRequestKeyLength 256 $
                        Parse.setMaxRequestNumFiles 16 $
                        Parse.setMaxRequestFilesSize (256 * 1024 * 1024) $
                        Parse.setMaxRequestParmsSize (64 * 1024) $
                        Parse.defaultParseRequestBodyOptions
                let fileBackend paramName (Parse.FileInfo fileName fileType _) getChunk =
                        handleUpload paramName fileName fileType getChunk
                (body, _) <- Parse.parseRequestBodyEx
                    parseOptions fileBackend request
                let params = foldr (\(k,v) -> Map.insert k v) uriParams body
                return $ JI.RequestData params approot
        Nothing -> JI.executeRequest jint rname (wAccept respond) $ JI.SimpleRequest $
            JI.RequestData uriParams approot
  where
    approot = case Approot.getApprootMay request of
        Nothing -> ""
        Just bs -> Text.decodeUtf8With Text.lenientDecode bs
    rname = Wai.pathInfo request
    uriParams = Map.fromList $
        map (\(k, mv) -> (k, fromMaybe "" mv)) $
            Wai.queryString request

wAccept :: (Wai.Response -> IO received) -> JI.Accept received
wAccept respond = JI.Accept (wAcceptResponse respond) (wAcceptStream respond)

wAcceptResponse :: (Wai.Response -> IO received) -> JI.Response -> IO received
wAcceptResponse respond jiResponse = respond $ transformResponse jiResponse

wAcceptStream :: (Wai.Response -> IO received) -> Int64 -> Text.Text -> ((BS.ByteString -> IO ()) -> IO ()) -> IO received
wAcceptStream respond fileSize fileType inner = do
    respond $ Wai.responseStream
        Http.ok200
        [ (Http.hContentLength, BSChar.pack $ show fileSize)
        , (Http.hContentType, Text.encodeUtf8 fileType)
        ]
        (\send _ -> inner (send . Builder.byteString))

transformResponse :: JI.Response -> Wai.Response
transformResponse (JI.Response jiStatus jiBody) = Wai.responseLBS
    waiStatus
    [ (Http.hContentLength, BSChar.pack $ show $ LBS.length content)
    , (Http.hContentType, "application/json")
    ]
    content
  where
    content = encode jiBody
    waiStatus = case jiStatus of
        JI.StatusOk -> Http.ok200
        JI.StatusBadRequest -> Http.badRequest400
        JI.StatusForbidden -> Http.forbidden403
        JI.StatusNotFound -> Http.notFound404
        JI.StatusConflict -> Http.conflict409
        JI.StatusPayloadTooLarge -> Http.requestEntityTooLarge413
        JI.StatusInternalError -> Http.internalServerError500
