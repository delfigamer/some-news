{-# LANGUAGE ScopedTypeVariables #-}

module SN.Surface
    ( surfaceApplication
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
import SN.Logger
import SN.Surface.EncodeResponse
import qualified SN.Medium as Medium
import qualified SN.Medium.Response as Medium

surfaceApplication :: Logger -> Medium.Medium -> Wai.Application
surfaceApplication logger medium = Approot.fromRequest $ \request respond -> do
    wApplicationInner logger medium request respond
        `catches`
        [ Handler $ \(ex :: SomeException) -> do
            logErr logger $ "WApp: exception in application: " <<|| displayException ex
            respond $ transformResponse $ Medium.errorResponse $ Medium.ErrInternal
        ]

wApplicationInner :: Logger -> Medium.Medium -> Wai.Application
wApplicationInner logger medium request respond = do
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
                    Medium.errorResponse $ Medium.ErrInvalidRequestMsg $ Text.pack msg
                Right (body, _) -> do
                    let params = foldr (\(k,v) -> Map.insert k v) uriParams body
                    Medium.executeRequest medium rname (wAccept respond) $ Medium.SimpleRequest $
                        Medium.RequestData params approot
        Just (Parse.Multipart _) -> do
            Medium.executeRequest medium rname (wAccept respond) $ Medium.UploadRequest $ \handleUpload -> do
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
                return $ Medium.RequestData params approot
        Nothing -> Medium.executeRequest medium rname (wAccept respond) $ Medium.SimpleRequest $
            Medium.RequestData uriParams approot
  where
    approot = case Approot.getApprootMay request of
        Nothing -> ""
        Just bs -> Text.decodeUtf8With Text.lenientDecode bs
    rname = Wai.pathInfo request
    uriParams = Map.fromList $
        map (\(k, mv) -> (k, fromMaybe "" mv)) $
            Wai.queryString request

wAccept :: (Wai.Response -> IO received) -> Medium.Accept received
wAccept respond = Medium.Accept (wAcceptResponse respond) (wAcceptStream respond)

wAcceptResponse :: (Wai.Response -> IO received) -> Medium.Response -> IO received
wAcceptResponse respond medResponse = respond $ transformResponse medResponse

wAcceptStream :: (Wai.Response -> IO received) -> Int64 -> Text.Text -> ((BS.ByteString -> IO ()) -> IO ()) -> IO received
wAcceptStream respond fileSize fileType inner = do
    respond $ Wai.responseStream
        Http.ok200
        [ (Http.hContentLength, BSChar.pack $ show fileSize)
        , (Http.hContentType, Text.encodeUtf8 fileType)
        ]
        (\send _ -> inner (send . Builder.byteString))

transformResponse :: Medium.Response -> Wai.Response
transformResponse (Medium.Response medStatus medBody) = Wai.responseLBS
    waiStatus
    [ (Http.hContentLength, BSChar.pack $ show $ LBS.length content)
    , (Http.hContentType, "application/json")
    ]
    content
  where
    content = Builder.toLazyByteString $ encodeResponseBody medBody
    waiStatus = case medStatus of
        Medium.StatusOk -> Http.ok200
        Medium.StatusBadRequest -> Http.badRequest400
        Medium.StatusForbidden -> Http.forbidden403
        Medium.StatusNotFound -> Http.notFound404
        Medium.StatusConflict -> Http.conflict409
        Medium.StatusPayloadTooLarge -> Http.requestEntityTooLarge413
        Medium.StatusInternalError -> Http.internalServerError500
