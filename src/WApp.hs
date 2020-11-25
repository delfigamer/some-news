{-# LANGUAGE ScopedTypeVariables #-}

module WApp
    (
    ) where

import Control.Exception
import Data.Aeson
import Data.Maybe
import qualified Data.ByteString.Char8 as BSChar
import qualified Data.ByteString.Lazy as LBS
import qualified Data.HashMap.Strict as Map
import qualified Data.Text as Text
import qualified Network.HTTP.Types as Http
import qualified Network.Wai as Wai
import qualified Network.Wai.Parse as Parse
import qualified Network.Wai.Handler.Warp as Warp
import Logger
import qualified JsonInterface as JI
import qualified JsonInterface.Internal as JI

wApplication :: Logger -> JI.JsonInterface -> Wai.Application
wApplication logger jint request respond = do
    wApplicationInner logger jint request respond
        `catches`
        [ Handler $ \(ex :: SomeException) -> do
            logErr logger $ "WApp: exception in application: " <<|| displayException ex
            respond $ transformResponse $
                JI.errorResponse $ JI.ErrInternal
        ]

wApplicationInner :: Logger -> JI.JsonInterface -> Wai.Application
wApplicationInner logger jint request respond = do
    case Parse.getRequestBodyType request of
        Just Parse.UrlEncoded -> do
            let parseOptions =
                    Parse.setMaxRequestParmsSize 0x10000 $ Parse.noLimitParseRequestBodyOptions
            let fileBackend _ _ _ = error "Files are not allowed"
            eBodyParse <- try $ Parse.parseRequestBodyEx
                parseOptions
                fileBackend
                request
            case eBodyParse of
                Left (ErrorCall msg) -> respond $ transformResponse $
                    JI.errorResponse $ JI.ErrInvalidRequestMsg $ Text.pack msg
                Right (body, _) -> do
                    let params = foldr (\(k,v) -> Map.insert k v) uriParams body
                    jiResponse <- JI.simpleRequest jint rname params
                    respond $ transformResponse $ jiResponse
        Just (Parse.Multipart _) -> do
            undefined
        Nothing -> do
            jiResponse <- JI.simpleRequest jint rname uriParams
            respond $ transformResponse $ jiResponse
  where
    rname = Wai.pathInfo request
    uriParams = Map.fromList $
        map (\(k, mv) -> (k, fromMaybe "" mv)) $
            Wai.queryString request

transformResponse :: JI.Response -> Wai.Response
transformResponse (JI.Response jiStatus (JI.JsonResponse value)) = Wai.responseLBS
    waiStatus
    [ (Http.hContentLength, BSChar.pack $ show $ LBS.length content)
    , (Http.hContentType, "application/json")
    ]
    content
  where
    content = encode value
    waiStatus = case jiStatus of
        JI.StatusOk -> Http.ok200
        JI.StatusBadRequest -> Http.badRequest400
        JI.StatusForbidden -> Http.forbidden403
        JI.StatusNotFound -> Http.notFound404
        JI.StatusConflict -> Http.conflict409
        JI.StatusInternalError -> Http.internalServerError500
