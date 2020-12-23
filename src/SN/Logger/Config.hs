module SN.Logger.Config
    ( LoggerConfig(..)
    , withLogger
    ) where

import Control.Monad
import Data.Aeson
import Data.Foldable
import qualified Data.HashMap.Strict as Map
import qualified Data.Text as Text
import SN.Logger

data LoggerConfig
    = NullLoggerConfig
    | StdLoggerConfig
    | FileLoggerConfig FilePath Bool
    | HtmlLoggerConfig FilePath
    | JoinLoggerConfig [LoggerConfig]
    | LoggerFilterConfig LogLevel LoggerConfig
    deriving (Show)

withLogger :: LoggerConfig -> (Logger -> IO r) -> IO r
withLogger NullLoggerConfig body = body nullLogger
withLogger StdLoggerConfig body = body stdLogger
withLogger (FileLoggerConfig fpath clear) body = withFileLogger fpath clear body
withLogger (HtmlLoggerConfig fpath) body = withHtmlLogger fpath body
withLogger (JoinLoggerConfig confs) body = withList confs $ \logs -> body $ joinLoggers logs
  where
    withList [] inner = inner []
    withList (c : cs) inner = withLogger c $ \lc -> withList cs $ \lcs -> inner (lc : lcs)
withLogger (LoggerFilterConfig level conf) body = withLogger conf $ \log -> body $ loggerFilter level log

instance FromJSON LoggerConfig where
    parseJSON (String "null") = return NullLoggerConfig
    parseJSON (String "std") = return StdLoggerConfig
    parseJSON (Array xs) = JoinLoggerConfig . toList <$> mapM parseJSON xs
    parseJSON (Object u)
        | Just x <- Map.lookup "file" u = flip (withText "FileLogger") x $ \path -> do
            let fpath = Text.unpack path
            case fpath of
                '*' : rest -> return $ FileLoggerConfig rest True
                _ -> return $ FileLoggerConfig fpath False
        | Just x <- Map.lookup "html" u = flip (withText "HtmlLogger") x $ \path -> do
            return $ HtmlLoggerConfig $ Text.unpack path
        | Just x <- Map.lookup "filter" u = flip (withObject "LoggerFilter") x $ \v -> do
            level <- v .:? "level" .!= LevelDebug
            target <- v .: "of"
            return $ LoggerFilterConfig level target
    parseJSON _ = fail "invalid log config"

instance FromJSON LogLevel where
    parseJSON (String "debug") = return LevelDebug
    parseJSON (String "info") = return LevelInfo
    parseJSON (String "warn") = return LevelWarn
    parseJSON (String "err") = return LevelErr
    parseJSON (String "none") = return LevelTopmost
    parseJSON _ = fail $ "invalid log level"
