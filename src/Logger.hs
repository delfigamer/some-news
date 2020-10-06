module Logger
    ( LogLevel(..)
    , Handle(..)
    , loggerFilter
    , withNullLogger
    , withFileLogger
    , withTestLogger
    , withStdLogger
    , withMultiLogger
    , debug
    , info
    , warn
    , err
    ) where

import Control.Exception
import Control.Monad
import Data.IORef
import Data.Text (Text, pack)
import qualified Data.Text.IO as TextIO
import Data.Time.Clock (getCurrentTime)
import Data.Time.Format (defaultTimeLocale, formatTime)
import System.Console.ANSI
import qualified System.IO as IO

data Handle =
    Handle
        { send :: LogLevel -> Text -> IO ()
        }

data LogLevel
    = Debug
    | Info
    | Warn
    | Err
    | Topmost
    deriving (Ord, Eq)

instance Show LogLevel where
    show Debug = "DEBUG"
    show Info = "INFO "
    show Warn = "WARN "
    show Err = "ERROR"
    show Topmost = "TOP  "

debug :: Handle -> Text -> IO ()
debug h = send h Debug

info :: Handle -> Text -> IO ()
info h = send h Info

warn :: Handle -> Text -> IO ()
warn h = send h Warn

err :: Handle -> Text -> IO ()
err h = send h Err

loggerFilter :: LogLevel -> Handle -> Handle
loggerFilter minlevel inner = do
    Handle
        { send =
              \level text -> do when (level >= minlevel) $ send inner level text
        }

withNullLogger :: (Handle -> IO r) -> IO r
withNullLogger body = do
    body $ Handle {send = \_ _ -> return ()}

withFileLogger :: FilePath -> (Handle -> IO r) -> IO r
withFileLogger path body = do
    bracket (newFileLogger path) fst (body . snd)

newFileLogger :: FilePath -> IO (IO (), Handle)
newFileLogger path = do
    fh <- IO.openFile path IO.AppendMode
    IO.hSetEncoding fh =<< IO.mkTextEncoding "UTF-8//TRANSLIT"
    return $
        (,) (IO.hClose fh) $
        Handle
            { send =
                  \level text -> do
                      time <- getCurrentTime
                      let timestr = formatTime defaultTimeLocale "%F %T.%q" time
                      TextIO.hPutStrLn fh $
                          pack (timestr <> ": " <> show level) <> ": " <> text
                      IO.hFlush fh
            }

-- this logger is intended to be used in test suites
-- if the test within `body` runs successfully, then all the output gets dropped
-- otherwise, if `body` throws - all the output gets written into the console, and the exception (test failure) is passed along unchanged
-- in other words: if `body` doesn't throw, `withTestLogger` behaves like `withNullLogger`
-- otherwise, if `body` does throw, then `withTestLogger` behaves as if it was `withStdLogger` all along
withTestLogger :: (Handle -> IO r) -> IO r
withTestLogger body = do
    pbuf <- newIORef $ return ()
    onException (doBody pbuf) (writeOutput pbuf)
  where
    doBody pbuf = do
        body $
            Handle
                { send =
                      \level text -> modifyIORef' pbuf (>> sendStd level text)
                }
    writeOutput pbuf = do
        join $ readIORef pbuf

withStdLogger :: (Handle -> IO r) -> IO r
withStdLogger body = do
    body $ Handle {send = sendStd}

sendStd :: LogLevel -> Text -> IO ()
sendStd level text = do
    case level of
        Debug -> setSGR [SetColor Foreground Dull Yellow]
        Warn -> setSGR [SetColor Foreground Vivid Yellow]
        Err -> setSGR [SetColor Foreground Vivid Red]
        _ -> return ()
    TextIO.putStrLn $ pack (show level) <> ": " <> text
    setSGR [Reset]

withMultiLogger :: Handle -> Handle -> (Handle -> IO r) -> IO r
withMultiLogger a b body = do
    body $
        Handle
            { send =
                  \level text -> do
                      send a level text
                      send b level text
            }
