module Logger
    ( LogLevel(..)
    , Handle(..)
    , loggerFilter
    , withNullLogger
    , withFileLogger
    , withHtmlLogger
    , withTestLogger
    , withStdLogger
    , withMultiLogger
    , debug
    , info
    , warn
    , err
    ) where

import Control.Concurrent.MVar
import Control.Exception
import Control.Monad
import Data.IORef
import Data.Text (Text, pack)
import Data.Time.Clock (getCurrentTime)
import Data.Time.Format (defaultTimeLocale, formatTime)
import System.Console.ANSI
import qualified Data.Text.IO as TextIO
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
    show Info  = "INFO "
    show Warn  = "WARN "
    show Err   = "ERROR"
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
        { send = \level text -> do
            when (level >= minlevel) $ send inner level text
        }

withNullLogger :: (Handle -> IO r) -> IO r
withNullLogger body = do
    body $ Handle
        { send = \_ _ -> return ()
        }

withFileLogger :: FilePath -> Bool -> (Handle -> IO r) -> IO r
withFileLogger path clear body = do
    mutex <- newMVar ()
    bracket (IO.openFile path openMode) (IO.hClose) $ \fh -> do
        IO.hSetEncoding fh =<< IO.mkTextEncoding "UTF-8//TRANSLIT"
        body $ Handle
            { send = \level text -> do
                withMVar mutex $ \() -> do
                    time <- getCurrentTime
                    let timestr = formatTime defaultTimeLocale "%F %T.%q" time
                    TextIO.hPutStrLn fh $
                        pack (timestr <> ": " <> show level) <> ": " <> text
                    IO.hFlush fh
            }
  where
    openMode = if clear
        then IO.WriteMode
        else IO.AppendMode

withHtmlLogger :: FilePath -> (Handle -> IO r) -> IO r
withHtmlLogger path body = do
    mutex <- newMVar ()
    bracket
        (do
            fh <- IO.openFile path IO.WriteMode
            TextIO.hPutStrLn fh "<!DOCTYPE html><html><head><style>"
            TextIO.hPutStrLn fh "body {font-family: monospace; font-size: 14px}"
            TextIO.hPutStrLn fh "p.debug {color: #808080}"
            TextIO.hPutStrLn fh "p.info {}"
            TextIO.hPutStrLn fh "p.warn {background-color: #ffff80}"
            TextIO.hPutStrLn fh "p.err {background-color: #ff8080}"
            TextIO.hPutStrLn fh "p.top {background-color: #00ffff}"
            TextIO.hPutStrLn fh "p {margin:0; padding: 4px 2px}"
            TextIO.hPutStrLn fh "</style></head><body>"
            return fh)
        (\fh -> do
            TextIO.hPutStrLn fh "</body></html>"
            IO.hClose fh)
        (\fh -> do
            IO.hSetEncoding fh =<< IO.mkTextEncoding "UTF-8//TRANSLIT"
            body $ Handle
                { send = \level text -> do
                    withMVar mutex $ \() -> do
                        time <- getCurrentTime
                        let timestr = formatTime defaultTimeLocale "%F %T.%q" time
                        let innerline = pack (timestr <> ": " <> show level) <> ": " <> text
                        let outerline = case level of
                                Debug -> "<p class=\"debug\">" <> innerline <> "</p>"
                                Info -> "<p class=\"info\">" <> innerline <> "</p>"
                                Warn -> "<p class=\"warn\">" <> innerline <> "</p>"
                                Err -> "<p class=\"err\">" <> innerline <> "</p>"
                                Topmost -> "<p class=\"top\">" <> innerline <> "</p>"
                        TextIO.hPutStrLn fh outerline
                        IO.hFlush fh
                })

-- this logger is intended to be used in test suites
-- if the test within `body` runs successfully, then all the output gets dropped
-- otherwise, if `body` throws - all the output gets written into the console, and the exception (test failure) is passed along unchanged
-- in other words: if `body` doesn't throw, `withTestLogger` behaves like `withNullLogger`
-- otherwise, if `body` does throw, then `withTestLogger` behaves as if it was `withStdLogger` all along
withTestLogger :: (Handle -> IO r) -> IO r
withTestLogger body = do
    pbuf <- newIORef $ id
    doBody pbuf `onException` writeOutput pbuf
  where
    doBody pbuf = do
        body $ Handle
            { send = \level text -> do
                atomicModifyIORef pbuf $ \buf1 -> do
                    let buf2 = \a -> buf1 ((level, text) : a)
                    (buf2, ())
            }
    writeOutput pbuf = do
        buf <- readIORef pbuf
        forM_ (buf []) $ \(level, text) -> writeToStd level text

withStdLogger :: (Handle -> IO r) -> IO r
withStdLogger body = do
    mutex <- newMVar ()
    body $ Handle
        { send = \level text -> do
            withMVar mutex $ \() -> writeToStd level text
        }

writeToStd :: LogLevel -> Text -> IO ()
writeToStd level text = do
    case level of
        Debug -> setSGR [SetColor Foreground Dull Yellow]
        Warn -> setSGR [SetColor Foreground Vivid Yellow]
        Err -> setSGR [SetColor Foreground Vivid Red]
        _ -> return ()
    TextIO.putStrLn $ pack (show level) <> ": " <> text
    setSGR [Reset]

withMultiLogger :: Handle -> Handle -> (Handle -> IO r) -> IO r
withMultiLogger a b body = do
    body $ Handle
        { send = \level text -> do
            send a level text
            send b level text
        }
