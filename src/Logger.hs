{-# LANGUAGE DefaultSignatures #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}

module Logger
    ( ToLog(..)
    , showLog
    , (<<)
    , (<<|)
    , (<<||)
    , LogLevel(..)
    , Logger(..)
    , logDebug
    , logInfo
    , logWarn
    , logErr
    , loggerFilter
    , nullLogger
    , withFileLogger
    , withHtmlLogger
    , withTestLogger
    , stdLogger
    , joinLoggers
    ) where

import Control.Concurrent.MVar
import Control.Exception
import Control.Monad
import Data.IORef
import Data.String
import System.Console.ANSI
import System.IO.Unsafe (unsafePerformIO)
import qualified Data.Text as TextStrict
import qualified Data.Text.Lazy as Text
import qualified Data.Text.Lazy.Builder as Builder
import qualified Data.Text.Lazy.IO as TextIO
import qualified Data.Time.Clock as Time
import qualified Data.Time.Format as Time
import qualified System.IO as IO

newtype Log = Log (Builder.Builder -> Builder.Builder)

instance IsString Log where
    fromString s = Log $ \z -> Builder.fromString s <> z

class ToLog a where
    toLog :: a -> Log

instance ToLog Log where
    toLog = id

instance ToLog Builder.Builder where
    toLog b = Log $ \z -> b <> z

instance ToLog Text.Text where
    toLog = toLog . Builder.fromLazyText

instance ToLog TextStrict.Text where
    toLog = toLog . Builder.fromText

instance ToLog String where
    toLog = toLog . Builder.fromString

showLog :: Show a => a -> Log
showLog = toLog . show

infixl 1 <<
(<<) :: Log -> Log -> Log
Log f << Log g = Log $ \z -> f (g z)

infixl 1 <<|
(<<|) :: Show a => Log -> a -> Log
x <<| y = x << showLog y

infixl 1 <<||
(<<||) :: ToLog a => Log -> a -> Log
x <<|| y = x << toLog y

data LogLevel
    = LevelDebug
    | LevelInfo
    | LevelWarn
    | LevelErr
    | LevelTopmost
    deriving (Ord, Eq)

instance Show LogLevel where
    show LevelDebug = "DEBUG"
    show LevelInfo  = "INFO "
    show LevelWarn  = "WARN "
    show LevelErr   = "ERROR"
    show LevelTopmost = "TOP  "

newtype Logger =
    Logger
        { logSend :: LogLevel -> Log -> IO ()
        }

logDebug :: Logger -> Log -> IO ()
logDebug h = logSend h LevelDebug

logInfo :: Logger -> Log -> IO ()
logInfo h = logSend h LevelInfo

logWarn :: Logger -> Log -> IO ()
logWarn h = logSend h LevelWarn

logErr :: Logger -> Log -> IO ()
logErr h = logSend h LevelErr

builderFromLevel :: LogLevel -> Builder.Builder
builderFromLevel = Builder.fromString . show

loggerFilter :: LogLevel -> Logger -> Logger
loggerFilter minlevel inner = do
    Logger
        { logSend = \level text -> do
            when (level >= minlevel) $ logSend inner level text
        }

nullLogger :: Logger
nullLogger = Logger
    { logSend = \_ _ -> return ()
    }

withFileLogger :: FilePath -> Bool -> (Logger -> IO r) -> IO r
withFileLogger path clear body = do
    mutex <- newMVar ()
    bracket (IO.openFile path openMode) (IO.hClose) $ \fh -> do
        IO.hSetEncoding fh =<< IO.mkTextEncoding "UTF-8//TRANSLIT"
        body $ Logger
            { logSend = \level (Log line) -> do
                withMVar mutex $ \() -> do
                    timestr <- currentTimestamp
                    TextIO.hPutStrLn fh $ Builder.toLazyText $
                        timestr <> ": " <> builderFromLevel level <> ": " <> line mempty
                    IO.hFlush fh
            }
  where
    openMode = if clear
        then IO.WriteMode
        else IO.AppendMode

withHtmlLogger :: FilePath -> (Logger -> IO r) -> IO r
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
            body $ Logger
                { logSend = \level (Log line) -> do
                    withMVar mutex $ \() -> do
                        timestr <- currentTimestamp
                        let lineText = Builder.toLazyText $ line mempty
                        let line2 = Builder.fromLazyText $ Text.concatMap escapeChar lineText
                        let inner = timestr <> ": " <> builderFromLevel level <> ": " <> line2
                        let outer = case level of
                                LevelDebug -> "<p class=\"debug\">" <> inner <> "</p>"
                                LevelInfo -> "<p class=\"info\">" <> inner <> "</p>"
                                LevelWarn -> "<p class=\"warn\">" <> inner <> "</p>"
                                LevelErr -> "<p class=\"err\">" <> inner <> "</p>"
                                LevelTopmost -> "<p class=\"top\">" <> inner <> "</p>"
                        TextIO.hPutStrLn fh $ Builder.toLazyText outer
                        IO.hFlush fh
                })
  where
    escapeChar '<' = "&lt;"
    escapeChar '>' = "&gt;"
    escapeChar '&' = "&amp;"
    escapeChar c = Text.singleton c

currentTimestamp :: IO Builder.Builder
currentTimestamp = do
    time <- Time.getCurrentTime
    return $ Builder.fromString $
        Time.formatTime Time.defaultTimeLocale "%F %T.%q" time

{- this logger is intended to be used in test suites -}
{- if the test within `body` runs successfully, then all the output gets dropped -}
{- otherwise, if `body` throws - all the output gets written into the console, and the exception (test failure) is passed along unchanged -}
{- in other words: if `body` doesn't throw, `withTestLogger` behaves like `withNullLogger` -}
{- otherwise, if `body` does throw, then `withTestLogger` behaves as if it was `withStdLogger` all along -}
withTestLogger :: (Logger -> IO r) -> IO r
withTestLogger body = do
    pbuf <- newIORef $ id
    doBody pbuf `onException` writeOutput pbuf
  where
    doBody pbuf = do
        body $ Logger
            { logSend = \level (Log line) -> do
                atomicModifyIORef pbuf $ \buf1 -> do
                    let buf2 = \a -> buf1 ((level, Builder.toLazyText $ line mempty) : a)
                    (buf2, ())
            }
    writeOutput pbuf = do
        buf <- readIORef pbuf
        forM_ (buf []) $ \(level, text) -> writeToStd level (toLog text)

stdLogger :: Logger
stdLogger = Logger
    { logSend = \level text -> do
        withMVar stdOutputMutex $ \() -> writeToStd level text
    }

writeToStd :: LogLevel -> Log -> IO ()
writeToStd level (Log line) = do
    case level of
        LevelDebug -> setSGR [SetColor Foreground Dull Yellow]
        LevelWarn -> setSGR [SetColor Foreground Vivid Yellow]
        LevelErr -> setSGR [SetColor Foreground Vivid Red]
        _ -> return ()
    TextIO.putStrLn $ Builder.toLazyText $
        builderFromLevel level <> ": " <> line mempty
    setSGR [Reset]

stdOutputMutex :: MVar ()
stdOutputMutex = unsafePerformIO $ newMVar ()
{-# NOINLINE stdOutputMutex #-}

joinLoggers :: [Logger] -> Logger
joinLoggers hs = Logger
    { logSend = \level text -> do
        forM_ hs $ \h -> logSend h level text
    }
