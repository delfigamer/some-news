module Main
    ( main
    ) where

import Control.Monad.Trans.Writer.CPS
import Data.Aeson
import Data.String
import GHC.IO.Encoding (textEncodingName)
import System.Environment
import System.IO
import qualified Data.Text as Text
import qualified Data.Yaml as Yaml
import qualified Network.Wai.Handler.Warp as Warp
import qualified Network.Wai.Handler.WarpTLS as Warp
import SN.Ground
import SN.Logger
import SN.Logger.Config
import SN.Medium
import SN.Sql.Database.Config
import SN.Surface

data CommandArgs = CommandArgs
    { cmdaConfigFile :: FilePath
    , cmdaRest :: [String]
    }

cmdaSetConfigFile :: FilePath -> CommandArgs -> CommandArgs
cmdaSetConfigFile fpath c = c
    { cmdaConfigFile = fpath
    }

cmdaPrependRest :: String -> CommandArgs -> CommandArgs
cmdaPrependRest x c = c
    { cmdaRest = x : cmdaRest c
    }

parseArgs :: [String] -> Either String CommandArgs
parseArgs [] = Right $ CommandArgs
    { cmdaConfigFile = "config.yaml"
    , cmdaRest = []
    }
parseArgs ("-config" : fpath : rest) = cmdaSetConfigFile fpath <$> parseArgs rest
parseArgs (pname@('-' : _) : _) = Left $ "Unknown parameter: " ++ pname
parseArgs (x : rest) = cmdaPrependRest x <$> parseArgs rest

main :: IO ()
main = do
    Just encoding1 <- hGetEncoding stdout
    encoding2 <- mkTextEncoding $ textEncodingName encoding1 ++ "//TRANSLIT"
    hSetEncoding stdout encoding2
    rawArgs <- getArgs
    case parseArgs rawArgs of
        Left err -> putStrLn err >> printUsage
        Right (CommandArgs confPath args) -> case args of
            "start" : _ -> do
                MainConfig logConf dbConf groundConf mediumConf warpSettings tlsSettings <- Yaml.decodeFileThrow confPath
                withLogger logConf $ \logger -> do
                    withDatabase dbConf logger $ \db -> do
                        withSqlGround groundConf logger db (onInitFailure logger) $ \ground -> do
                            withMedium mediumConf logger ground $ \medium -> do
                                Warp.runTLS tlsSettings warpSettings $ surfaceApplication logger medium
            "upgrade" : _ -> do
                UpgradeConfig logConf dbConf <- Yaml.decodeFileThrow confPath
                withLogger logConf $ \logger -> do
                    withDatabase dbConf logger $ \db -> do
                        upgradeResult <- upgradeSchema logger db
                        case upgradeResult of
                            Right () -> logInfo logger $ "Upgraded successfully to schema: " <<| currentSchema
                            Left err -> onInitFailure logger err
            "createAdmin" : aname : asurname : apassword : _ -> do
                ActionConfig logConf dbConf groundConf <- Yaml.decodeFileThrow confPath
                withLogger logConf $ \logger -> do
                    withDatabase dbConf logger $ \db -> do
                        withSqlGround groundConf logger db (onInitFailure logger) $ \ground -> do
                            ret <- groundPerform ground $ UserCreate
                                (fromString aname) (fromString asurname) (fromString apassword) True
                            case ret of
                                Left err -> logErr logger $ "Failed to create the admin:\n    " <<| err
                                Right user -> logInfo logger $ "Created successfully:\n    " <<| user
            _ -> printUsage
  where
    onInitFailure logger reason = do
        logErr logger $ "Cannot use the given database: " << logReason reason
    logReason InitFailureEmpty =
        "database is empty or not set up for this application\n\
        \    database upgrade may be possible"
    logReason (InitFailureObsoleteSchema dbSchema) =
        "reported schema is too old: " <<| currentSchema << " expected, " <<| dbSchema << " reported\n\
        \    database upgrade may be possible"
    logReason (InitFailureAdvancedSchema dbSchema) =
        "reported schema is too new: " <<| currentSchema << " expected, " <<| dbSchema << " reported\n\
        \    database may be opened in a read-only mode (NYI)"
    logReason (InitFailureIncompatibleSchema dbSchema) =
        "incompatible schema: " <<| currentSchema << " expected, " <<| dbSchema << " reported"
    logReason InitFailureInvalidSchema = "invalid metadata"
    logReason InitFailureDatabaseError = "internal database error"
    printUsage = do
        putStrLn "Commands:"
        putStrLn "    SomeNews start"
        putStrLn "    SomeNews upgrade"
        putStrLn "    SomeNews createAdmin <name> <surname> <password>"
        putStrLn "Options:"
        putStrLn "    -config <configFile.yaml>"
        putStrLn "        default: \"config.yaml\""


parseHostString :: Text.Text -> Maybe (Maybe Warp.HostPreference, Maybe Warp.Port)
parseHostString str = do
    let (left, right) = Text.breakOn ":" str
    let mAddr = case left of
            "" -> Nothing
            _ -> Just $ fromString $ Text.unpack left
    case right of
        "" -> Just (mAddr, Nothing)
        _ -> case reads $ Text.unpack $ Text.drop 1 right of
            (port, "") : _ -> Just (mAddr, Just port)
            _ -> Nothing

data MainConfig = MainConfig
    LoggerConfig
    DatabaseConfig
    GroundConfig
    MediumConfig
    Warp.Settings
    Warp.TLSSettings

instance FromJSON MainConfig where
    parseJSON = withObject "MainConfig" $ \v -> do
        mHostString <- v .:? "host" .!= ""
        warpSettingFunc <- case parseHostString mHostString of
            Nothing -> fail "invalid host value"
            Just (mAddr, mPort) -> return $ maybe id Warp.setHost mAddr . maybe id Warp.setPort mPort
        tlsCertFile <- v .: "tlsCertFile"
        tlsKeyFile <- v .: "tlsKeyFile"
        logConf <- v .:? "logger" .!= StdLoggerConfig
        dbConf <- v .: "database"
        groundConf <- v .:? "ground" .!= defaultGroundConfig
        mediumConf <- v .:? "medium" .!= defaultMediumConfig
        return $ MainConfig
            logConf
            dbConf
            groundConf
            mediumConf
            (warpSettingFunc Warp.defaultSettings)
            (Warp.tlsSettings tlsCertFile tlsKeyFile)

data ActionConfig = ActionConfig
    LoggerConfig
    DatabaseConfig
    GroundConfig

instance FromJSON ActionConfig where
    parseJSON = withObject "MainConfig" $ \v -> do
        ActionConfig
            <$> v .:? "logger" .!= StdLoggerConfig
            <*> v .: "database"
            <*> v .:? "ground" .!= defaultGroundConfig

data UpgradeConfig = UpgradeConfig
    LoggerConfig
    DatabaseConfig

instance FromJSON UpgradeConfig where
    parseJSON = withObject "MainConfig" $ \v -> do
        UpgradeConfig
            <$> v .:? "logger" .!= StdLoggerConfig
            <*> v .: "database"
