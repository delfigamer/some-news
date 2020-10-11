module Storage.Schema
    ( SchemaVersion(..)
    , InitFailure(..)
    , currentSchema
    , matchCurrentSchema
    , upgradeSchema
    ) where

import Control.Exception
import Data.Int
import qualified Data.Text as Text
import qualified Logger
import Sql.Query
import qualified Sql.Database as Db
import Tuple

data SchemaVersion = SomeNewsSchema Int Int
    deriving (Show, Read, Eq)

data InitFailure
    = InitFailureEmpty -- no schema, can upgrade
    | InitFailureObsoleteSchema SchemaVersion -- db schema is older than current, can upgrade
    | InitFailureAdvancedSchema SchemaVersion -- db schema is newer than current in minor version (backwards-compatible), can open in read-only
    | InitFailureIncompatibleSchema SchemaVersion -- db schema is newer than current in major version (compatibility-breaking), cannot open safely
    | InitFailureInvalidSchema -- invalid or corrupted schema metadata, cannot open safely
    deriving (Show, Eq)

matchCurrentSchema :: Logger.Handle -> Db.Handle -> (InitFailure -> IO r) -> IO r -> IO r
matchCurrentSchema logger db onFail onMatch = do
    withSchemaVersion logger db
        (onFail InitFailureInvalidSchema)
        (onFail InitFailureEmpty)
        $ \dbSchema@(SomeNewsSchema dbMajor dbMinor) -> do
            case currentSchema of
                SomeNewsSchema myMajor myMinor
                    | dbMajor < myMajor -> onFail $ InitFailureObsoleteSchema dbSchema
                    | dbMajor > myMajor -> onFail $ InitFailureIncompatibleSchema dbSchema
                    | dbMinor < myMinor -> onFail $ InitFailureObsoleteSchema dbSchema
                    | dbMinor > myMinor -> onFail $ InitFailureAdvancedSchema dbSchema
                    | otherwise -> onMatch

upgradeSchema :: Logger.Handle -> Db.Handle -> IO (Either InitFailure ())
upgradeSchema logger db = do
    withSchemaVersion logger db
        (return $ Left InitFailureInvalidSchema)
        (upgradeEmptyToCurrent logger db)
        $ \dbSchema@(SomeNewsSchema dbMajor dbMinor) -> do
            case currentSchema of
                SomeNewsSchema myMajor myMinor
                    | dbMajor < myMajor -> upgradeFromToCurrent logger db dbSchema
                    | dbMajor > myMajor -> return $ Left $ InitFailureIncompatibleSchema dbSchema
                    | dbMinor < myMinor -> upgradeFromToCurrent logger db dbSchema
                    | dbMinor > myMinor -> return $ Left $ InitFailureAdvancedSchema dbSchema
                    | otherwise -> do
                        return $ Right ()

withSchemaVersion :: Logger.Handle -> Db.Handle -> IO r -> IO r -> (SchemaVersion -> IO r) -> IO r
withSchemaVersion logger db onInvalid onEmpty onSchema = do
    Logger.info logger $ "Storage: Current application schema: " <> Text.pack (show currentSchema)
    selret <- Db.queryMaybe db $ Select "sn_metadata" (fString "mvalue" :* E) (Just (Condition "mkey = 'schema_version'" E)) Nothing Nothing
    case selret of
        Nothing -> do
            Logger.warn logger $ "Storage: No database schema"
            onEmpty
        Just [Val versionstr :* E] | (version, ""):_ <- reads versionstr -> do
            Logger.info logger $ "Storage: Database schema: " <> Text.pack (show version)
            onSchema version
        Just _ -> do
            Logger.err logger $ "Storage: Invalid database schema"
            onInvalid

currentSchema :: SchemaVersion
currentSchema = SomeNewsSchema 0 1

upgradeEmptyToCurrent :: Logger.Handle -> Db.Handle -> IO (Either InitFailure ())
upgradeEmptyToCurrent logger db = do
    Logger.info logger $ "Storage: Create a new database from scratch"
    Db.withTransaction db $ do
        Db.query db $ CreateTable "sn_metadata"
            [ ColumnDecl (FText "mkey") [CPrimaryKey]
            , ColumnDecl (FText "mvalue") []
            ]
            []
        Db.query db $ Insert "sn_metadata"
            (fString "mkey" :* fString "mvalue" :* E)
            [Val "schema_version" :* Val (show currentSchema) :* E]
        Db.query db $ CreateTable "sn_users"
            [ ColumnDecl (FInt "user_id") [CIntegerId]
            , ColumnDecl (FInt "user_salt") [CIntegerSalt]
            , ColumnDecl (FText "user_name") []
            , ColumnDecl (FText "user_surname") []
            ]
            []
        return $ Right ()

upgradeFromToCurrent :: Logger.Handle -> Db.Handle -> SchemaVersion -> IO (Either InitFailure ())
upgradeFromToCurrent logger db version = do
    Logger.err logger $ "Storage: Cannot upgrade from schema: " <> Text.pack (show version)
    return $ Left $ InitFailureIncompatibleSchema version
