{-# LANGUAGE TypeFamilies #-}

module Storage.Schema
    ( SchemaVersion(..)
    , InitFailure(..)
    , currentSchema
    , matchCurrentSchema
    , upgradeSchema
    ) where

import Control.Exception
import Data.Int
import Data.Proxy
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
    selret <- Db.queryMaybe db $ Select ["sn_metadata"] (fSchemaVersion "mvalue" :/ E) [Where "mkey = 'schema_version'" E] [] AllRows
    case selret of
        Nothing -> do
            Logger.warn logger $ "Storage: No database schema"
            onEmpty
        Just [Just version :/ E] -> do
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
            [ ColumnDecl (FText "mkey") [CCPrimaryKey]
            , ColumnDecl (FText "mvalue") []
            ]
            []
        Db.query db $ Insert "sn_metadata"
            (fString "mkey" :/ fSchemaVersion "mvalue" :/ E)
            (Just "schema_version" :/ Just currentSchema :/ E)
            E
        Db.query db $ CreateTable "sn_users"
            [ ColumnDecl (FBlob "user_id") [CCPrimaryKey]
            , ColumnDecl (FText "user_name") [CCNotNull]
            , ColumnDecl (FText "user_surname") [CCNotNull]
            , ColumnDecl (FTime "user_join_date") [CCNotNull]
            , ColumnDecl (FInt "user_is_admin") []
            ]
            []
        Db.query db $ CreateTable "sn_access_keys"
            [ ColumnDecl (FBlob "access_key_id") [CCPrimaryKey]
            , ColumnDecl (FBlob "access_key_hash") [CCNotNull]
            , ColumnDecl (FBlob "access_key_user_id") [CCNotNull, CCReferences "sn_users" "user_id" FKRCascade FKRCascade]
            ]
            []
        Db.query db $ CreateIndex "sn_access_keys_user_id_idx" "sn_access_keys"
            [Asc "access_key_user_id", Asc "access_key_id"]
        Db.query db $ CreateTable "sn_authors"
            [ ColumnDecl (FBlob "author_id") [CCPrimaryKey]
            , ColumnDecl (FText "author_name") [CCNotNull]
            , ColumnDecl (FText "author_description") [CCNotNull]
            ]
            []
        Db.query db $ CreateTable "sn_author2user"
            [ ColumnDecl (FBlob "a2u_author_id") [CCNotNull, CCReferences "sn_authors" "author_id" FKRCascade FKRCascade]
            , ColumnDecl (FBlob "a2u_user_id") [CCNotNull, CCReferences "sn_users" "user_id" FKRCascade FKRCascade]
            ]
            [ TCPrimaryKey ["a2u_author_id", "a2u_user_id"]
            ]
        Db.query db $ CreateIndex "sn_author2user_rev_idx" "sn_author2user"
            [Asc "a2u_user_id", Asc "a2u_author_id"]
        Db.query db $ CreateTable "sn_articles"
            [ ColumnDecl (FBlob "article_id") [CCPrimaryKey]
            , ColumnDecl (FBlob "article_author_id") [CCReferences "sn_authors" "author_id" FKRCascade FKRSetNull]
            , ColumnDecl (FBlob "article_version") [CCNotNull]
            , ColumnDecl (FText "article_name") [CCNotNull]
            , ColumnDecl (FText "article_text") [CCNotNull]
            , ColumnDecl (FTime "article_publication_date") [CCNotNull]
            ]
            []
        Db.query db $ CreateIndex "sn_articles_main_idx" "sn_articles"
            [Desc "article_publication_date", Asc "article_id"]
        Db.query db $ CreateIndex "sn_articles_author_idx" "sn_articles"
            [Asc "article_author_id", Desc "article_publication_date", Asc "article_id"]
        Db.query db $ CreateTable "sn_files"
            [ ColumnDecl (FBlob "file_id") [CCPrimaryKey]
            , ColumnDecl (FTime "file_name") []
            , ColumnDecl (FTime "file_mimetype") [CCNotNull]
            ]
            []
        Db.query db $ CreateTable "sn_file_chunks"
            [ ColumnDecl (FBlob "chunk_file_id") [CCNotNull, CCReferences "sn_files" "file_id" FKRCascade FKRCascade]
            , ColumnDecl (FInt "chunk_index") [CCNotNull]
            , ColumnDecl (FBlob "chunk_content") [CCNotNull]
            ]
            [ TCPrimaryKey ["chunk_file_id", "chunk_index"]
            ]
        return $ Right ()

upgradeFromToCurrent :: Logger.Handle -> Db.Handle -> SchemaVersion -> IO (Either InitFailure ())
upgradeFromToCurrent logger db version = do
    Logger.err logger $ "Storage: Cannot upgrade from schema: " <> Text.pack (show version)
    return $ Left $ InitFailureIncompatibleSchema version

instance IsValue SchemaVersion where
    type Prims SchemaVersion = '[ 'TText ]
    primDecode vals = do
        VText str :/ E <- return vals
        (version, ""):_ <- return $ reads $ Text.unpack str
        Just version
    primEncode version = VText (Text.pack $ show version) :/ E

fSchemaVersion :: FieldName -> Field SchemaVersion
fSchemaVersion a = Field (FText a :/ E)
