module Sql.Database.Config
    ( Config
    , withDatabase
    ) where

import Control.Monad
import Data.Aeson
import qualified Data.HashMap.Strict as H
import qualified Data.Text as Text
import qualified Logger
import Sql.Database
import Sql.Database.Postgres
import Sql.Database.Sqlite

data Config
    = PostgresConfig String
    | SqliteConfig String

withDatabase :: Config -> Logger.Handle -> (Handle -> IO r) -> IO r
withDatabase (PostgresConfig conf) = withPostgres conf
withDatabase (SqliteConfig conf) = withSqlite conf

instance FromJSON Config where
    parseJSON = withObject "DatabaseConfig" $ \v -> do
        msum
            [ do
                conf <- v .: "sqlite"
                return $ SqliteConfig conf
            , do
                table <- v .: "postgres"
                let conf = H.foldrWithKey prependParam "" table
                return $ PostgresConfig conf
            ]

prependParam :: Text.Text -> Text.Text -> String -> String
prependParam key value str =
    Text.unpack key ++ "='" ++ escapeParamValue (Text.unpack value) ++ "' " ++ str

escapeParamValue :: String -> String
escapeParamValue ('\'' : rest) = '\\' : '\'' : escapeParamValue rest
escapeParamValue ('\\' : rest) = '\\' : '\\' : escapeParamValue rest
escapeParamValue (c : rest) = c : escapeParamValue rest
escapeParamValue [] = []
