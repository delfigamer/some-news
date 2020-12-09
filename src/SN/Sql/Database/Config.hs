module SN.Sql.Database.Config
    ( DatabaseConfig
    , withDatabase
    ) where

import Control.Monad
import Data.Aeson
import qualified Data.HashMap.Strict as Map
import qualified Data.Text as Text
import SN.Sql.Database
import SN.Sql.Database.Postgres
import SN.Sql.Database.Sqlite
import SN.Logger

data DatabaseConfig
    = PostgresConfig String
    | SqliteConfig String

withDatabase :: DatabaseConfig -> Logger -> (Database -> IO r) -> IO r
withDatabase (PostgresConfig conf) = withPostgres conf
withDatabase (SqliteConfig conf) = withSqlite conf

instance FromJSON DatabaseConfig where
    parseJSON = withObject "DatabaseConfig" $ \v -> do
        msum
            [ do
                conf <- v .: "sqlite"
                return $ SqliteConfig conf
            , do
                table <- v .: "postgres"
                let conf = Map.foldrWithKey prependParam "" table
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
