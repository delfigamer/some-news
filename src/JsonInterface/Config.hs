module JsonInterface.Config
    ( Config(..)
    ) where

import Data.Aeson
import Data.Time.Clock
import Data.Int
import qualified Data.Text as Text

data Config = Config
    { defaultPageLimit :: Int64
    , maxPageLimit :: Int64
    , minPasswordLength :: Int
    , maxAccessKeyCount :: Int64
    , ticketLength :: Int
    , ticketLifetime :: NominalDiffTime
    , fileChunkSize :: Int64
    , maxFileSize :: Int64
    }

instance FromJSON Config where
    parseJSON = withObject "JsonInterface.Config" $ \v -> do
        Config
            <$> v .:? "defaultPageLimit" .!= 20
            <*> v .:? "maxPageLimit" .!= 100
            <*> v .:? "minPasswordLength" .!= 8
            <*> v .:? "maxAccessKeyCount" .!= 100
            <*> v .:? "ticketLength" .!= 32
            <*> v .:? "ticketLifetime" .!= 300
            <*> v .:? "fileChunkSize" .!= 0x10000 -- 64 K
            <*> v .:? "maxFileSize" .!= 0x1000000 -- 16 M
