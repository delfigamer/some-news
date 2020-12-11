module SN.Medium.Config
    ( MediumConfig(..)
    , defaultMediumConfig
    ) where

import Data.Aeson
import Data.Time.Clock
import Data.Int
import qualified Data.Text as Text

data MediumConfig = MediumConfig
    { mediumConfigDefaultPageLimit :: !Int64
    , mediumConfigMaxPageLimit :: !Int64
    , mediumConfigMinPasswordLength :: !Int
    , mediumConfigMaxAccessKeyCount :: !Int
    , mediumConfigTicketLength :: !Int
    , mediumConfigTicketLifetime :: !NominalDiffTime
    , mediumConfigFileChunkSize :: !Int64
    , mediumConfigMaxFileSize :: !Int64
    }

instance FromJSON MediumConfig where
    parseJSON = withObject "Medium.Config" $ \v -> do
        MediumConfig
            <$> v .:? "defaultPageLimit" .!= 20
            <*> v .:? "maxPageLimit" .!= 100
            <*> v .:? "minPasswordLength" .!= 8
            <*> v .:? "maxAccessKeyCount" .!= 100
            <*> v .:? "ticketLength" .!= 32
            <*> v .:? "ticketLifetime" .!= 300
            <*> v .:? "fileChunkSize" .!= 0x10000 -- 64 K
            <*> v .:? "maxFileSize" .!= 0x1000000 -- 16 M

defaultMediumConfig :: MediumConfig
Success defaultMediumConfig = fromJSON $ Object mempty
