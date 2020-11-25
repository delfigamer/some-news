module JsonInterface.Config
    ( Config(..)
    ) where

import Data.Aeson
import Data.Int

data Config = Config
    { defaultPageLimit :: Int64
    , maxPageLimit :: Int64
    , maxAccessKeyCount :: Int64
    }

instance FromJSON Config where
    parseJSON = withObject "JsonInterface.Config" $ \v -> do
        Config
            <$> v .:? "defaultPageLimit" .!= 20
            <*> v .:? "maxPageLimit" .!= 100
            <*> v .:? "maxAccessKeyCount" .!= 100
