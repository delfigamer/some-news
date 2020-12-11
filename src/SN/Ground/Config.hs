module SN.Ground.Config
    ( GroundConfig(..)
    , defaultGroundConfig
    ) where

import Data.Aeson

data GroundConfig = GroundConfig
    { groundConfigUserIdLength :: !Int
    , groundConfigAccessKeyIdLength :: !Int
    , groundConfigAccessKeyTokenLength :: !Int
    , groundConfigAuthorIdLength :: !Int
    , groundConfigCategoryIdLength :: !Int
    , groundConfigArticleIdLength :: !Int
    , groundConfigArticleVersionLength :: !Int
    , groundConfigTagIdLength :: !Int
    , groundConfigCommentIdLength :: !Int
    , groundConfigFileIdLength :: !Int
    , groundConfigTransactionRetryCount :: !Int
    }

instance FromJSON GroundConfig where
    parseJSON = withObject "Ground.Config" $ \v -> do
        GroundConfig
            <$> v .:? "userIdLength" .!= 18
            <*> v .:? "accessKeyIdLength" .!= 18
            <*> v .:? "accessKeyTokenLength" .!= 36
            <*> v .:? "authorIdLength" .!= 18
            <*> v .:? "categoryIdLength" .!= 6
            <*> v .:? "articleIdLength" .!= 18
            <*> v .:? "articleVersionLength" .!= 18
            <*> v .:? "tagIdLength" .!= 6
            <*> v .:? "commentIdLength" .!= 18
            <*> v .:? "fileIdLength" .!= 18
            <*> v .:? "transactionRetryCount" .!= 1000

defaultGroundConfig :: GroundConfig
Success defaultGroundConfig = fromJSON $ Object mempty
