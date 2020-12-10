module SN.Ground.Config
    ( GroundConfig(..)
    , defaultGroundConfig
    ) where

data GroundConfig = GroundConfig
    { groundConfigUserIdLength :: Int
    , groundConfigAccessKeyIdLength :: Int
    , groundConfigAccessKeyTokenLength :: Int
    , groundConfigAuthorIdLength :: Int
    , groundConfigCategoryIdLength :: Int
    , groundConfigArticleIdLength :: Int
    , groundConfigArticleVersionLength :: Int
    , groundConfigTagIdLength :: Int
    , groundConfigCommentIdLength :: Int
    , groundConfigFileIdLength :: Int
    , groundConfigTransactionRetryCount :: Int
    }

defaultGroundConfig :: GroundConfig
defaultGroundConfig = GroundConfig
    { groundConfigUserIdLength = 16
    , groundConfigAccessKeyIdLength = 16
    , groundConfigAccessKeyTokenLength = 48
    , groundConfigAuthorIdLength = 16
    , groundConfigCategoryIdLength = 4
    , groundConfigArticleIdLength = 16
    , groundConfigArticleVersionLength = 16
    , groundConfigTagIdLength = 4
    , groundConfigCommentIdLength = 16
    , groundConfigFileIdLength = 16
    , groundConfigTransactionRetryCount = 1000
    }
