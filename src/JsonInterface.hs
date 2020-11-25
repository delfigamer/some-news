module JsonInterface
    ( ResponseStatus(..)
    , ResponseContent(..)
    , Response(..)
    , JsonInterface(..)
    , withJsonInterface
    ) where

import Control.Monad.Cont
import Control.Monad.Reader
import Data.Void
import qualified Data.ByteString as BS
import qualified Data.HashMap.Strict as Map
import qualified Data.Text as Text
import JsonInterface.Internal
import Hex
import Logger
import Storage
import qualified JsonInterface.Config as Config

data JsonInterface = JsonInterface
    { simpleRequest :: [Text.Text] -> Map.HashMap BS.ByteString BS.ByteString -> IO Response
    }

withJsonInterface :: Config.Config -> Logger -> Storage -> (JsonInterface -> IO r) -> IO r
withJsonInterface config logger storage body = do
    let context = Context config logger storage
    body $ JsonInterface
        { simpleRequest = \rname params -> do
            logDebug logger $ "JsonInterface: " << rnameText rname << paramText params
            runRequestHandler params context $ handleSimpleRequest rname
        }
  where
    rnameText (a : b : bs) = toLog a << "/" << rnameText (b : bs)
    rnameText [a] = toLog a
    rnameText [] = ""
    paramText = Map.foldrWithKey
        (\pname pvalue rest -> "\n    " <<| pname << " = " <<| pvalue << rest)
        ""

handleSimpleRequest :: [Text.Text] -> RequestHandler Void
handleSimpleRequest ["users", "create"] = do
    name <- getParam "name" textParser
    surname <- getParam "surname" textParser
    user <- performRequire $ UserCreate name surname
    exitOk user
handleSimpleRequest ["users", "me"] = do
    myUser <- requireUserAccess
    exitOk myUser
handleSimpleRequest ["users", "setName"] = do
    newName <- getParam "name" textParser
    newSurname <- getParam "surname" textParser
    myUser <- requireUserAccess
    performRequire $ UserSetName (userId myUser) newName newSurname
    exitOk True
handleSimpleRequest ["users", "delete"] = do
    ref <- getParam "user" referenceParser
    myUser <- requireUserAccess
    when (ref /= userId myUser) $ do
        exitError $ ErrInvalidParameter "user"
    performRequire $ UserDelete ref
    exitOk True

handleSimpleRequest ["users", "setName_"] = do
    requireAdminAccess
    ref <- getParam "user" referenceParser
    newName <- getParam "name" textParser
    newSurname <- getParam "surname" textParser
    performRequire $ UserSetName ref newName newSurname
    exitOk True
handleSimpleRequest ["users", "grantAdmin_"] = do
    requireAdminAccess
    ref <- getParam "user" referenceParser
    performRequire $ UserSetIsAdmin ref True
    exitOk True
handleSimpleRequest ["users", "dropAdmin_"] = do
    requireAdminAccess
    ref <- getParam "user" referenceParser
    performRequire $ UserSetIsAdmin ref False
    exitOk True
handleSimpleRequest ["users", "delete_"] = do
    requireAdminAccess
    ref <- getParam "user" referenceParser
    performRequire $ UserDelete ref
    exitOk True
handleSimpleRequest ["users", "list_"] = do
    requireAdminAccess
    config <- getConfig
    offset <- getParam "offset" $
        withDefault 0 $ intParser 0 maxBound
    limit <- getParam "limit" $
        withDefault (Config.defaultPageLimit config) $ intParser 1 (Config.maxPageLimit config)
    order <- getParam "orderBy" $
        sortOrderParser
            [ ("name", OrderUserName)
            , ("surname", OrderUserSurname)
            , ("joinDate", OrderUserJoinDate)
            , ("isAdmin", OrderUserIsAdmin)
            ]
    elems <- performRequire $ UserList $ ListView offset limit [] order
    exitOk elems

handleSimpleRequest _ = do
    exitError $ ErrUnknownRequest

    -- AccessKeyCreate :: Reference User -> Action AccessKey
    -- AccessKeyDelete :: Reference User -> Reference AccessKey -> Action ()
    -- AccessKeyList :: Reference User -> ListView AccessKey -> Action [Reference AccessKey]
    -- AccessKeyLookup :: AccessKey -> Action (Reference User)

    -- AuthorCreate :: Text.Text -> Text.Text -> Action Author
    -- AuthorSetName :: Reference Author -> Text.Text -> Action ()
    -- AuthorSetDescription :: Reference Author -> Text.Text -> Action ()
    -- AuthorDelete :: Reference Author -> Action ()
    -- AuthorList :: ListView Author -> Action [Author]
    -- AuthorSetOwnership :: Reference Author -> Reference User -> Bool -> Action ()

    -- CategoryCreate :: Text.Text -> Reference Category -> Action Category
    -- CategorySetName :: Reference Category -> Text.Text -> Action ()
    -- CategorySetParent :: Reference Category -> Reference Category -> Action ()
    -- CategoryDelete :: Reference Category -> Action ()
    -- CategoryList :: ListView Category -> Action [Category]

    -- ArticleCreate :: Reference Author -> Action Article
    -- ArticleSetAuthor :: Reference Article -> Reference Author -> Action ()
    -- ArticleSetName :: Reference Article -> Version Article -> Text.Text -> Action (Version Article)
    -- ArticleSetText :: Reference Article -> Version Article -> Text.Text -> Action (Version Article)
    -- ArticleSetCategory :: Reference Article -> Reference Category -> Action ()
    -- ArticleSetPublicationStatus :: Reference Article -> PublicationStatus -> Action ()
    -- ArticleDelete :: Reference Article -> Action ()
    -- ArticleList :: Bool -> ListView Article -> Action [Article]
    -- ArticleSetTag :: Reference Article -> Reference Tag -> Bool -> Action ()

    -- TagCreate :: Text.Text -> Action Tag
    -- TagSetName :: Reference Tag -> Text.Text -> Action ()
    -- TagDelete :: Reference Tag -> Action ()
    -- TagList :: ListView Tag -> Action [Tag]

    -- CommentCreate :: Reference Article -> Reference User -> Text.Text -> Action Comment
    -- CommentSetText :: Reference Comment -> Text.Text -> Action ()
    -- CommentDelete :: Reference Comment -> Action ()
    -- CommentList :: ListView Comment -> Action [Comment]
