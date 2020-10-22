{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE StandaloneDeriving #-}
{-# LANGUAGE TypeFamilies #-}

module Storage.Model
    ( assertOk
    , Model
    , newStorageModel
    , modelSetUser
    , modelDeleteUser
    , modelListUsers
    , modelSetAccessKey
    , modelDeleteAccessKey
    , modelListAccessKeys
    , modelSetAuthor
    , modelDeleteAuthor
    , modelListAuthors
    , modelConnectUserAuthor
    , modelDisconnectUserAuthor
    , modelListAuthorsOfUser
    , modelListUsersOfAuthor
    , modelSetArticle
    , modelDeleteArticle
    , modelListArticleVersions
    , modelListArticles
    , modelListArticlesOfAuthor
    , modelListArticlesOfUser
    , verifyUsers
    , verifyAccessKeys
    , verifyAuthors
    , verifyUserAuthorConns
    , verifyArticles
    ) where

import Control.Applicative
import Control.Exception
import Control.Monad
import Data.Bits
import Data.Function
import Data.IORef
import Data.Int
import Data.List
import Data.Maybe
import Data.Proxy
import Data.Time.Clock
import Data.Word
import Test.Hspec
import qualified Crypto.Hash as CHash
import qualified Crypto.Random as CRand
import qualified Data.ByteArray as BA
import qualified Data.ByteString as BS
import qualified Data.Map as Map
import qualified Data.Set as Set
import qualified Data.Text as Text
import Sql.Query
import Storage
import Storage.Schema
import Tuple
import qualified Logger
import qualified Sql.Database as Db

assertOk :: (Show a, HasCallStack) => Result a -> IO a
assertOk (Ok x) = return x
assertOk r = expectationFailure ("Expected an Ok, got " ++ show r) >> undefined

data UserRel = UserRel
    { urUser :: IORef User
    , urAccessKeys :: IORef (Set.Set AccessKey)
    , urAuthors :: IORef (Set.Set (Reference Author))
    }

data AuthorRel = AuthorRel
    { arAuthor :: IORef Author
    , arUsers :: IORef (Set.Set (Reference User))
    , arArticles :: IORef (Set.Set (Reference Article))
    }

data ArticleRel = ArticleRel
    { rrArticle :: IORef Article
    , rrVersion :: IORef (Version Article)
    }

data Model = Model
    { mUsers :: IORef (Map.Map (Reference User) UserRel)
    , mAuthors :: IORef (Map.Map (Reference Author) AuthorRel)
    , mArticles :: IORef (Map.Map (Reference Article) ArticleRel)
    }

newStorageModel :: IO Model
newStorageModel = do
    Model
        <$> newIORef Map.empty
        <*> newIORef Map.empty
        <*> newIORef Map.empty

modelSetUser :: Model -> Reference User -> User -> IO ()
modelSetUser model userRef user = do
    setTableRow (mUsers model) userRef
        (UserRel
            <$> newIORef user
            <*> newIORef Set.empty
            <*> newIORef Set.empty)
        (\userRel -> do
            atomicWriteIORef (urUser userRel) $! user)

modelDeleteUser :: Model -> Reference User -> IO ()
modelDeleteUser model userRef = do
    withTableRow_ (mUsers model) userRef $ \userRel -> do
        authorSet <- readIORef (urAuthors userRel)
        forTableRows_ (mAuthors model) authorSet $ \_ authorRel -> do
            atomicModifyIORef_ (arUsers authorRel) $ Set.delete userRef
        atomicModifyIORef_ (mUsers model) $ Map.delete userRef

modelListUsers :: Model -> IO [(Reference User, User)]
modelListUsers model = do
    listTableWith (mUsers model) $ \userRef userRel -> do
        user <- readIORef (urUser userRel)
        return (userRef, user)

modelSetAccessKey :: Model -> Reference User -> AccessKey -> IO ()
modelSetAccessKey model userRef akey = do
    withTableRow_ (mUsers model) userRef $ \userRel -> do
        atomicModifyIORef_ (urAccessKeys userRel) $ Set.insert akey

modelDeleteAccessKey :: Model -> Reference User -> AccessKey -> IO ()
modelDeleteAccessKey model userRef akey = do
    withTableRow_ (mUsers model) userRef $ \userRel -> do
        atomicModifyIORef_ (urAccessKeys userRel) $ Set.delete akey

modelListAccessKeys :: Model -> IO [(Reference User, [AccessKey])]
modelListAccessKeys model = do
    listTableWith (mUsers model) $ \userRef userRel -> do
        keys <- readIORef (urAccessKeys userRel)
        return (userRef, Set.toAscList keys)

modelSetAuthor :: Model -> Reference Author -> Author -> IO ()
modelSetAuthor model authorRef author = do
    setTableRow (mAuthors model) authorRef
        (AuthorRel
            <$> newIORef author
            <*> newIORef Set.empty
            <*> newIORef Set.empty)
        (\authorRel -> do
            atomicWriteIORef (arAuthor authorRel) $! author)

modelDeleteAuthor :: Model -> Reference Author -> IO ()
modelDeleteAuthor model authorRef = do
    withTableRow_ (mAuthors model) authorRef $ \authorRel -> do
        userSet <- readIORef (arUsers authorRel)
        forTableRows_ (mUsers model) userSet $ \_ userRel -> do
            atomicModifyIORef_ (urAuthors userRel) $ Set.delete authorRef
        articleSet <- readIORef (arArticles authorRel)
        forTableRowsFilter_ (mArticles model) articleSet $ \_ articleRel doKeep doDelete -> do
            article <- readIORef (rrArticle articleRel)
            case articlePublicationStatus article of
                NonPublished -> doDelete
                PublishAt _ -> do
                    atomicModifyIORef_ (rrArticle articleRel) $ \a -> a {articleAuthor = Reference ""}
                    doKeep
        atomicModifyIORef_ (mAuthors model) $ Map.delete authorRef

modelListAuthors :: Model -> IO [(Reference Author, Author)]
modelListAuthors model = do
    listTableWith (mAuthors model) $ \authorRef authorRel -> do
        author <- readIORef (arAuthor authorRel)
        return (authorRef, author)

modelConnectUserAuthor :: Model -> Reference User -> Reference Author -> IO ()
modelConnectUserAuthor model userRef authorRef = do
    withTableRow_ (mUsers model) userRef $ \userRel -> do
        atomicModifyIORef_ (urAuthors userRel) $ Set.insert authorRef
    withTableRow_ (mAuthors model) authorRef $ \authorRel -> do
        atomicModifyIORef_ (arUsers authorRel) $ Set.insert userRef

modelDisconnectUserAuthor :: Model -> Reference User -> Reference Author -> IO ()
modelDisconnectUserAuthor model userRef authorRef = do
    withTableRow_ (mUsers model) userRef $ \userRel -> do
        atomicModifyIORef_ (urAuthors userRel) $ Set.delete authorRef
    withTableRow_ (mAuthors model) authorRef $ \authorRel -> do
        atomicModifyIORef_ (arUsers authorRel) $ Set.delete userRef

modelListAuthorsOfUser :: Model -> Reference User -> IO [(Reference Author, Author)]
modelListAuthorsOfUser model userRef = do
    mrefList <- withTableRow (mUsers model) userRef $ \userRel -> do
        Set.toAscList <$> readIORef (urAuthors userRel)
    case mrefList of
        Nothing -> return []
        Just refList -> do
            fmap catMaybes $ forTableRows (mAuthors model) refList $ \authorRef authorRel -> do
                author <- readIORef (arAuthor authorRel)
                return (authorRef, author)

modelListUsersOfAuthor :: Model -> Reference Author -> IO [(Reference User, User)]
modelListUsersOfAuthor model authorRef = do
    mrefList <- withTableRow (mAuthors model) authorRef $ \authorRel -> do
        Set.toAscList <$> readIORef (arUsers authorRel)
    case mrefList of
        Nothing -> return []
        Just refList -> do
            fmap catMaybes $ forTableRows (mUsers model) refList $ \userRef userRel -> do
                user <- readIORef (urUser userRel)
                return (userRef, user)

modelSetArticle :: Model -> Reference Article -> Article -> Version Article -> IO ()
modelSetArticle model articleRef article articleVersion = do
    let newAuthor = articleAuthor article
    setTableRow (mArticles model) articleRef
        (do
            withTableRow_ (mAuthors model) newAuthor $ \authorRel -> do
                atomicModifyIORef_ (arArticles authorRel) $ Set.insert articleRef
            ArticleRel
                <$> newIORef article
                <*> newIORef articleVersion)
        (\articleRel -> do
            oldAuthor <- atomicModifyIORef' (rrArticle articleRel) $ \olda -> do
                (article, articleAuthor olda)
            when (oldAuthor /= newAuthor) $ do
                withTableRow_ (mAuthors model) oldAuthor $ \authorRel -> do
                    atomicModifyIORef_ (arArticles authorRel) $ Set.delete articleRef
                withTableRow_ (mAuthors model) newAuthor $ \authorRel -> do
                    atomicModifyIORef_ (arArticles authorRel) $ Set.insert articleRef
            atomicWriteIORef (rrVersion articleRel) $! articleVersion)

modelDeleteArticle :: Model -> Reference Article -> IO ()
modelDeleteArticle model articleRef = do
    withTableRow_ (mArticles model) articleRef $ \articleRel -> do
        article <- readIORef (rrArticle articleRel)
        withTableRow_ (mAuthors model) (articleAuthor article) $ \authorRel -> do
            atomicModifyIORef_ (arArticles authorRel) $ Set.delete articleRef
        atomicModifyIORef_ (mArticles model) $ Map.delete articleRef

modelListArticleVersions :: Model -> IO [(Reference Article, Article, Version Article)]
modelListArticleVersions model = do
    listTableWith (mArticles model) $ \articleRef articleRel -> do
        article <- readIORef (rrArticle articleRel)
        articleVersion <- readIORef (rrVersion articleRel)
        return (articleRef, article, articleVersion)

modelListArticles :: Model -> IO [(Reference Article, Article)]
modelListArticles model = do
    fmap sortArticleList $ listTableWith (mArticles model) $ \articleRef articleRel -> do
        article <- readIORef (rrArticle articleRel)
        return (articleRef, article)

modelListArticlesOfAuthor :: Model -> Reference Author -> IO [(Reference Article, Article)]
modelListArticlesOfAuthor model authorRef = do
    fmap (maybe [] sortArticleList) $ withTableRow (mAuthors model) authorRef $ \authorRel -> do
        articleSet <- readIORef (arArticles authorRel)
        fmap catMaybes $ forTableRows (mArticles model) (Set.toList articleSet) $ \articleRef articleRel -> do
            article <- readIORef (rrArticle articleRel)
            return (articleRef, article)

modelListArticlesOfUser :: Model -> Reference User -> IO [(Reference Article, Article)]
modelListArticlesOfUser model userRef = do
    fmap (maybe [] sortArticleList) $ withTableRow (mUsers model) userRef $ \userRel -> do
        authorSet <- readIORef (urAuthors userRel)
        fmap (concat . catMaybes) $ forTableRows (mAuthors model) (Set.toList authorSet) $ \authorRef _ -> do
            modelListArticlesOfAuthor model authorRef

sortArticleList :: [(Reference Article, Article)] -> [(Reference Article, Article)]
sortArticleList = sortOn (\(ref, article) -> (articlePublicationStatus article, ref))

verifyUsers :: HasCallStack => Model -> Handle -> IO ()
verifyUsers model storage = do
    storageUsers <- getAllStorageObjects $ listUsers storage
    modelUsers <- modelListUsers model
    storageUsers `shouldBe` modelUsers

verifyAccessKeys :: HasCallStack => Model -> Handle -> IO ()
verifyAccessKeys model storage = do
    modelKeys <- modelListAccessKeys model
    forM_ modelKeys $ \(userRef1, akeys) -> do
        forM_ akeys $ \akey -> do
            userRef2 <- assertOk =<< lookupAccessKey storage akey
            userRef2 `shouldBe` userRef1
        akeyRefs <- getAllStorageObjects $ listAccessKeysOf storage userRef1
        akeyRefs `shouldBe` map accessKeyId akeys

verifyAuthors :: HasCallStack => Model -> Handle -> IO ()
verifyAuthors model storage = do
    storageAuthors <- getAllStorageObjects $ listAuthors storage
    modelAuthors <- modelListAuthors model
    storageAuthors `shouldBe` modelAuthors

verifyUserAuthorConns :: HasCallStack => Model -> Handle -> IO ()
verifyUserAuthorConns model storage = do
    users <- Map.keys <$> readIORef (mUsers model)
    authors <- Map.keys <$> readIORef (mAuthors model)
    forM_ users $ \userRef -> do
        storageAuthors <- getAllStorageObjects $ listAuthorsOfUser storage userRef
        modelAuthors <- modelListAuthorsOfUser model userRef
        storageAuthors `shouldBe` modelAuthors
    forM_ authors $ \authorRef -> do
        storageUsers <- getAllStorageObjects $ listUsersOfAuthor storage authorRef
        modelUsers <- modelListUsersOfAuthor model authorRef
        storageUsers `shouldBe` modelUsers

verifyArticles :: HasCallStack => Model -> Handle -> PublicationStatus -> IO ()
verifyArticles model storage minPubStatus = do
    modelArticles <- modelListArticles model
    storageArticlesAll <- getAllStorageObjects $ listArticles storage False
    storageArticlesAll `shouldBe` modelArticles
    storageArticlesPub <- getAllStorageObjects $ listArticles storage True
    storageArticlesPub `shouldBe` filterPub modelArticles
    listTableWith_ (mArticles model) $ \articleRef articleRel -> do
        modelArticle <- readIORef (rrArticle articleRel)
        modelArticleVersion <- readIORef (rrVersion articleRel)
        (storageArticle, storageArticleVersion) <- assertOk =<< getArticle storage articleRef
        storageArticle `shouldBe` modelArticle
        storageArticleVersion `shouldBe` modelArticleVersion
    listTableWith_ (mAuthors model) $ \authorRef _ -> do
        modelArticles <- modelListArticlesOfAuthor model authorRef
        storageArticlesAll <- getAllStorageObjects $ listArticlesOfAuthor storage False authorRef
        storageArticlesAll `shouldBe` modelArticles
        storageArticlesPub <- getAllStorageObjects $ listArticlesOfAuthor storage True authorRef
        storageArticlesPub `shouldBe` filterPub modelArticles
    listTableWith_ (mUsers model) $ \userRef _ -> do
        modelArticles <- modelListArticlesOfUser model userRef
        storageArticlesAll <- getAllStorageObjects $ listArticlesOfUser storage False userRef
        storageArticlesAll `shouldBe` modelArticles
        storageArticlesPub <- getAllStorageObjects $ listArticlesOfUser storage True userRef
        storageArticlesPub `shouldBe` filterPub modelArticles
  where
    filterPub = dropWhile (\(_, a) -> articlePublicationStatus a < minPubStatus)

getAllStorageObjects :: (Show a, HasCallStack) => (Int64 -> Int64 -> IO (Result [a])) -> IO [a]
getAllStorageObjects func = do
    collect 0
  where
    collect i = do
        page <- assertOk =<< func i 10
        case page of
            [] -> return []
            rs -> fmap (rs ++) $ collect (i + 10)

atomicModifyTableCell
    :: Ord k
    => Map.Map k t
    -> k
    -> (t -> IORef a)
    -> (a -> a)
    -> IO ()
atomicModifyTableCell table ref setcol func = do
    case Map.lookup ref table of
        Nothing -> return ()
        Just row -> do
            atomicModifyIORef_ (setcol row) func

setTableRow :: Ord k => IORef (Map.Map k t) -> k -> IO t -> (t -> IO ()) -> IO ()
setTableRow ttable ref efunc rfunc = do
    table <- readIORef ttable
    case Map.lookup ref table of
        Nothing -> do
            row <- efunc
            atomicModifyIORef_ ttable $ Map.insert ref row
        Just row -> rfunc row

withTableRow_ :: Ord k => IORef (Map.Map k t) -> k -> (t -> IO ()) -> IO ()
withTableRow_ ttable ref func = do
    table <- readIORef ttable
    case Map.lookup ref table of
        Nothing -> return ()
        Just row -> func row

withTableRow :: Ord k => IORef (Map.Map k t) -> k -> (t -> IO r) -> IO (Maybe r)
withTableRow ttable ref func = do
    table <- readIORef ttable
    case Map.lookup ref table of
        Nothing -> return Nothing
        Just row -> Just <$> func row

forTableRows_ :: (Ord k, Foldable f) => IORef (Map.Map k t) -> f k -> (k -> t -> IO ()) -> IO ()
forTableRows_ ttable refList func = do
    table <- readIORef ttable
    forM_ refList $ \ref -> do
        case Map.lookup ref table of
            Nothing -> return ()
            Just row -> func ref row

forTableRows :: (Ord k, Traversable f) => IORef (Map.Map k t) -> f k -> (k -> t -> IO r) -> IO (f (Maybe r))
forTableRows ttable refList func = do
    table <- readIORef ttable
    forM refList $ \ref -> do
        case Map.lookup ref table of
            Nothing -> return Nothing
            Just row -> Just <$> func ref row

forTableRowsFilter_ :: (Ord k, Foldable f) => IORef (Map.Map k t) -> f k -> (forall q. k -> t -> IO q -> IO q -> IO q) -> IO ()
forTableRowsFilter_ ttable refList func = do
    table <- readIORef ttable
    forM_ refList $ \ref -> do
        case Map.lookup ref table of
            Nothing -> return ()
            Just row -> func ref row
                (return ())
                (atomicModifyIORef_ ttable $ Map.delete ref)

listTableWith :: (Ord k) => IORef (Map.Map k t) -> (k -> t -> IO a) -> IO [a]
listTableWith ttable func = do
    table <- readIORef ttable
    forM (Map.toAscList table) $ \(ref, row) -> do
        func ref row

listTableWith_ :: (Ord k) => IORef (Map.Map k t) -> (k -> t -> IO ()) -> IO ()
listTableWith_ ttable func = do
    table <- readIORef ttable
    forM_ (Map.toList table) $ \(ref, row) -> do
        func ref row

atomicModifyIORef_ :: IORef a -> (a -> a) -> IO ()
atomicModifyIORef_ ioref f = atomicModifyIORef' ioref $ \x -> (f x, ())
