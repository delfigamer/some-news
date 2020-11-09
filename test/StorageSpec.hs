module StorageSpec
    ( spec
    ) where

import Control.Concurrent
import Control.Concurrent.MVar
import Control.Concurrent.QSemN
import Control.Exception
import Control.Monad
import Data.Aeson hiding (Result)
import Data.IORef
import Data.Int
import Data.List
import Data.Maybe
import Data.Ord
import Data.Time.Clock
import Data.Tuple
import Data.Yaml
import System.IO.Unsafe
import Test.Hspec
import qualified Data.ByteString as BS
import qualified Data.Char as Char
import qualified Data.Map.Strict as Map
import qualified Data.Map.Lazy as MapLazy
import qualified Data.Set as Set
import qualified Data.Text as Text
import qualified System.IO as IO
import Gen
import Sql.Query
import Storage
import Tuple
import qualified Bimap
import qualified Logger
import qualified Sql.Database as Db
import qualified Sql.Database.Config as Db
import qualified Tree

data TestConfig = TestConfig
    { databaseConfig :: Db.Config
    , confSampleSize :: Int
    }

instance FromJSON TestConfig where
    parseJSON = withObject "TestConfig" $ \v -> do
        TestConfig
            <$> v .: "database"
            <*> v .: "sample-size"

randomText :: Gen Text.Text
randomText = fmap Text.pack $ randomPrintableString =<< randomWithin 4 32

randomByteString :: Int -> Gen BS.ByteString
randomByteString n = BS.pack <$> replicateM n random

timeGenBase :: UTCTime
timeGenBase = case unsafePerformIO getCurrentTime of
    UTCTime day _ -> UTCTime day 0
{-# NOINLINE timeGenBase #-}

randomTime :: Gen UTCTime
randomTime = do
    seconds <- randomWithin (-86400*1000) (-86400)
    epoch <- randomWithin 0 1
    let delta = fromInteger seconds + nominalDay * (fromInteger (epoch * 365 * 10))
    return $ addUTCTime delta timeGenBase

randomReference :: Gen (Reference a)
randomReference = Reference <$> randomByteString 16

randomVersion :: Gen (Version a)
randomVersion = Version <$> randomByteString 16

inParallel :: (a -> b -> r) -> IO a -> IO b -> IO r
inParallel combine aleft aright = mask $ \restore -> do
    mvar <- newEmptyMVar
    tid <- forkIOWithUnmask $ \unmask -> do
        r <- trySome $ unmask $ yield >> aright
        putMVar mvar r
    lret <- trySome $ restore aleft
    case lret of
        Left ex -> do
            _ <- killThread tid
            _ <- takeMVar mvar
            throwIO ex
        Right x -> do
            rret <- takeMVar mvar
            case rret of
                Right y -> return $ combine x y
                Left ex -> throwIO ex
  where
    trySome :: IO a -> IO (Either SomeException a)
    trySome = try

parallelTraverse :: [IO a] -> IO [a]
parallelTraverse [] = return []
parallelTraverse (t : ts) = inParallel (:) t $ parallelTraverse ts

parallelTraverse_ :: [IO ()] -> IO ()
parallelTraverse_ [] = return ()
parallelTraverse_ (t : ts) = inParallel (\_ _ -> ()) t $ parallelTraverse_ ts

parallelFor :: [a] -> (a -> IO b) -> IO [b]
parallelFor xs f = parallelTraverse (map f xs)

parallelFor_ :: [a] -> (a -> IO ()) -> IO ()
parallelFor_ xs f = parallelTraverse_ (map f xs)

splitSetOn :: (a -> Ordering) -> Set.Set a -> (Set.Set a, Set.Set a, Set.Set a)
splitSetOn func init = do
    let (lt, ge) = Set.spanAntitone ((== GT) . func) init
    let (eq, gt) = Set.spanAntitone ((== EQ) . func) ge
    (lt, eq, gt)

atomicModifyIORef_ :: IORef a -> (a -> a) -> IO ()
atomicModifyIORef_ ioref f = atomicModifyIORef' ioref $ \x -> (f x, ())

byChance :: Int -> Int -> IO a -> IO a -> IO a
byChance num den ma mb = do
    i <- generate $ randomWithin 1 den
    if i <= num
        then ma
        else mb

repeatSome_ :: Int -> Int -> IO a -> IO ()
repeatSome_ lo hi act = do
    n <- generate $ randomWithin lo hi
    replicateM_ n act

clearDatabase :: Db.Handle -> IO ()
clearDatabase db = do
    mapM_ (Db.makeQuery db . DropTable) $ reverse $
        [ "sn_metadata"
        , "sn_users"
        , "sn_access_keys"
        , "sn_authors"
        , "sn_author2user"
        , "sn_categories"
        , "sn_articles"
        , "sn_tags"
        , "sn_article2tag"
        , "sn_comments"
        , "sn_files"
        , "sn_file_chunks"
        ]

assertRight :: (Show a, Show b, HasCallStack) => Either a b -> IO b
assertRight (Right x) = return x
assertRight r = expectationFailure ("Expected a Right, got " ++ show r) >> undefined

spec :: Spec
spec = do
    beforeAll (decodeFileThrow "test-config.yaml" :: IO [TestConfig]) $ do
        describe "Storage" $ do
            clearStorage "handles user objects" $ \sampleSize storage -> do
                userTable <- generateUsers storage sampleSize
                minceUsers storage sampleSize userTable
            clearStorage "handles access key objects" $ \sampleSize storage -> do
                userTable <- generateUsers storage sampleSize
                accessKeyTable <- generateAccessKeys storage sampleSize userTable
                minceAccessKeys storage sampleSize userTable accessKeyTable
            clearStorage "handles author objects" $ \sampleSize storage -> do
                authorTable <- generateAuthors storage sampleSize
                minceAuthors storage sampleSize authorTable
            clearStorage "handles user-author relations" $ \sampleSize storage -> do
                userTable <- generateUsers storage sampleSize
                authorTable <- generateAuthors storage sampleSize
                userAuthorRels <- generateUserAuthorRels storage sampleSize userTable authorTable
                minceUserAuthorRels storage sampleSize userTable authorTable userAuthorRels
            clearStorage "handles category objects" $ \sampleSize storage -> do
                categoryTable <- generateCategories storage sampleSize
                minceCategories storage sampleSize categoryTable
            clearStorage "handles article objects" $ \sampleSize storage -> do
                userTable <- generateUsers storage sampleSize
                authorTable <- generateAuthors storage sampleSize
                userAuthorRels <- generateUserAuthorRels storage sampleSize userTable authorTable
                categoryTable <- generateCategories storage sampleSize
                articleTable <- generateArticles storage sampleSize userTable authorTable userAuthorRels categoryTable
                minceArticles storage sampleSize userTable authorTable userAuthorRels categoryTable articleTable
            clearStorage "handles tag objects" $ \sampleSize storage -> do
                tagTable <- generateTags storage sampleSize
                minceTags storage sampleSize tagTable
            clearStorage "handles article-tag relations" $ \sampleSize storage -> do
                userTable <- generateUsers storage sampleSize
                authorTable <- generateAuthors storage sampleSize
                userAuthorRels <- generateUserAuthorRels storage sampleSize userTable authorTable
                categoryTable <- generateCategories storage sampleSize
                articleTable <- generateArticles storage sampleSize userTable authorTable userAuthorRels categoryTable
                tagTable <- generateTags storage sampleSize
                articleTagRels <- generateArticleTagRels storage sampleSize articleTable tagTable
                minceArticleTagRels storage sampleSize articleTable tagTable articleTagRels
            clearStorage "handles comment objects" $ \sampleSize storage -> do
                userTable <- generateUsers storage sampleSize
                authorTable <- generateAuthors storage sampleSize
                userAuthorRels <- generateUserAuthorRels storage sampleSize userTable authorTable
                categoryTable <- generateCategories storage sampleSize
                articleTable <- generateArticles storage sampleSize userTable authorTable userAuthorRels categoryTable
                tagTable <- generateTags storage sampleSize
                commentTable <- generateComments storage sampleSize userTable articleTable
                minceComments storage sampleSize userTable articleTable commentTable

clearStorage :: String -> (Int -> Handle -> IO ()) -> SpecWith [TestConfig]
clearStorage name body = do
    it name $ \testConfList -> do
        forM_ testConfList $ \testConf -> do
            let logfile = ".test-" ++ name ++ ".html"
            Logger.withHtmlLogger logfile $ \logger -> do
                Db.withDatabase (databaseConfig testConf) logger $ \db -> do
                    clearDatabase db
                    upgradeSchema logger db `shouldReturn` Right ()
                    withSqlStorage logger db (expectationFailure . show) $ \storage -> do
                        body (confSampleSize testConf) storage

generateUsers
    :: Handle -> Int
    -> IO (IORef (Map.Map (Reference User) User))
generateUsers storage sampleSize = do
    userTable <- newIORef Map.empty
    parallelFor_ [1 .. sampleSize] $ \_ -> do
        name <- generate $ randomText
        surname <- generate $ randomText
        user <- assertRight =<< perform storage (UserCreate name surname)
        userName user `shouldBe` name
        userSurname user `shouldBe` surname
        userIsAdmin user `shouldBe` False
        isAdmin <- generate $ randomBool
        when isAdmin $
            assertRight =<< perform storage (UserSetIsAdmin (userId user) True)
        atomicModifyIORef_ userTable $ Map.insert (userId user) $
            user {userIsAdmin = isAdmin}
    return userTable

validateUsers
    :: Handle
    -> IORef (Map.Map (Reference User) User)
    -> IO ()
validateUsers storage userTable = do
    userList <- Map.elems <$> readIORef userTable
    perform storage (UserList (ListView 0 maxBound [] []))
        `shouldReturn`
        Right userList

minceUsers
    :: Handle -> Int
    -> IORef (Map.Map (Reference User) User)
    -> IO ()
minceUsers storage sampleSize userTable = do
    validateUsers storage userTable
    do
        userList <- Map.elems <$> readIORef userTable
        toAlter <- generate $ chooseFrom (sampleSize `div` 5) userList
        parallelFor_ toAlter $ \user -> do
            name <- generate $ randomText
            surname <- generate $ randomText
            assertRight =<< perform storage (UserSetName (userId user) name surname)
            atomicModifyIORef_ userTable $ Map.insert (userId user) $
                user {userName = name, userSurname = surname}
        validateUsers storage userTable
    do
        userList <- Map.elems <$> readIORef userTable
        toAlter <- generate $ chooseFrom (sampleSize `div` 5) userList
        parallelFor_ toAlter $ \user -> do
            isAdmin <- generate $ randomBool
            assertRight =<< perform storage (UserSetIsAdmin (userId user) isAdmin)
            atomicModifyIORef_ userTable $ Map.insert (userId user) $
                user {userIsAdmin = isAdmin}
        validateUsers storage userTable
    do
        userList <- Map.elems <$> readIORef userTable
        toDelete <- generate $ chooseFrom (sampleSize `div` 5) userList
        parallelFor_ toDelete $ \user -> do
            assertRight =<< perform storage (UserDelete (userId user))
            atomicModifyIORef_ userTable $ Map.delete (userId user)
        validateUsers storage userTable
    do
        userMap <- readIORef userTable
        badRefs <- generate $
            replicateM 20 $
                randomReference `suchThat` (\ref -> not (Map.member ref userMap))
        parallelFor_ badRefs $ \ref -> do
            perform storage (UserSetName ref "foo" "bar") `shouldReturn` Left NotFoundError
            perform storage (UserSetIsAdmin ref True) `shouldReturn` Left NotFoundError
            perform storage (UserDelete ref) `shouldReturn` Left NotFoundError
            perform storage (UserList (ListView 0 1 [FilterUserId ref] []))
                `shouldReturn`
                Right []
    do
        userList <- Map.elems <$> readIORef userTable
        parallelFor_ userList $ \user -> do
            perform storage (UserList (ListView 0 1 [FilterUserId (userId user)] []))
                `shouldReturn`
                Right [user]
        perform storage (UserList (ListView 0 maxBound [FilterUserIsAdmin True] []))
            `shouldReturn`
            Right (filter userIsAdmin userList)
        perform storage (UserList (ListView 0 maxBound [FilterUserIsAdmin False] []))
            `shouldReturn`
            Right (filter (not . userIsAdmin) userList)
        perform storage (UserList (ListView 0 maxBound [] [(OrderUserName, Ascending)]))
            `shouldReturn`
            Right (sortOn userName userList)
        perform storage (UserList (ListView 0 maxBound [] [(OrderUserName, Descending)]))
            `shouldReturn`
            Right (sortOn (Down . userName) userList)
        perform storage (UserList (ListView 0 maxBound [] [(OrderUserSurname, Ascending)]))
            `shouldReturn`
            Right (sortOn userSurname userList)
        perform storage (UserList (ListView 0 maxBound [] [(OrderUserJoinDate, Ascending)]))
            `shouldReturn`
            Right (sortOn (Down . userJoinDate) userList)
        perform storage (UserList (ListView 0 maxBound [] [(OrderUserIsAdmin, Ascending), (OrderUserName, Descending)]))
            `shouldReturn`
            Right (sortOn (\u -> (Down (userIsAdmin u), Down (userName u))) userList)
    return ()

generateAccessKeys
    :: Handle -> Int
    -> IORef (Map.Map (Reference User) User)
    -> IO (IORef (Map.Map (Reference User) (Set.Set AccessKey)))
generateAccessKeys storage sampleSize userTable = do
    userRefs <- Map.keys <$> readIORef userTable
    akeyTable <- newIORef $ Map.fromList $ map (\uref -> (uref, Set.empty)) userRefs
    parallelFor_ userRefs $ \userRef -> do
        repeatSome_ 1 5 $ do
            akey <- assertRight =<< perform storage (AccessKeyCreate userRef)
            atomicModifyIORef_ akeyTable $ Map.update (Just . Set.insert akey) userRef
    validateAccessKeys storage akeyTable
    return akeyTable

minceAccessKeys
    :: Handle -> Int
    -> IORef (Map.Map (Reference User) User)
    -> IORef (Map.Map (Reference User) (Set.Set AccessKey))
    -> IO ()
minceAccessKeys storage sampleSize userTable akeyTable = do
    userRefs <- Map.keys <$> readIORef userTable
    do
        userKeys <- readIORef akeyTable
        let allKeys = do
                (userRef, akeys) <- Map.toList userKeys
                key <- Set.elems akeys
                [(userRef, key)]
        toDelete <- generate $ chooseFrom (sampleSize `div` 5) allKeys
        parallelFor_ toDelete $ \(userRef, akey) -> do
            assertRight =<< perform storage (AccessKeyDelete userRef (accessKeyId akey))
            atomicModifyIORef_ akeyTable $ Map.update (Just . Set.delete akey) userRef
        validateAccessKeys storage akeyTable
    do
        userKeys <- readIORef akeyTable
        let oneKeys = do
                (userRef, akeys) <- Map.toList userKeys
                oneKey : _ <- return $ Set.elems akeys
                [(userRef, oneKey)]
        let (users, keys) = unzip oneKeys
        let badKeysFull = zip users (drop 1 keys)
        badKeys <- generate $ chooseFrom (sampleSize `div` 5) badKeysFull
        parallelFor_ badKeys $ \(userRef, akey) -> do
            perform storage (AccessKeyDelete userRef (accessKeyId akey))
                `shouldReturn`
                Left NotFoundError
        validateAccessKeys storage akeyTable
    return ()

validateAccessKeys
    :: Handle
    -> IORef (Map.Map (Reference User) (Set.Set AccessKey))
    -> IO ()
validateAccessKeys storage akeyTable = do
    akeys <- Map.toList <$> readIORef akeyTable
    parallelFor_ akeys $ \(userRef, akeySet) -> do
        perform storage (AccessKeyList userRef (ListView 0 maxBound [] []))
            `shouldReturn`
            Right (map accessKeyId $ Set.toAscList akeySet)
        parallelFor_ (Set.elems akeySet) $ \akey -> do
            perform storage (AccessKeyLookup akey)
                `shouldReturn`
                Right userRef

generateAuthors
    :: Handle -> Int
    -> IO (IORef (Map.Map (Reference Author) Author))
generateAuthors storage sampleSize = do
    authorTable <- newIORef Map.empty
    parallelFor_ [1 .. sampleSize] $ \_ -> do
        name <- generate $ randomText
        description <- generate $ randomText
        author <- assertRight =<< perform storage (AuthorCreate name description)
        authorName author `shouldBe` name
        authorDescription author `shouldBe` description
        atomicModifyIORef_ authorTable $ Map.insert (authorId author) author
    validateAuthors storage authorTable
    return authorTable

minceAuthors
    :: Handle -> Int
    -> IORef (Map.Map (Reference Author) Author)
    -> IO ()
minceAuthors storage sampleSize authorTable = do
    do
        authorList <- Map.elems <$> readIORef authorTable
        toAlter <- generate $ chooseFrom (sampleSize `div` 5) authorList
        parallelFor_ toAlter $ \author -> do
            name <- generate $ randomText
            assertRight =<< perform storage (AuthorSetName (authorId author) name)
            atomicModifyIORef_ authorTable $ Map.insert (authorId author) $
                author {authorName = name}
        validateAuthors storage authorTable
    do
        authorList <- Map.elems <$> readIORef authorTable
        toAlter <- generate $ chooseFrom (sampleSize `div` 5) authorList
        parallelFor_ toAlter $ \author -> do
            description <- generate $ randomText
            assertRight =<< perform storage (AuthorSetDescription (authorId author) description)
            atomicModifyIORef_ authorTable $ Map.insert (authorId author) $
                author {authorDescription = description}
        validateAuthors storage authorTable
    do
        authorList <- Map.elems <$> readIORef authorTable
        toDelete <- generate $ chooseFrom (sampleSize `div` 5) authorList
        parallelFor_ toDelete $ \author -> do
            assertRight =<< perform storage (AuthorDelete (authorId author))
            atomicModifyIORef_ authorTable $ Map.delete (authorId author)
        validateAuthors storage authorTable
    do
        authorMap <- readIORef authorTable
        badRefs <- generate $
            replicateM 20 $
                randomReference `suchThat` (\ref -> not (Map.member ref authorMap))
        parallelFor_ badRefs $ \ref -> do
            perform storage (AuthorSetName ref "foo") `shouldReturn` Left NotFoundError
            perform storage (AuthorSetDescription ref "bar") `shouldReturn` Left NotFoundError
            perform storage (AuthorDelete ref) `shouldReturn` Left NotFoundError
            perform storage (AuthorList (ListView 0 1 [FilterAuthorId ref] []))
                `shouldReturn`
                Right []
    do
        authorList <- Map.elems <$> readIORef authorTable
        parallelFor_ authorList $ \author -> do
            perform storage (AuthorList (ListView 0 1 [FilterAuthorId (authorId author)] []))
                `shouldReturn`
                Right [author]
        perform storage (AuthorList (ListView 0 maxBound [] [(OrderAuthorName, Ascending)]))
            `shouldReturn`
            Right (sortOn authorName authorList)
    return ()

validateAuthors
    :: Handle
    -> IORef (Map.Map (Reference Author) Author)
    -> IO ()
validateAuthors storage authorTable = do
    authorList <- Map.elems <$> readIORef authorTable
    perform storage (AuthorList (ListView 0 maxBound [] []))
        `shouldReturn`
        Right authorList

generateUserAuthorRels
    :: Handle -> Int
    -> IORef (Map.Map (Reference User) User)
    -> IORef (Map.Map (Reference Author) Author)
    -> IO (IORef (Bimap.Bimap (Reference User) (Reference Author)))
generateUserAuthorRels storage sampleSize userTable authorTable = do
    userRefs <- Map.keys <$> readIORef userTable
    authorRefs <- Map.keys <$> readIORef authorTable
    connSet <- newIORef $ Bimap.fromKeys userRefs authorRefs
    parallelFor_ authorRefs $ \authorRef -> do
        parallelFor_ userRefs $ \userRef -> do
            byChance 1 3
                (do
                    assertRight =<< perform storage (AuthorSetOwnership authorRef userRef True)
                    atomicModifyIORef_ connSet $ Bimap.insert (userRef, authorRef))
                (return ())
    validateUserAuthorRels storage userTable authorTable connSet
    return connSet

minceUserAuthorRels
    :: Handle -> Int
    -> IORef (Map.Map (Reference User) User)
    -> IORef (Map.Map (Reference Author) Author)
    -> IORef (Bimap.Bimap (Reference User) (Reference Author))
    -> IO ()
minceUserAuthorRels storage sampleSize userTable authorTable connSet = do
    userRefs <- Map.keys <$> readIORef userTable
    authorRefs <- Map.keys <$> readIORef authorTable
    do
        parallelFor_ authorRefs $ \authorRef -> do
            parallelFor_ userRefs $ \userRef -> do
                conn <- generate $ randomBool
                assertRight =<< perform storage (AuthorSetOwnership authorRef userRef conn)
                atomicModifyIORef_ connSet $ if conn
                    then Bimap.insert (userRef, authorRef)
                    else Bimap.delete (userRef, authorRef)
        validateUserAuthorRels storage userTable authorTable connSet
    do
        usersToDelete <- generate $ chooseFrom (sampleSize `div` 5) userRefs
        parallelFor_ usersToDelete $ \userRef -> do
            assertRight =<< perform storage (UserDelete userRef)
            atomicModifyIORef_ userTable $ Map.delete userRef
            atomicModifyIORef_ connSet $ Bimap.removeLeft userRef
        validateUserAuthorRels storage userTable authorTable connSet
    do
        authorsToDelete <- generate $ chooseFrom (sampleSize `div` 5) authorRefs
        parallelFor_ authorsToDelete $ \authorRef -> do
            assertRight =<< perform storage (AuthorDelete authorRef)
            atomicModifyIORef_ authorTable $ Map.delete authorRef
            atomicModifyIORef_ connSet $ Bimap.removeRight authorRef
        validateUserAuthorRels storage userTable authorTable connSet
    return ()

validateUserAuthorRels
    :: Handle
    -> IORef (Map.Map (Reference User) User)
    -> IORef (Map.Map (Reference Author) Author)
    -> IORef (Bimap.Bimap (Reference User) (Reference Author))
    -> IO ()
validateUserAuthorRels storage userTable authorTable connSet = do
    userMap <- readIORef userTable
    authorMap <- readIORef authorTable
    conns <- readIORef connSet
    parallelFor_ (Map.toList (Bimap.left conns)) $ \(userRef, authorSet) -> do
        perform storage (AuthorList (ListView 0 maxBound [FilterAuthorUserId userRef] []))
            `shouldReturn`
            Right (map (authorMap Map.!) $ Set.elems authorSet)
    parallelFor_ (Map.toList (Bimap.right conns)) $ \(authorRef, userSet) -> do
        perform storage (UserList (ListView 0 maxBound [FilterUserAuthorId authorRef] []))
            `shouldReturn`
            Right (map (userMap Map.!) $ Set.elems userSet)

generateCategories
    :: Handle -> Int
    -> IO (IORef (Tree.Tree (Reference Category) Text.Text))
generateCategories storage sampleSize = do
    categoryTable <- newIORef Tree.empty
    parallelFor_ [1 .. sampleSize] $ \_ -> do
        prevCats <- Tree.keys <$> readIORef categoryTable
        name <- generate $ randomText
        parent <- generate $ do
            x <- randomWithin 0 (length prevCats)
            y <- randomWithin 0 x
            case y of
                0 -> return $ Reference ""
                _ -> return $ prevCats !! (y - 1)
        category <- assertRight =<< perform storage (CategoryCreate name parent)
        categoryName category `shouldBe` name
        categoryParent category `shouldBe` parent
        atomicModifyIORef_ categoryTable $ Tree.include (categoryId category) name parent
    validateCategories storage categoryTable
    return categoryTable

minceCategories
    :: Handle -> Int
    -> IORef (Tree.Tree (Reference Category) Text.Text)
    -> IO ()
minceCategories storage sampleSize categoryTable = do
    do
        categoryList <- toCategoryList <$> readIORef categoryTable
        toAlter <- generate $ chooseFrom (sampleSize `div` 5) categoryList
        parallelFor_ toAlter $ \category -> do
            name <- generate $ randomText
            assertRight =<< perform storage (CategorySetName (categoryId category) name)
            atomicModifyIORef_ categoryTable $ Tree.adjust
                (\_ -> name)
                (categoryId category)
        validateCategories storage categoryTable
    do
        categoryTree <- readIORef categoryTable
        toAlter <- generate $ chooseFrom (sampleSize `div` 5) (toCategoryList categoryTree)
        let stableCategories = Tree.excludeSubtreeSet
                (Set.fromList $ map categoryId toAlter)
                categoryTree
        parallelFor_ toAlter $ \category -> do
            [parent] <- generate $ do
                b <- randomBool
                if b
                    then return [Reference ""]
                    else chooseFromSuch
                        (\cref -> categoryId category `notElem` Tree.ancestors cref stableCategories)
                        1 (Tree.keys stableCategories)
            assertRight =<< perform storage (CategorySetParent (categoryId category) parent)
            atomicModifyIORef_ categoryTable $ fromJust . Tree.trySetParent (categoryId category) parent
        validateCategories storage categoryTable
    do
        categoryList <- toCategoryList <$> readIORef categoryTable
        toDelete <- generate $ chooseFrom (sampleSize `div` 5) categoryList
        parallelFor_ toDelete $ \category -> do
            assertRight =<< perform storage (CategoryDelete (categoryId category))
            atomicModifyIORef_ categoryTable $ snd . Tree.exclude (categoryId category)
        validateCategories storage categoryTable
    do
        categoryTree <- readIORef categoryTable
        badRefs <- generate $
            replicateM 20 $
                randomReference `suchThat` (\ref -> not (Tree.member ref categoryTree))
        parallelFor_ badRefs $ \ref -> do
            perform storage (CategorySetName ref "foo") `shouldReturn` Left NotFoundError
            perform storage (CategorySetParent ref (Reference "")) `shouldReturn` Left NotFoundError
            perform storage (CategoryDelete ref) `shouldReturn` Left NotFoundError
            perform storage (CategoryList (ListView 0 1 [FilterCategoryId ref] []))
                `shouldReturn`
                Right []
    do
        categoryTree <- readIORef categoryTable
        let categoryList = toCategoryList categoryTree
        parallelFor_ categoryList $ \category -> do
            perform storage (CategoryList (ListView 0 1 [FilterCategoryId (categoryId category)] []))
                `shouldReturn`
                Right [category]
        perform storage (CategoryList (ListView 0 maxBound [] [(OrderCategoryName, Ascending)]))
            `shouldReturn`
            Right (sortOn categoryName categoryList)
        let strictSubcategories = Tree.childMap categoryTree
        parallelFor_ (Map.toList strictSubcategories) $ \(categoryRef, subcatSet) -> do
            perform storage (CategoryList (ListView 0 maxBound [FilterCategoryParentId categoryRef] []))
                `shouldReturn`
                Right (map (lookupCategory categoryTree) (Set.elems subcatSet))
        let transitiveSubcategories = Tree.descendantMap categoryTree
        parallelFor_ (Map.toList transitiveSubcategories) $ \(categoryRef, subcatSet) -> do
            let subcatList = map (lookupCategory categoryTree) (Set.elems subcatSet)
            perform storage (CategoryList (ListView 0 maxBound [FilterCategoryTransitiveParentId categoryRef] []))
                `shouldReturn`
                Right subcatList
            perform storage (CategoryList (ListView 0 maxBound [FilterCategoryTransitiveParentId categoryRef] [(OrderCategoryName, Ascending)]))
                `shouldReturn`
                Right (sortOn categoryName subcatList)
            someSubcats <- generate $ chooseFrom 2 $ Set.elems subcatSet
            parallelFor_ someSubcats $ \subcat -> do
                perform storage (CategorySetParent categoryRef subcat) `shouldReturn` Left InvalidRequestError
    return ()

validateCategories
    :: Handle
    -> IORef (Tree.Tree (Reference Category) Text.Text)
    -> IO ()
validateCategories storage categoryTable = do
    categoryList <- toCategoryList <$> readIORef categoryTable
    perform storage (CategoryList (ListView 0 maxBound [] []))
        `shouldReturn`
        Right categoryList

toCategoryList :: Tree.Tree (Reference Category) Text.Text -> [Category]
toCategoryList = Tree.foldr
    (\ref name parent rest -> Category ref name parent : rest)
    []

lookupCategory :: Tree.Tree (Reference Category) Text.Text -> Reference Category -> Category
lookupCategory tree ref = case Tree.lookup ref tree of
    Nothing -> error "invalid category id"
    Just (name, parent) -> Category ref name parent

generateArticles
    :: Handle -> Int
    -> IORef (Map.Map (Reference User) User)
    -> IORef (Map.Map (Reference Author) Author)
    -> IORef (Bimap.Bimap (Reference User) (Reference Author))
    -> IORef (Tree.Tree (Reference Category) Text.Text)
    -> IO (IORef (Map.Map (Reference Article) Article))
generateArticles storage sampleSize userTable authorTable userAuthorConnSet categoryTable = do
    authorRefs <- Map.keys <$> readIORef authorTable
    categoryRefs <- Tree.keys <$> readIORef categoryTable
    articleTable <- newIORef Map.empty
    parallelFor_ [1 .. sampleSize] $ \_ -> do
        [authorRef] <- generate $ chooseFrom 1 authorRefs
        article <- assertRight =<< perform storage (ArticleCreate authorRef)
        articleAuthor article `shouldBe` authorRef
        articleName article `shouldBe` ""
        articleText article `shouldBe` ""
        articlePublicationStatus article `shouldBe` NonPublished
        articleCategory article `shouldBe` Reference ""
        let version1 = articleVersion article
        name <- generate $ randomText
        version2 <- assertRight =<< perform storage (ArticleSetName (articleId article) version1 name)
        text <- generate $ randomText
        version3 <- assertRight =<< perform storage (ArticleSetText (articleId article) version2 text)
        category <- byChance 3 5
            (do
                [category] <- generate $ chooseFrom 1 categoryRefs
                assertRight =<< perform storage (ArticleSetCategory (articleId article) category)
                return category)
            (return $ Reference "")
        pubStatus <- byChance 3 5
            (do
                pubStatus <- generate $ PublishAt <$> randomTime
                assertRight =<< perform storage (ArticleSetPublicationStatus (articleId article) pubStatus)
                return pubStatus)
            (return NonPublished)
        atomicModifyIORef_ articleTable $ Map.insert (articleId article) $
            article
                { articleVersion = version3
                , articleName = name
                , articleText = text
                , articleCategory = category
                , articlePublicationStatus = pubStatus
                }
    validateArticles storage articleTable
    return articleTable

minceArticles
    :: Handle -> Int
    -> IORef (Map.Map (Reference User) User)
    -> IORef (Map.Map (Reference Author) Author)
    -> IORef (Bimap.Bimap (Reference User) (Reference Author))
    -> IORef (Tree.Tree (Reference Category) Text.Text)
    -> IORef (Map.Map (Reference Article) Article)
    -> IO ()
minceArticles storage sampleSize userTable authorTable userAuthorConnSet categoryTable articleTable = do
    authorRefs <- Map.keys <$> readIORef authorTable
    categoryRefs <- Tree.keys <$> readIORef categoryTable
    do
        articleList <- Map.elems <$> readIORef articleTable
        toAlter <- generate $ chooseFrom (sampleSize `div` 5) articleList
        parallelFor_ toAlter $ \article -> do
            [authorRef] <- generate $ chooseFrom 1 authorRefs
            assertRight =<< perform storage (ArticleSetAuthor (articleId article) authorRef)
            atomicModifyIORef_ articleTable $ Map.insert (articleId article) $
                article {articleAuthor = authorRef}
        validateArticles storage articleTable
    do
        articleList <- Map.elems <$> readIORef articleTable
        toAlter <- generate $ chooseFrom (sampleSize `div` 5) articleList
        parallelFor_ toAlter $ \article -> do
            name <- generate $ randomText
            version <- assertRight =<< perform storage
                (ArticleSetName (articleId article) (articleVersion article) name)
            atomicModifyIORef_ articleTable $ Map.insert (articleId article) $
                article {articleName = name, articleVersion = version}
        validateArticles storage articleTable
    do
        articleList <- Map.elems <$> readIORef articleTable
        toAlter <- generate $ chooseFrom (sampleSize `div` 5) articleList
        parallelFor_ toAlter $ \article -> do
            text <- generate $ randomText
            version <- assertRight =<< perform storage
                (ArticleSetText (articleId article) (articleVersion article) text)
            atomicModifyIORef_ articleTable $ Map.insert (articleId article) $
                article {articleText = text, articleVersion = version}
        validateArticles storage articleTable
    do
        articleList <- Map.elems <$> readIORef articleTable
        toAlter <- generate $ chooseFrom (sampleSize `div` 5) articleList
        parallelFor_ toAlter $ \article -> do
            pubStatus <- byChance 3 5
                (generate $ PublishAt <$> randomTime)
                (return NonPublished)
            assertRight =<< perform storage (ArticleSetPublicationStatus (articleId article) pubStatus)
            atomicModifyIORef_ articleTable $ Map.insert (articleId article) $
                article {articlePublicationStatus = pubStatus}
        validateArticles storage articleTable
    do
        articleList <- Map.elems <$> readIORef articleTable
        toAlter <- generate $ chooseFrom (sampleSize `div` 5) articleList
        parallelFor_ toAlter $ \article -> do
            category <- byChance 3 5
                (generate $ head <$> chooseFrom 1 categoryRefs)
                (return $ Reference "")
            assertRight =<< perform storage (ArticleSetCategory (articleId article) category)
            atomicModifyIORef_ articleTable $ Map.insert (articleId article) $
                article {articleCategory = category}
        validateArticles storage articleTable
    do
        articleList <- Map.elems <$> readIORef articleTable
        toDelete <- generate $ chooseFrom (sampleSize `div` 5) articleList
        parallelFor_ toDelete $ \article -> do
            assertRight =<< perform storage (ArticleDelete (articleId article))
            atomicModifyIORef_ articleTable $ Map.delete (articleId article)
        validateArticles storage articleTable
    do
        articleMap <- readIORef articleTable
        let articleList = Map.elems articleMap
        parallelFor_ articleList $ \article -> do
            badVersion <- generate $ randomVersion `suchThat` (/= articleVersion article)
            perform storage (ArticleSetName (articleId article) badVersion "foo") `shouldReturn` Left VersionError
            perform storage (ArticleSetText (articleId article) badVersion "bar") `shouldReturn` Left VersionError
        badRefs <- generate $
            replicateM 20 $
                randomReference `suchThat` (\ref -> not (Map.member ref articleMap))
        parallelFor_ badRefs $ \ref -> do
            perform storage (ArticleSetAuthor ref (Reference "")) `shouldReturn` Left NotFoundError
            perform storage (ArticleSetName ref (Version "") "foo") `shouldReturn` Left NotFoundError
            perform storage (ArticleSetText ref (Version "") "bar") `shouldReturn` Left NotFoundError
            perform storage (ArticleSetCategory ref (Reference "")) `shouldReturn` Left NotFoundError
            perform storage (ArticleSetPublicationStatus ref NonPublished) `shouldReturn` Left NotFoundError
            perform storage (ArticleDelete ref) `shouldReturn` Left NotFoundError
            perform storage (ArticleList True (ListView 0 1 [FilterArticleId ref] []))
                `shouldReturn`
                Right []
    do
        authorsToDelete <- generate $ chooseFrom (sampleSize `div` 5) authorRefs
        parallelFor_ authorsToDelete $ \authorRef -> do
            assertRight =<< perform storage (AuthorDelete authorRef)
            atomicModifyIORef_ authorTable $ Map.delete authorRef
            atomicModifyIORef_ userAuthorConnSet $ Bimap.removeRight authorRef
        atomicModifyIORef_ articleTable $ Map.mapMaybe $ \article ->
            if articleAuthor article `elem` authorsToDelete
                then case articlePublicationStatus article of
                    PublishAt _ -> Just $ article {articleAuthor = Reference ""}
                    NonPublished -> Nothing
                else Just article
        validateArticles storage articleTable
    do
        categoryList <- toCategoryList <$> readIORef categoryTable
        categoriesToDelete <- generate $ chooseFrom (sampleSize `div` 5) categoryList
        parallelFor_ categoriesToDelete $ \category -> do
            assertRight =<< perform storage (CategoryDelete (categoryId category))
            Just parentRef <- atomicModifyIORef' categoryTable $ swap . Tree.exclude (categoryId category)
            atomicModifyIORef_ articleTable $ Map.map $ \article ->
                if articleCategory article == categoryId category
                    then article {articleCategory = parentRef}
                    else article
        validateArticles storage articleTable
    do
        articleList <- Map.elems <$> readIORef articleTable
        parallelFor_ articleList $ \article -> do
            perform storage (ArticleList True (ListView 0 1 [FilterArticleId (articleId article)] []))
                `shouldReturn`
                Right [article]
            perform storage (ArticleList False (ListView 0 1 [FilterArticleId (articleId article)] []))
                `shouldReturn`
                Right [article | articlePublicationStatus article >= PublishAt timeGenBase]
        parallelFor_ (Reference "" : authorRefs) $ \authorRef -> do
            perform storage (ArticleList True (ListView 0 maxBound [FilterArticleAuthorId authorRef] []))
                `shouldReturn`
                Right (filter (\article -> articleAuthor article == authorRef) articleList)
        userAuthorConns <- readIORef userAuthorConnSet
        let userAuthorList = (Reference "", Set.singleton (Reference ""))
                : Map.toList (Bimap.left userAuthorConns)
        parallelFor_ userAuthorList $ \(userRef, authorSet) -> do
            perform storage (ArticleList True (ListView 0 maxBound [FilterArticleUserId userRef] []))
                `shouldReturn`
                Right (filter (\article -> articleAuthor article `Set.member` authorSet) articleList)
        categoryTree <- readIORef categoryTable
        let transitiveSubcategories = Tree.descendantMap categoryTree
        let subcategoryList = (Reference "", Set.singleton $ Reference "") : Map.toList transitiveSubcategories
        parallelFor_ subcategoryList $ \(categoryRef, subcatSet) -> do
            perform storage (ArticleList True (ListView 0 maxBound [FilterArticleCategoryId categoryRef] []))
                `shouldReturn`
                Right (filter (\article -> articleCategory article == categoryRef) articleList)
            perform storage (ArticleList True (ListView 0 maxBound [FilterArticleTransitiveCategoryId categoryRef] []))
                `shouldReturn`
                Right (filter (\article -> articleCategory article `Set.member` subcatSet) articleList)
        let timeA = addUTCTime (86400 * (-500)) timeGenBase
        let timeB = addUTCTime (86400 * (365 * 10 + 500)) timeGenBase
        perform storage (ArticleList True (ListView 0 maxBound [FilterArticlePublishedBefore timeB] []))
            `shouldReturn`
            Right (filter (publishedBefore timeB) articleList)
        perform storage (ArticleList False (ListView 0 maxBound [FilterArticlePublishedBefore timeB] []))
            `shouldReturn`
            Right (filter (publishedBefore timeGenBase) articleList)
        perform storage (ArticleList True (ListView 0 maxBound [FilterArticlePublishedAfter timeA] []))
            `shouldReturn`
            Right (filter (publishedAfter timeA) articleList)
        perform storage (ArticleList False (ListView 0 maxBound [FilterArticlePublishedAfter timeA] []))
            `shouldReturn`
            Right (filter (publishedWithin timeA timeGenBase) articleList)
        perform storage (ArticleList True (ListView 0 maxBound [FilterArticlePublishedAfter timeA, FilterArticlePublishedBefore timeB] []))
            `shouldReturn`
            Right (filter (publishedWithin timeA timeB) articleList)
        perform storage (ArticleList False (ListView 0 maxBound [FilterArticlePublishedAfter timeA, FilterArticlePublishedBefore timeB] []))
            `shouldReturn`
            Right (filter (publishedWithin timeA timeGenBase) articleList)
        perform storage (ArticleList True (ListView 0 maxBound [] [(OrderArticleName, Ascending)]))
            `shouldReturn`
            Right (sortOn articleName articleList)
        perform storage (ArticleList True (ListView 0 maxBound [] [(OrderArticleDate, Ascending)]))
            `shouldReturn`
            Right (sortOn articlePublicationStatus articleList)
        authorMap <- readIORef authorTable
        perform storage (ArticleList True (ListView 0 maxBound [] [(OrderArticleAuthorName, Ascending)]))
            `shouldReturn`
            Right (sortOn (articleAuthorName authorMap) articleList)
        perform storage (ArticleList True (ListView 0 maxBound [] [(OrderArticleCategoryName, Ascending)]))
            `shouldReturn`
            Right (sortOn (articleCategoryName categoryTree) articleList)
    return ()
  where
    publishedBefore ta article = articlePublicationStatus article >= PublishAt ta
    publishedAfter ta article = articlePublicationStatus article <= PublishAt ta
    publishedWithin ta tb article = publishedAfter ta article && publishedBefore tb article
    articleAuthorName authorMap article = authorName <$> Map.lookup (articleAuthor article) authorMap
    articleCategoryName categoryTree article = case Tree.lookup (articleCategory article) categoryTree of
        Just (catName, _) -> Just catName
        Nothing -> Nothing

validateArticles storage articleTable = do
    articleList <- Map.elems <$> readIORef articleTable
    perform storage (ArticleList True (ListView 0 maxBound [] []))
        `shouldReturn`
        Right articleList
    perform storage (ArticleList False (ListView 0 maxBound [] []))
        `shouldReturn`
        Right (filter (\article -> articlePublicationStatus article >= PublishAt timeGenBase) articleList)

generateTags
    :: Handle -> Int
    -> IO (IORef (Map.Map (Reference Tag) Tag))
generateTags storage sampleSize = do
    tagTable <- newIORef Map.empty
    parallelFor_ [1 .. sampleSize] $ \_ -> do
        name <- generate $ randomText
        tag <- assertRight =<< perform storage (TagCreate name)
        tagName tag `shouldBe` name
        atomicModifyIORef_ tagTable $ Map.insert (tagId tag) tag
    validateTags storage tagTable
    return tagTable

minceTags
    :: Handle -> Int
    -> IORef (Map.Map (Reference Tag) Tag)
    -> IO ()
minceTags storage sampleSize tagTable = do
    do
        tagList <- Map.elems <$> readIORef tagTable
        toAlter <- generate $ chooseFrom (sampleSize `div` 5) tagList
        parallelFor_ toAlter $ \tag -> do
            name <- generate $ randomText
            assertRight =<< perform storage (TagSetName (tagId tag) name)
            atomicModifyIORef_ tagTable $ Map.insert (tagId tag) $
                tag {tagName = name}
        validateTags storage tagTable
    do
        tagList <- Map.elems <$> readIORef tagTable
        toDelete <- generate $ chooseFrom (sampleSize `div` 5) tagList
        parallelFor_ toDelete $ \tag -> do
            assertRight =<< perform storage (TagDelete (tagId tag))
            atomicModifyIORef_ tagTable $ Map.delete (tagId tag)
        validateTags storage tagTable
    do
        tagMap <- readIORef tagTable
        badRefs <- generate $
            replicateM 20 $
                randomReference `suchThat` (\ref -> not (Map.member ref tagMap))
        parallelFor_ badRefs $ \ref -> do
            perform storage (TagSetName ref "foo") `shouldReturn` Left NotFoundError
            perform storage (TagDelete ref) `shouldReturn` Left NotFoundError
            perform storage (TagList (ListView 0 1 [FilterTagId ref] []))
                `shouldReturn`
                Right []
    do
        tagList <- Map.elems <$> readIORef tagTable
        parallelFor_ tagList $ \tag -> do
            perform storage (TagList (ListView 0 1 [FilterTagId (tagId tag)] []))
                `shouldReturn`
                Right [tag]
        perform storage (TagList (ListView 0 maxBound [] [(OrderTagName, Ascending)]))
            `shouldReturn`
            Right (sortOn tagName tagList)
    return ()

validateTags
    :: Handle
    -> IORef (Map.Map (Reference Tag) Tag)
    -> IO ()
validateTags storage tagTable = do
    tagList <- Map.elems <$> readIORef tagTable
    perform storage (TagList (ListView 0 maxBound [] []))
        `shouldReturn`
        Right tagList

generateArticleTagRels
    :: Handle -> Int
    -> IORef (Map.Map (Reference Article) Article)
    -> IORef (Map.Map (Reference Tag) Tag)
    -> IO (IORef (Bimap.Bimap (Reference Article) (Reference Tag)))
generateArticleTagRels storage sampleSize articleTable tagTable = do
    articleRefs <- Map.keys <$> readIORef articleTable
    tagRefs <- Map.keys <$> readIORef tagTable
    connSet <- newIORef $ Bimap.fromKeys articleRefs tagRefs
    parallelFor_ articleRefs $ \articleRef -> do
        parallelFor_ tagRefs $ \tagRef -> do
            byChance 1 3
                (do
                    assertRight =<< perform storage (ArticleSetTag articleRef tagRef True)
                    atomicModifyIORef_ connSet $ Bimap.insert (articleRef, tagRef))
                (return ())
    validateArticleTagRels storage articleTable tagTable connSet
    return connSet

minceArticleTagRels
    :: Handle -> Int
    -> IORef (Map.Map (Reference Article) Article)
    -> IORef (Map.Map (Reference Tag) Tag)
    -> IORef (Bimap.Bimap (Reference Article) (Reference Tag))
    -> IO ()
minceArticleTagRels storage sampleSize articleTable tagTable connSet = do
    articleRefs <- Map.keys <$> readIORef articleTable
    tagRefs <- Map.keys <$> readIORef tagTable
    do
        parallelFor_ articleRefs $ \articleRef -> do
            parallelFor_ tagRefs $ \tagRef -> do
                conn <- generate $ randomBool
                assertRight =<< perform storage (ArticleSetTag articleRef tagRef conn)
                atomicModifyIORef_ connSet $ if conn
                    then Bimap.insert (articleRef, tagRef)
                    else Bimap.delete (articleRef, tagRef)
        validateArticleTagRels storage articleTable tagTable connSet
    do
        articlesToDelete <- generate $ chooseFrom (sampleSize `div` 5) articleRefs
        parallelFor_ articlesToDelete $ \articleRef -> do
            assertRight =<< perform storage (ArticleDelete articleRef)
            atomicModifyIORef_ articleTable $ Map.delete articleRef
            atomicModifyIORef_ connSet $ Bimap.removeLeft articleRef
        validateArticleTagRels storage articleTable tagTable connSet
    do
        tagsToDelete <- generate $ chooseFrom (sampleSize `div` 5) tagRefs
        parallelFor_ tagsToDelete $ \tagRef -> do
            assertRight =<< perform storage (TagDelete tagRef)
            atomicModifyIORef_ tagTable $ Map.delete tagRef
            atomicModifyIORef_ connSet $ Bimap.removeRight tagRef
        validateArticleTagRels storage articleTable tagTable connSet
    do
        conns <- readIORef connSet
        articleMap <- readIORef articleTable
        parallelFor_ [1..sampleSize] $ \_ -> do
            sample <- generate $ do
                n <- randomWithin 1 (sampleSize `div` 5)
                chooseFrom n (Map.toList $ Bimap.right conns)
            let tags = map fst sample
            let articleUnionSet = Set.unions $ map snd sample
            perform storage (ArticleList True (ListView 0 maxBound [FilterArticleTagIds tags] []))
                `shouldReturn`
                Right (map (articleMap Map.!) $ Set.elems articleUnionSet)
            let articleIntersectionSet = foldl1' Set.intersection $ map snd sample
            perform storage (ArticleList True (ListView 0 maxBound [FilterArticleTagIds [tag] | tag <- tags] []))
                `shouldReturn`
                Right (map (articleMap Map.!) $ Set.elems articleIntersectionSet)
    return ()

validateArticleTagRels
    :: Handle
    -> IORef (Map.Map (Reference Article) Article)
    -> IORef (Map.Map (Reference Tag) Tag)
    -> IORef (Bimap.Bimap (Reference Article) (Reference Tag))
    -> IO ()
validateArticleTagRels storage articleTable tagTable connSet = do
    articleMap <- readIORef articleTable
    tagMap <- readIORef tagTable
    conns <- readIORef connSet
    parallelFor_ (Map.toList (Bimap.right conns)) $ \(tagRef, articleSet) -> do
        perform storage (ArticleList True (ListView 0 maxBound [FilterArticleTagIds [tagRef]] []))
            `shouldReturn`
            Right (map (articleMap Map.!) $ Set.elems articleSet)
    parallelFor_ (Map.toList (Bimap.left conns)) $ \(articleRef, tagSet) -> do
        perform storage (TagList (ListView 0 maxBound [FilterTagArticleId articleRef] []))
            `shouldReturn`
            Right (map (tagMap Map.!) $ Set.elems tagSet)

generateComments
    :: Handle -> Int
    -> IORef (Map.Map (Reference User) User)
    -> IORef (Map.Map (Reference Article) Article)
    -> IO (IORef (Map.Map (Reference Comment) Comment))
generateComments storage sampleSize userTable articleTable = do
    userRefs <- Map.keys <$> readIORef userTable
    articleRefs <- Map.keys <$> readIORef articleTable
    commentTable <- newIORef $ Map.empty
    parallelFor_ userRefs $ \userRef -> do
        parallelFor_ articleRefs $ \articleRef -> do
            byChance 1 3
                (do
                    text <- generate $ randomText
                    comment <- assertRight =<< perform storage (CommentCreate articleRef userRef text)
                    commentArticle comment `shouldBe` articleRef
                    commentUser comment `shouldBe` userRef
                    commentText comment `shouldBe` text
                    commentEditDate comment `shouldBe` Nothing
                    atomicModifyIORef_ commentTable $ Map.insert (commentId comment) comment)
                (return ())
    validateComments storage commentTable
    return commentTable

minceComments
    :: Handle -> Int
    -> IORef (Map.Map (Reference User) User)
    -> IORef (Map.Map (Reference Article) Article)
    -> IORef (Map.Map (Reference Comment) Comment)
    -> IO ()
minceComments storage sampleSize userTable articleTable commentTable = do
    do
        commentList <- Map.elems <$> readIORef commentTable
        parallelFor_ commentList $ \comment -> do
            byChance 1 3
                (do
                    text <- generate $ randomText
                    assertRight =<< perform storage (CommentSetText (commentId comment) text)
                    Right [comment2] <- perform storage (CommentList (ListView 0 1 [FilterCommentId (commentId comment)] []))
                    commentId comment2 `shouldBe` commentId comment
                    commentArticle comment2 `shouldBe` commentArticle comment
                    commentUser comment2 `shouldBe` commentUser comment
                    commentText comment2 `shouldBe` text
                    commentDate comment2 `shouldBe` commentDate comment
                    commentEditDate comment2 `shouldSatisfy` isJust
                    commentEditDate comment2 `shouldSatisfy` \(Just editDate) -> editDate >= commentDate comment
                    atomicModifyIORef_ commentTable $ Map.insert (commentId comment) comment2)
                (return ())
        validateComments storage commentTable
    do
        commentList <- Map.elems <$> readIORef commentTable
        toDelete <- generate $ chooseFrom (sampleSize `div` 5) commentList
        parallelFor_ toDelete $ \comment -> do
            assertRight =<< perform storage (CommentDelete (commentId comment))
            atomicModifyIORef_ commentTable $ Map.delete (commentId comment)
        validateComments storage commentTable
    do
        userRefs <- Map.keys <$> readIORef userTable
        usersToDelete <- generate $ chooseFrom (sampleSize `div` 5) userRefs
        parallelFor_ usersToDelete $ \userRef -> do
            assertRight =<< perform storage (UserDelete userRef)
            atomicModifyIORef_ userTable $ Map.delete userRef
            atomicModifyIORef_ commentTable $ Map.map $ \comment -> do
                if commentUser comment == userRef
                    then comment {commentUser = Reference ""}
                    else comment
        validateComments storage commentTable
    do
        articleRefs <- Map.keys <$> readIORef articleTable
        articlesToDelete <- generate $ chooseFrom (sampleSize `div` 5) articleRefs
        parallelFor_ articlesToDelete $ \articleRef -> do
            assertRight =<< perform storage (ArticleDelete articleRef)
            atomicModifyIORef_ articleTable $ Map.delete articleRef
            atomicModifyIORef_ commentTable $ Map.filter $ \comment ->
                commentArticle comment /= articleRef
        validateComments storage commentTable
    do
        commentMap <- readIORef commentTable
        badRefs <- generate $
            replicateM 20 $
                randomReference `suchThat` (\ref -> not (Map.member ref commentMap))
        parallelFor_ badRefs $ \ref -> do
            perform storage (CommentSetText ref "foo") `shouldReturn` Left NotFoundError
            perform storage (CommentDelete ref) `shouldReturn` Left NotFoundError
            perform storage (CommentList (ListView 0 1 [FilterCommentId ref] []))
                `shouldReturn`
                Right []
    do
        userRefs <- Map.keys <$> readIORef userTable
        articleRefs <- Map.keys <$> readIORef articleTable
        commentList <- Map.elems <$> readIORef commentTable
        parallelFor_ commentList $ \comment -> do
            perform storage (CommentList (ListView 0 1 [FilterCommentId (commentId comment)] []))
                `shouldReturn`
                Right [comment]
        parallelFor_ (Reference "" : userRefs) $ \userRef -> do
            perform storage (CommentList (ListView 0 maxBound [FilterCommentUserId userRef] []))
                `shouldReturn`
                Right (filter (isOfUser userRef) commentList)
            perform storage (CommentList (ListView 0 maxBound [FilterCommentUserId userRef] [(OrderCommentDate, Ascending)]))
                `shouldReturn`
                Right (sortOn (Down . commentDate) $ filter (isOfUser userRef) commentList)
        parallelFor_ articleRefs $ \articleRef -> do
            perform storage (CommentList (ListView 0 maxBound [FilterCommentArticleId articleRef] []))
                `shouldReturn`
                Right (filter (isOfArticle articleRef) commentList)
            perform storage (CommentList (ListView 0 maxBound [FilterCommentArticleId articleRef] [(OrderCommentDate, Ascending)]))
                `shouldReturn`
                Right (sortOn (Down . commentDate) $ filter (isOfArticle articleRef) commentList)
    return ()
  where
    isOfUser userRef comment = commentUser comment == userRef
    isOfArticle articleRef comment = commentArticle comment == articleRef

validateComments
    :: Handle
    -> IORef (Map.Map (Reference Comment) Comment)
    -> IO ()
validateComments storage commentTable = do
    commentList <- Map.elems <$> readIORef commentTable
    perform storage (CommentList (ListView 0 maxBound [] []))
        `shouldReturn`
        Right commentList
