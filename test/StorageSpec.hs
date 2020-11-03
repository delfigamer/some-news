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
import qualified Logger
import qualified Sql.Database as Db
import qualified Sql.Database.Config as Db

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
            it "handles objects" specBody

specBody :: [TestConfig] -> IO ()
specBody testconfList = do
    forM_ testconfList $ \testConf -> do
        Logger.withHtmlLogger ".test.html" $ \logger -> do
            Db.withDatabase (databaseConfig testConf) logger $ \db -> do
                performRandomStorageTest testConf logger db

performRandomStorageTest :: TestConfig -> Logger.Handle -> Db.Handle -> IO ()
performRandomStorageTest testConf logger db = do
    clearDatabase db
    upgradeSchema logger db `shouldReturn` Right ()
    withSqlStorage logger db (expectationFailure . show) $ \storage -> do
        let sampleSize = confSampleSize testConf
        userTable <- testUsers storage sampleSize
        authorTable <- testAuthors storage sampleSize
        testAccessKeys storage sampleSize userTable
        userAuthorConnSet <- testAuthorUserConns storage sampleSize userTable authorTable
        categoryTable <- testCategories storage sampleSize
        articleTable <- testArticles storage sampleSize userTable authorTable categoryTable
        return ()

testUsers
    :: Handle -> Int
    -> IO (IORef (Map.Map (Reference User) User))
testUsers storage sampleSize = do
    userTable <- newIORef Map.empty
    do
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
        checkUsers userTable
    do
        userList <- Map.elems <$> readIORef userTable
        toAlter <- generate $ chooseFrom (sampleSize `div` 5) userList
        parallelFor_ toAlter $ \user -> do
            name <- generate $ randomText
            surname <- generate $ randomText
            assertRight =<< perform storage (UserSetName (userId user) name surname)
            atomicModifyIORef_ userTable $ Map.insert (userId user) $
                user {userName = name, userSurname = surname}
        checkUsers userTable
    do
        userList <- Map.elems <$> readIORef userTable
        toAlter <- generate $ chooseFrom (sampleSize `div` 5) userList
        parallelFor_ toAlter $ \user -> do
            isAdmin <- generate $ randomBool
            assertRight =<< perform storage (UserSetIsAdmin (userId user) isAdmin)
            atomicModifyIORef_ userTable $ Map.insert (userId user) $
                user {userIsAdmin = isAdmin}
        checkUsers userTable
    do
        userList <- Map.elems <$> readIORef userTable
        toDelete <- generate $ chooseFrom (sampleSize `div` 5) userList
        parallelFor_ toDelete $ \user -> do
            assertRight =<< perform storage (UserDelete (userId user))
            atomicModifyIORef_ userTable $ Map.delete (userId user)
        checkUsers userTable
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
    return userTable
  where
    checkUsers userTable = do
        userList <- Map.elems <$> readIORef userTable
        perform storage (UserList (ListView 0 maxBound [] []))
            `shouldReturn`
            Right userList

testAccessKeys
    :: Handle -> Int
    -> (IORef (Map.Map (Reference User) User))
    -> IO ()
testAccessKeys storage sampleSize userTable = do
    userRefs <- Map.keys <$> readIORef userTable
    akeyTable <- newIORef $ Map.fromList $ map (\uref -> (uref, Set.empty)) userRefs
    do
        parallelFor_ userRefs $ \userRef -> do
            repeatSome_ 1 5 $ do
                akey <- assertRight =<< perform storage (AccessKeyCreate userRef)
                atomicModifyIORef_ akeyTable $ Map.update (Just . Set.insert akey) userRef
        checkAkeys akeyTable
    do
        userKeys <- readIORef akeyTable
        let allKeys = do
                (userRef, akeys) <- Map.toList userKeys
                key <- Set.toList akeys
                [(userRef, key)]
        toDelete <- generate $ chooseFrom (sampleSize `div` 5) allKeys
        parallelFor_ toDelete $ \(userRef, akey) -> do
            assertRight =<< perform storage (AccessKeyDelete userRef (accessKeyId akey))
            atomicModifyIORef_ akeyTable $ Map.update (Just . Set.delete akey) userRef
        checkAkeys akeyTable
    do
        userKeys <- readIORef akeyTable
        let oneKeys = do
                (userRef, akeys) <- Map.toList userKeys
                oneKey : _ <- return $ Set.toList akeys
                [(userRef, oneKey)]
        let (users, keys) = unzip oneKeys
        let badKeysFull = zip users (drop 1 keys)
        badKeys <- generate $ chooseFrom (sampleSize `div` 5) badKeysFull
        parallelFor_ badKeys $ \(userRef, akey) -> do
            perform storage (AccessKeyDelete userRef (accessKeyId akey))
                `shouldReturn`
                Left NotFoundError
        checkAkeys akeyTable
    return ()
  where
    checkAkeys akeyTable = do
        akeys <- Map.toList <$> readIORef akeyTable
        parallelFor_ akeys $ \(userRef, akeySet) -> do
            perform storage (AccessKeyList userRef (ListView 0 maxBound [] []))
                `shouldReturn`
                Right (map accessKeyId $ Set.toAscList akeySet)
            parallelFor_ (Set.toList akeySet) $ \akey -> do
                perform storage (AccessKeyLookup akey)
                    `shouldReturn`
                    Right userRef

testAuthors
    :: Handle -> Int
    -> IO (IORef (Map.Map (Reference Author) Author))
testAuthors storage sampleSize = do
    authorTable <- newIORef Map.empty
    do
        parallelFor_ [1 .. sampleSize] $ \_ -> do
            name <- generate $ randomText
            description <- generate $ randomText
            author <- assertRight =<< perform storage (AuthorCreate name description)
            authorName author `shouldBe` name
            authorDescription author `shouldBe` description
            atomicModifyIORef_ authorTable $ Map.insert (authorId author) author
        checkAuthors authorTable
    do
        authorList <- Map.elems <$> readIORef authorTable
        toAlter <- generate $ chooseFrom (sampleSize `div` 5) authorList
        parallelFor_ toAlter $ \author -> do
            name <- generate $ randomText
            assertRight =<< perform storage (AuthorSetName (authorId author) name)
            atomicModifyIORef_ authorTable $ Map.insert (authorId author) $
                author {authorName = name}
        checkAuthors authorTable
    do
        authorList <- Map.elems <$> readIORef authorTable
        toAlter <- generate $ chooseFrom (sampleSize `div` 5) authorList
        parallelFor_ toAlter $ \author -> do
            description <- generate $ randomText
            assertRight =<< perform storage (AuthorSetDescription (authorId author) description)
            atomicModifyIORef_ authorTable $ Map.insert (authorId author) $
                author {authorDescription = description}
        checkAuthors authorTable
    do
        authorList <- Map.elems <$> readIORef authorTable
        toDelete <- generate $ chooseFrom (sampleSize `div` 5) authorList
        parallelFor_ toDelete $ \author -> do
            assertRight =<< perform storage (AuthorDelete (authorId author))
            atomicModifyIORef_ authorTable $ Map.delete (authorId author)
        checkAuthors authorTable
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
    return authorTable
  where
    checkAuthors authorTable = do
        authorList <- Map.elems <$> readIORef authorTable
        perform storage (AuthorList (ListView 0 maxBound [] []))
            `shouldReturn`
            Right authorList

testAuthorUserConns
    :: Handle -> Int
    -> IORef (Map.Map (Reference User) User)
    -> IORef (Map.Map (Reference Author) Author)
    -> IO (IORef (Set.Set (Reference User, Reference Author)))
testAuthorUserConns storage sampleSize userTable authorTable = do
    connSet <- newIORef Set.empty
    userRefs <- Map.keys <$> readIORef userTable
    authorRefs <- Map.keys <$> readIORef authorTable
    do
        parallelFor_ authorRefs $ \authorRef -> do
            parallelFor_ userRefs $ \userRef -> do
                byChance 1 3
                    (do
                        assertRight =<< perform storage (AuthorSetOwnership authorRef userRef True)
                        atomicModifyIORef_ connSet $ Set.insert (userRef, authorRef))
                    (return ())
        checkConns connSet
    do
        parallelFor_ authorRefs $ \authorRef -> do
            parallelFor_ userRefs $ \userRef -> do
                conn <- generate $ randomBool
                assertRight =<< perform storage (AuthorSetOwnership authorRef userRef conn)
                atomicModifyIORef_ connSet $ if conn
                    then Set.insert (userRef, authorRef)
                    else Set.delete (userRef, authorRef)
        checkConns connSet
    do
        usersToDelete <- generate $ chooseFrom (sampleSize `div` 5) userRefs
        parallelFor_ usersToDelete $ \userRef -> do
            assertRight =<< perform storage (UserDelete userRef)
            atomicModifyIORef_ userTable $ Map.delete userRef
            atomicModifyIORef_ connSet $ \set -> do
                let (lt, _, gt) = splitSetOn ((compare userRef) . fst) set
                Set.union lt gt
        checkConns connSet
    do
        authorsToDelete <- generate $ chooseFrom (sampleSize `div` 5) authorRefs
        parallelFor_ authorsToDelete $ \authorRef -> do
            assertRight =<< perform storage (AuthorDelete authorRef)
            atomicModifyIORef_ authorTable $ Map.delete authorRef
            atomicModifyIORef_ connSet $ Set.filter $ \(_, aref) -> aref /= authorRef
        checkConns connSet
    return connSet
  where
    checkConns connSet = do
        connSet <- readIORef connSet
        users <- readIORef userTable
        authors <- readIORef authorTable
        parallelFor_ (Map.toList users) $ \(userRef, _) -> do
            let authorRefList = authorsOfUser connSet userRef
            let authorList = map (authors Map.!) authorRefList
            perform storage (AuthorList (ListView 0 maxBound [FilterAuthorUserId userRef] []))
                `shouldReturn`
                Right authorList
        parallelFor_ (Map.toList authors) $ \(authorRef, _) -> do
            let userRefList = usersOfAuthor connSet authorRef
            let userList = map (users Map.!) userRefList
            perform storage (UserList (ListView 0 maxBound [FilterUserAuthorId authorRef] []))
                `shouldReturn`
                Right userList
    usersOfAuthor set authorRef = sort $ do
        (uref, aref) <- Set.toList set
        guard $ aref == authorRef
        [uref]
    authorsOfUser set userRef = do
        let (_, eq, _) = splitSetOn ((compare userRef) . fst) set
        map (\(_, aref) -> aref) $ Set.toAscList eq

testCategories
    :: Handle -> Int
    -> IO (IORef (Map.Map (Reference Category) Category))
testCategories storage sampleSize = do
    categoryTable <- newIORef Map.empty
    do
        parallelFor_ [1 .. sampleSize] $ \_ -> do
            prevCats <- Map.keys <$> readIORef categoryTable
            name <- generate $ randomText
            [parent] <- generate $ chooseFrom 1 $ Reference "" : prevCats
            category <- assertRight =<< perform storage (CategoryCreate name parent)
            categoryName category `shouldBe` name
            categoryParent category `shouldBe` parent
            atomicModifyIORef_ categoryTable $ Map.insert (categoryId category) category
        checkCategories categoryTable
    do
        categoryList <- Map.elems <$> readIORef categoryTable
        toAlter <- generate $ chooseFrom (sampleSize `div` 5) categoryList
        parallelFor_ toAlter $ \category -> do
            name <- generate $ randomText
            assertRight =<< perform storage (CategorySetName (categoryId category) name)
            atomicModifyIORef_ categoryTable $ Map.insert (categoryId category) $
                category {categoryName = name}
        checkCategories categoryTable
    do
        categoryMap <- readIORef categoryTable
        toAlter <- generate $ chooseFrom (sampleSize `div` 5) (Map.elems categoryMap)
        parallelFor_ toAlter $ \category -> do
            [parent] <- generate $ do
                b <- randomBool
                if b
                    then return [Reference ""]
                    else chooseFromSuch
                        (\cref -> categoryId category `notElem` parentList categoryMap cref)
                        1 (Map.keys categoryMap)
            assertRight =<< perform storage (CategorySetParent (categoryId category) parent)
            atomicModifyIORef_ categoryTable $ Map.insert (categoryId category) $
                category {categoryParent = parent}
        checkCategories categoryTable
    do
        categoryList <- Map.elems <$> readIORef categoryTable
        toDelete <- generate $ chooseFrom (sampleSize `div` 5) categoryList
        parallelFor_ toDelete $ \category -> do
            assertRight =<< perform storage (CategoryDelete (categoryId category))
            atomicModifyIORef_ categoryTable $ \cats1 -> do
                let cats2 = Map.delete (categoryId category) cats1
                flip Map.map cats2 $ \cat ->
                    if categoryParent cat == categoryId category
                        then cat {categoryParent = categoryParent category}
                        else cat
        checkCategories categoryTable
    do
        categoryMap <- readIORef categoryTable
        badRefs <- generate $
            replicateM 20 $
                randomReference `suchThat` (\ref -> not (Map.member ref categoryMap))
        parallelFor_ badRefs $ \ref -> do
            perform storage (CategorySetName ref "foo") `shouldReturn` Left NotFoundError
            perform storage (CategorySetParent ref (Reference "")) `shouldReturn` Left NotFoundError
            perform storage (CategoryDelete ref) `shouldReturn` Left NotFoundError
            perform storage (CategoryList (ListView 0 1 [FilterCategoryId ref] []))
                `shouldReturn`
                Right []
    do
        categoryMap <- readIORef categoryTable
        let categoryList = Map.elems categoryMap
        parallelFor_ categoryList $ \category -> do
            perform storage (CategoryList (ListView 0 1 [FilterCategoryId (categoryId category)] []))
                `shouldReturn`
                Right [category]
        perform storage (CategoryList (ListView 0 maxBound [] [(OrderCategoryName, Ascending)]))
            `shouldReturn`
            Right (sortOn categoryName categoryList)
        let strictSubcategories = Map.mapWithKey
                (\cref _ -> Map.keysSet $ Map.filter (\cat -> categoryParent cat == cref) categoryMap)
                categoryMap
        parallelFor_ (Map.toList strictSubcategories) $ \(categoryRef, subcatSet) -> do
            perform storage (CategoryList (ListView 0 maxBound [FilterCategoryParentId categoryRef] []))
                `shouldReturn`
                Right (map (categoryMap Map.!) (Set.toList subcatSet))
        let transitiveSubcategories = MapLazy.mapWithKey
                (\cref subcats -> Set.insert cref $
                    Set.unions $
                        subcats :
                            map (transitiveSubcategories Map.!) (Set.toList subcats))
                strictSubcategories
        parallelFor_ (Map.toList transitiveSubcategories) $ \(categoryRef, subcatSet) -> do
            perform storage (CategoryList (ListView 0 maxBound [FilterCategoryTransitiveParentId categoryRef] []))
                `shouldReturn`
                Right (map (categoryMap Map.!) (Set.toList subcatSet))
            someSubcats <- generate $ chooseFrom 2 $ Set.toList subcatSet
            parallelFor_ someSubcats $ \subcat -> do
                perform storage (CategorySetParent categoryRef subcat) `shouldReturn` Left InvalidRequestError
    return categoryTable
  where
    checkCategories categoryTable = do
        categoryList <- Map.elems <$> readIORef categoryTable
        perform storage (CategoryList (ListView 0 maxBound [] []))
            `shouldReturn`
            Right categoryList
    parentList catMap (Reference "") = []
    parentList catMap cref = cref : parentList catMap (categoryParent $ catMap Map.! cref)

testArticles
    :: Handle -> Int
    -> IORef (Map.Map (Reference User) User)
    -> IORef (Map.Map (Reference Author) Author)
    -> IORef (Map.Map (Reference Category) Category)
    -> IO (IORef (Map.Map (Reference Article) Article))
testArticles storage sampleSize userTable authorTable categoryTable = do
    authorRefs <- Map.keys <$> readIORef authorTable
    categoryRefs <- Map.keys <$> readIORef categoryTable
    articleTable <- newIORef Map.empty
    do
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
        checkArticles articleTable
    do
        articleList <- Map.elems <$> readIORef articleTable
        toAlter <- generate $ chooseFrom (sampleSize `div` 5) articleList
        parallelFor_ toAlter $ \article -> do
            [authorRef] <- generate $ chooseFrom 1 authorRefs
            assertRight =<< perform storage (ArticleSetAuthor (articleId article) authorRef)
            atomicModifyIORef_ articleTable $ Map.insert (articleId article) $
                article {articleAuthor = authorRef}
        checkArticles articleTable
    do
        articleList <- Map.elems <$> readIORef articleTable
        toAlter <- generate $ chooseFrom (sampleSize `div` 5) articleList
        parallelFor_ toAlter $ \article -> do
            name <- generate $ randomText
            version <- assertRight =<< perform storage
                (ArticleSetName (articleId article) (articleVersion article) name)
            atomicModifyIORef_ articleTable $ Map.insert (articleId article) $
                article {articleName = name, articleVersion = version}
        checkArticles articleTable
    do
        articleList <- Map.elems <$> readIORef articleTable
        toAlter <- generate $ chooseFrom (sampleSize `div` 5) articleList
        parallelFor_ toAlter $ \article -> do
            text <- generate $ randomText
            version <- assertRight =<< perform storage
                (ArticleSetText (articleId article) (articleVersion article) text)
            atomicModifyIORef_ articleTable $ Map.insert (articleId article) $
                article {articleText = text, articleVersion = version}
        checkArticles articleTable
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
        checkArticles articleTable
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
        checkArticles articleTable
    do
        articleList <- Map.elems <$> readIORef articleTable
        toDelete <- generate $ chooseFrom (sampleSize `div` 5) articleList
        parallelFor_ toDelete $ \article -> do
            assertRight =<< perform storage (ArticleDelete (articleId article))
            atomicModifyIORef_ articleTable $ Map.delete (articleId article)
        checkArticles articleTable
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
        atomicModifyIORef_ articleTable $ Map.mapMaybe $ \article ->
            if articleAuthor article `elem` authorsToDelete
                then case articlePublicationStatus article of
                    PublishAt _ -> Just $ article {articleAuthor = Reference ""}
                    NonPublished -> Nothing
                else Just article
        checkArticles articleTable
    do
        articleList <- Map.elems <$> readIORef articleTable
        parallelFor_ articleList $ \article -> do
            perform storage (ArticleList True (ListView 0 1 [FilterArticleId (articleId article)] []))
                `shouldReturn`
                Right [article]
            perform storage (ArticleList False (ListView 0 1 [FilterArticleId (articleId article)] []))
                `shouldReturn`
                Right [article | isPublished article]
        parallelFor_ (Reference "" : authorRefs) $ \authorRef -> do
            perform storage (ArticleList True (ListView 0 maxBound [FilterArticleAuthorId authorRef] []))
                `shouldReturn`
                Right (filter (\article -> articleAuthor article == authorRef) articleList)
    return articleTable
  where
    checkArticles articleTable = do
        articleList <- Map.elems <$> readIORef articleTable
        perform storage (ArticleList True (ListView 0 maxBound [] []))
            `shouldReturn`
            Right articleList
        perform storage (ArticleList False (ListView 0 maxBound [] []))
            `shouldReturn`
            Right (filter isPublished articleList)
    isPublished article = articlePublicationStatus article >= PublishAt timeGenBase
