{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE StandaloneDeriving #-}

module StorageSpec
    ( spec
    ) where

import Control.Concurrent
import Control.Concurrent.MVar
import Control.Exception
import Control.Monad
import Data.Aeson hiding (Result)
import Data.Hashable
import Data.IORef
import Data.Int
import Data.List
import Data.Maybe
import Data.Ord
import Data.Time.Clock
import Data.Tuple
import Data.Yaml
import GHC.Generics
import System.IO.Unsafe
import Test.Hspec
import qualified Data.ByteString as BS
import qualified Data.ByteString.Builder as Builder
import qualified Data.ByteString.Lazy as LBS
import qualified Data.Char as Char
import qualified Data.Map as PureMap
import qualified Data.Text as Text
import qualified System.IO as IO
import Gen
import Parallel
import Sql.Query
import Storage
import Tuple
import qualified Logger
import qualified Sql.Database as Db
import qualified Sql.Database.Config as Db
import qualified TData.Map as Map
import qualified TData.Multimap as Multimap
import qualified TData.Relmap as Relmap
import qualified TData.Tree as Tree

deriving instance Generic AccessKey
instance Hashable AccessKey

data TestConfig = TestConfig
    { databaseConfig :: Db.DatabaseConfig
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

clearDatabase :: Db.Database -> IO ()
clearDatabase db = do
    mapM_ (Db.execute db Db.ReadCommited . Db.query . DropTable) $ reverse $
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
        , "sn_tickets"
        ]

assertRight :: (Show a, Show b, HasCallStack) => Either a b -> IO b
assertRight (Right x) = return x
assertRight r = expectationFailure ("Expected a Right, got " ++ show r) >> undefined

sampleBadRefs :: (Reference a -> IO Bool) -> IO [Reference a]
sampleBadRefs memberF = do
    forM [1..20] $ \_ -> do
        makeOne
  where
    makeOne = do
        ref <- generate $ randomReference
        isMember <- memberF ref
        if isMember
            then makeOne
            else return ref

sampleSubsetProduct :: Int -> Map.Map a b -> Map.Map c d -> (a -> c -> IO ()) -> IO ()
sampleSubsetProduct sampleSize atable btable func = do
    let subsize = ceiling (sqrt (fromIntegral sampleSize :: Float))
    akeys <- Map.keys atable
    asample <- generate $ chooseFrom subsize akeys
    bkeys <- Map.keys btable
    bsample <- generate $ chooseFrom subsize bkeys
    parallelFor_ asample $ \a -> do
        parallelFor_ bsample $ \b -> do
            byChance 1 2
                (func a b)
                (return ())

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
                categoryTree <- generateCategories storage sampleSize
                minceCategories storage sampleSize categoryTree
            clearStorage "handles article objects" $ \sampleSize storage -> do
                userTable <- generateUsers storage sampleSize
                authorTable <- generateAuthors storage sampleSize
                userAuthorRels <- generateUserAuthorRels storage sampleSize userTable authorTable
                categoryTree <- generateCategories storage sampleSize
                articleTable <- generateArticles storage sampleSize authorTable categoryTree
                minceArticles storage sampleSize userTable authorTable userAuthorRels categoryTree articleTable
            clearStorage "handles tag objects" $ \sampleSize storage -> do
                tagTable <- generateTags storage sampleSize
                minceTags storage sampleSize tagTable
            clearStorage "handles article-tag relations" $ \sampleSize storage -> do
                authorTable <- generateAuthors storage 1
                categoryTree <- generateCategories storage 1
                articleTable <- generateArticles storage sampleSize authorTable categoryTree
                tagTable <- generateTags storage sampleSize
                articleTagRels <- generateArticleTagRels storage sampleSize articleTable tagTable
                minceArticleTagRels storage sampleSize articleTable tagTable articleTagRels
            clearStorage "handles comment objects" $ \sampleSize storage -> do
                userTable <- generateUsers storage sampleSize
                authorTable <- generateAuthors storage 1
                categoryTree <- generateCategories storage 1
                articleTable <- generateArticles storage sampleSize authorTable categoryTree
                commentTable <- generateComments storage sampleSize userTable articleTable
                minceComments storage sampleSize userTable articleTable commentTable
            clearStorage "handles files" $ \sampleSize storage -> do
                userTable <- generateUsers storage sampleSize
                authorTable <- generateAuthors storage 1
                categoryTree <- generateCategories storage 1
                articleTable <- generateArticles storage sampleSize authorTable categoryTree
                fileTable <- generateFiles storage sampleSize userTable articleTable
                minceFiles storage sampleSize userTable articleTable fileTable

testStorageConfig :: StorageConfig
testStorageConfig = StorageConfig
    { storageConfigUserIdLength = 10
    , storageConfigAccessKeyIdLength = 11
    , storageConfigAccessKeyTokenLength = 12
    , storageConfigAuthorIdLength = 13
    , storageConfigCategoryIdLength = 14
    , storageConfigArticleIdLength = 15
    , storageConfigArticleVersionLength = 16
    , storageConfigTagIdLength = 17
    , storageConfigCommentIdLength = 18
    , storageConfigFileIdLength = 19
    }

clearStorage :: String -> (Int -> Storage -> IO ()) -> SpecWith [TestConfig]
clearStorage name body = do
    it name $ \testConfList -> do
        forM_ testConfList $ \testConf -> do
            let logfile = ".test-" ++ name ++ ".html"
            Logger.withHtmlLogger logfile $ \logger -> do
                Db.withDatabase (databaseConfig testConf) logger $ \db -> do
                    clearDatabase db
                    upgradeSchema logger db `shouldReturn` Right ()
                    withSqlStorage testStorageConfig logger db (expectationFailure . show) $ \storage -> do
                        body (confSampleSize testConf) storage

generateUsers
    :: Storage -> Int
    -> IO (Map.Map (Reference User) (User, BS.ByteString))
generateUsers storage sampleSize = do
    userTable <- Map.new
    parallelFor_ [1 .. sampleSize] $ \_ -> do
        name <- generate $ randomText
        surname <- generate $ randomText
        password <- generate $ randomWithin 0 100 >>= randomByteString
        user <- assertRight =<< storagePerform storage (UserCreate name surname (Password password))
        userName user `shouldBe` name
        userSurname user `shouldBe` surname
        userIsAdmin user `shouldBe` False
        isAdmin <- generate $ randomBool
        when isAdmin $
            assertRight =<< storagePerform storage (UserSetIsAdmin (userId user) True)
        Map.insert userTable (userId user) $
            (user {userIsAdmin = isAdmin}, password)
    return userTable

validateUsers
    :: HasCallStack
    => Storage
    -> Map.Map (Reference User) (User, BS.ByteString)
    -> IO ()
validateUsers storage userTable = do
    userList <- Map.elems userTable
    storagePerform storage (UserList (ListView 0 maxBound [] []))
        `shouldReturn`
        Right (sortOn userId $ map fst userList)
    parallelFor_ userList $ \(user, password) -> do
        storagePerform storage (UserCheckPassword (userId user) (Password password))
            `shouldReturn`
            Right ()

minceUsers
    :: Storage -> Int
    -> Map.Map (Reference User) (User, BS.ByteString)
    -> IO ()
minceUsers storage sampleSize userTable = do
    validateUsers storage userTable
    do
        userList <- Map.elems userTable
        toAlter <- generate $ chooseFrom (sampleSize `div` 5) userList
        parallelFor_ toAlter $ \(user, password) -> do
            name <- generate $ randomText
            surname <- generate $ randomText
            assertRight =<< storagePerform storage (UserSetName (userId user) name surname)
            Map.insert userTable (userId user) $
                (user {userName = name, userSurname = surname}, password)
        validateUsers storage userTable
    do
        userList <- Map.elems userTable
        toAlter <- generate $ chooseFrom (sampleSize `div` 5) userList
        parallelFor_ toAlter $ \(user, password) -> do
            isAdmin <- generate $ randomBool
            assertRight =<< storagePerform storage (UserSetIsAdmin (userId user) isAdmin)
            Map.insert userTable (userId user) $
                (user {userIsAdmin = isAdmin}, password)
        validateUsers storage userTable
    do
        userList <- Map.elems userTable
        toAlter <- generate $ chooseFrom (sampleSize `div` 5) userList
        parallelFor_ toAlter $ \(user, _) -> do
            password <- generate $ randomWithin 0 100 >>= randomByteString
            assertRight =<< storagePerform storage (UserSetPassword (userId user) (Password password))
            Map.insert userTable (userId user) $
                (user, password)
        validateUsers storage userTable
    do
        userList <- Map.elems userTable
        toDelete <- generate $ chooseFrom (sampleSize `div` 5) userList
        parallelFor_ toDelete $ \(user, _) -> do
            assertRight =<< storagePerform storage (UserDelete (userId user))
            Map.delete userTable (userId user)
        validateUsers storage userTable
    do
        badRefs <- sampleBadRefs $ Map.member userTable
        parallelFor_ badRefs $ \ref -> do
            storagePerform storage (UserSetName ref "foo" "bar") `shouldReturn` Left NotFoundError
            storagePerform storage (UserSetIsAdmin ref True) `shouldReturn` Left NotFoundError
            storagePerform storage (UserSetPassword ref "foo") `shouldReturn` Left NotFoundError
            storagePerform storage (UserDelete ref) `shouldReturn` Left NotFoundError
            storagePerform storage (UserList (ListView 0 1 [FilterUserId ref] []))
                `shouldReturn`
                Right []
    do
        userPasswordList <- Map.elems userTable
        let userList = sortOn userId $ map fst userPasswordList
        parallelFor_ userPasswordList $ \(user, password) -> do
            storagePerform storage (UserList (ListView 0 1 [FilterUserId (userId user)] []))
                `shouldReturn`
                Right [user]
            badPassword <- generate $ randomByteString 16 `suchThat` (/= password)
            storagePerform storage (UserCheckPassword (userId user) (Password badPassword))
                `shouldReturn`
                Left NotFoundError
        storagePerform storage (UserList (ListView 0 maxBound [FilterUserIsAdmin True] []))
            `shouldReturn`
            Right (filter userIsAdmin userList)
        storagePerform storage (UserList (ListView 0 maxBound [FilterUserIsAdmin False] []))
            `shouldReturn`
            Right (filter (not . userIsAdmin) userList)
        storagePerform storage (UserList (ListView 0 maxBound [] [(OrderUserName, Ascending)]))
            `shouldReturn`
            Right (sortOn userName userList)
        storagePerform storage (UserList (ListView 0 maxBound [] [(OrderUserName, Descending)]))
            `shouldReturn`
            Right (sortOn (Down . userName) userList)
        storagePerform storage (UserList (ListView 0 maxBound [] [(OrderUserSurname, Ascending)]))
            `shouldReturn`
            Right (sortOn userSurname userList)
        storagePerform storage (UserList (ListView 0 maxBound [] [(OrderUserJoinDate, Ascending)]))
            `shouldReturn`
            Right (sortOn (Down . userJoinDate) userList)
        storagePerform storage (UserList (ListView 0 maxBound [] [(OrderUserIsAdmin, Ascending), (OrderUserName, Descending)]))
            `shouldReturn`
            Right (sortOn (\u -> (Down (userIsAdmin u), Down (userName u))) userList)
    return ()

generateAccessKeys
    :: Storage -> Int
    -> Map.Map (Reference User) (User, BS.ByteString)
    -> IO (Multimap.Multimap (Reference User) AccessKey)
generateAccessKeys storage sampleSize userTable = do
    akeyTable <- Multimap.new
    userRefs <- Map.keys userTable
    parallelFor_ userRefs $ \userRef -> do
        repeatSome_ 1 5 $ do
            akey <- assertRight =<< storagePerform storage (AccessKeyCreate userRef)
            Multimap.insert akeyTable userRef akey
    return akeyTable

minceAccessKeys
    :: Storage -> Int
    -> Map.Map (Reference User) (User, BS.ByteString)
    -> Multimap.Multimap (Reference User) AccessKey
    -> IO ()
minceAccessKeys storage sampleSize userTable akeyTable = do
    validateAccessKeys storage userTable akeyTable
    userRefs <- Map.keys userTable
    do
        usersToClean <- generate $ chooseFrom (sampleSize `div` 5) userRefs
        parallelFor_ usersToClean $ \userRef -> do
            assertRight =<< storagePerform storage (AccessKeyClear userRef)
            Multimap.deleteAll akeyTable userRef
        validateAccessKeys storage userTable akeyTable
    return ()

validateAccessKeys
    :: HasCallStack
    => Storage
    -> Map.Map (Reference User) (User, BS.ByteString)
    -> Multimap.Multimap (Reference User) AccessKey
    -> IO ()
validateAccessKeys storage userTable akeyTable = do
    akeyGroups <- Multimap.toGroupsWith akeyTable userTable
    parallelFor_ akeyGroups $ \(userRef, userAkeys) -> do
        storagePerform storage (AccessKeyList userRef (ListView 0 maxBound [] []))
            `shouldReturn`
            Right (sort $ map accessKeyId $ userAkeys)
        (user, _) <- Map.lookup' userTable userRef
        parallelFor_ userAkeys $ \akey -> do
            storagePerform storage (UserList (ListView 0 1 [FilterUserAccessKey akey] []))
                `shouldReturn`
                Right [user]

generateAuthors
    :: Storage -> Int
    -> IO (Map.Map (Reference Author) Author)
generateAuthors storage sampleSize = do
    authorTable <- Map.new
    parallelFor_ [1 .. sampleSize] $ \_ -> do
        name <- generate $ randomText
        description <- generate $ randomText
        author <- assertRight =<< storagePerform storage (AuthorCreate name description)
        authorName author `shouldBe` name
        authorDescription author `shouldBe` description
        Map.insert authorTable (authorId author) author
    return authorTable

minceAuthors
    :: Storage -> Int
    -> Map.Map (Reference Author) Author
    -> IO ()
minceAuthors storage sampleSize authorTable = do
    validateAuthors storage authorTable
    do
        authorList <- Map.elems authorTable
        toAlter <- generate $ chooseFrom (sampleSize `div` 5) authorList
        parallelFor_ toAlter $ \author -> do
            name <- generate $ randomText
            assertRight =<< storagePerform storage (AuthorSetName (authorId author) name)
            Map.insert authorTable (authorId author) $
                author {authorName = name}
        validateAuthors storage authorTable
    do
        authorList <- Map.elems authorTable
        toAlter <- generate $ chooseFrom (sampleSize `div` 5) authorList
        parallelFor_ toAlter $ \author -> do
            description <- generate $ randomText
            assertRight =<< storagePerform storage (AuthorSetDescription (authorId author) description)
            Map.insert authorTable (authorId author) $
                author {authorDescription = description}
        validateAuthors storage authorTable
    do
        authorList <- Map.elems authorTable
        toDelete <- generate $ chooseFrom (sampleSize `div` 5) authorList
        parallelFor_ toDelete $ \author -> do
            assertRight =<< storagePerform storage (AuthorDelete (authorId author))
            Map.delete authorTable (authorId author)
        validateAuthors storage authorTable
    do
        badRefs <- sampleBadRefs $ Map.member authorTable
        parallelFor_ badRefs $ \ref -> do
            storagePerform storage (AuthorSetName ref "foo") `shouldReturn` Left NotFoundError
            storagePerform storage (AuthorSetDescription ref "bar") `shouldReturn` Left NotFoundError
            storagePerform storage (AuthorDelete ref) `shouldReturn` Left NotFoundError
            storagePerform storage (AuthorList (ListView 0 1 [FilterAuthorId ref] []))
                `shouldReturn`
                Right []
    do
        authorList <- sortOn authorId <$> Map.elems authorTable
        parallelFor_ authorList $ \author -> do
            storagePerform storage (AuthorList (ListView 0 1 [FilterAuthorId (authorId author)] []))
                `shouldReturn`
                Right [author]
        storagePerform storage (AuthorList (ListView 0 maxBound [] [(OrderAuthorName, Ascending)]))
            `shouldReturn`
            Right (sortOn authorName authorList)
    return ()

validateAuthors
    :: HasCallStack
    => Storage
    -> Map.Map (Reference Author) Author
    -> IO ()
validateAuthors storage authorTable = do
    authorList <- sortOn authorId <$> Map.elems authorTable
    storagePerform storage (AuthorList (ListView 0 maxBound [] []))
        `shouldReturn`
        Right authorList

generateUserAuthorRels
    :: Storage -> Int
    -> Map.Map (Reference User) (User, BS.ByteString)
    -> Map.Map (Reference Author) Author
    -> IO (Relmap.Relmap (Reference User) (Reference Author))
generateUserAuthorRels storage sampleSize userTable authorTable = do
    connSet <- Relmap.new
    sampleSubsetProduct sampleSize userTable authorTable $ \userRef authorRef -> do
        assertRight =<< storagePerform storage (AuthorSetOwnership authorRef userRef True)
        Relmap.insert connSet userRef authorRef
    return connSet

minceUserAuthorRels
    :: Storage -> Int
    -> Map.Map (Reference User) (User, BS.ByteString)
    -> Map.Map (Reference Author) Author
    -> Relmap.Relmap (Reference User) (Reference Author)
    -> IO ()
minceUserAuthorRels storage sampleSize userTable authorTable connSet = do
    validateUserAuthorRels storage userTable authorTable connSet
    do
        sampleSubsetProduct sampleSize userTable authorTable $ \userRef authorRef -> do
            conn <- generate $ randomBool
            assertRight =<< storagePerform storage (AuthorSetOwnership authorRef userRef conn)
            if conn
                then Relmap.insert connSet userRef authorRef
                else Relmap.delete connSet userRef authorRef
        validateUserAuthorRels storage userTable authorTable connSet
    do
        userAuthorRels <- Multimap.toList (Relmap.left connSet)
        parallelFor_ userAuthorRels $ \(userRef, authorRef) -> do
            conn <- generate $ randomBool
            assertRight =<< storagePerform storage (AuthorSetOwnership authorRef userRef conn)
            if conn
                then Relmap.insert connSet userRef authorRef
                else Relmap.delete connSet userRef authorRef
        validateUserAuthorRels storage userTable authorTable connSet
    do
        userRefs <- Map.keys userTable
        usersToDelete <- generate $ chooseFrom (sampleSize `div` 5) userRefs
        parallelFor_ usersToDelete $ \userRef -> do
            assertRight =<< storagePerform storage (UserDelete userRef)
            Map.delete userTable userRef
            Relmap.removeLeft connSet userRef
        validateUserAuthorRels storage userTable authorTable connSet
    do
        authorRefs <- Map.keys authorTable
        authorsToDelete <- generate $ chooseFrom (sampleSize `div` 5) authorRefs
        parallelFor_ authorsToDelete $ \authorRef -> do
            assertRight =<< storagePerform storage (AuthorDelete authorRef)
            Map.delete authorTable authorRef
            Relmap.removeRight connSet authorRef
        validateUserAuthorRels storage userTable authorTable connSet
    return ()

validateUserAuthorRels
    :: HasCallStack
    => Storage
    -> Map.Map (Reference User) (User, BS.ByteString)
    -> Map.Map (Reference Author) Author
    -> Relmap.Relmap (Reference User) (Reference Author)
    -> IO ()
validateUserAuthorRels storage userTable authorTable connSet = do
    userAuthorGroups <- Multimap.toGroupsWith (Relmap.left connSet) userTable
    parallelFor_ userAuthorGroups $ \(userRef, userAuthors) -> do
        authorList <- parallelFor (sort userAuthors) $ Map.lookup' authorTable
        storagePerform storage (AuthorList (ListView 0 maxBound [FilterAuthorUserId userRef] []))
            `shouldReturn`
            Right authorList
    authorUserGroups <- Multimap.toGroupsWith (Relmap.right connSet) authorTable
    parallelFor_ authorUserGroups $ \(authorRef, authorUsers) -> do
        userList <- parallelFor (sort authorUsers) $ \userRef -> do
            (user, _) <- Map.lookup' userTable userRef
            return user
        storagePerform storage (UserList (ListView 0 maxBound [FilterUserAuthorId authorRef] []))
            `shouldReturn`
            Right userList

generateCategories
    :: Storage -> Int
    -> IO (Tree.Tree (Reference Category) Text.Text)
generateCategories storage sampleSize = do
    categoryTree <- Tree.new
    parallelFor_ [1 .. sampleSize] $ \_ -> do
        prevCats <- Tree.keys categoryTree
        name <- generate $ randomText
        parent <- generate $ do
            x <- randomWithin 0 (length prevCats)
            y <- randomWithin 0 x
            case y of
                0 -> return $ ""
                _ -> return $ prevCats !! (y - 1)
        category <- assertRight =<< storagePerform storage (CategoryCreate name parent)
        categoryName category `shouldBe` name
        categoryParent category `shouldBe` parent
        Tree.include categoryTree (categoryId category) name parent
    return categoryTree

minceCategories
    :: Storage -> Int
    -> Tree.Tree (Reference Category) Text.Text
    -> IO ()
minceCategories storage sampleSize categoryTree = do
    validateCategories storage categoryTree
    do
        categoryList <- map toCategory <$> Tree.toList categoryTree
        toAlter <- generate $ chooseFrom (sampleSize `div` 5) categoryList
        parallelFor_ toAlter $ \category -> do
            name <- generate $ randomText
            assertRight =<< storagePerform storage (CategorySetName (categoryId category) name)
            Tree.adjust categoryTree (categoryId category) $ \_ -> name
        validateCategories storage categoryTree
    do
        categoryList <- map toCategory <$> Tree.toList categoryTree
        toAlter <- generate $ chooseFrom (sampleSize `div` 5) categoryList
        stableCategories <- map toCategory <$>
            Tree.withoutSubtreesOf categoryTree (map categoryId toAlter)
        parallelFor_ toAlter $ \category -> do
            parent <- generate $ do
                b <- randomBool
                if b
                    then return $ ""
                    else categoryId <$> chooseOne stableCategories
            assertRight =<< storagePerform storage (CategorySetParent (categoryId category) parent)
            _ <- Tree.trySetParent categoryTree (categoryId category) parent
            return ()
        validateCategories storage categoryTree
    do
        categoryList <- map toCategory <$> Tree.toList categoryTree
        toDelete <- generate $ chooseFrom (sampleSize `div` 5) categoryList
        parallelFor_ toDelete $ \category -> do
            assertRight =<< storagePerform storage (CategoryDelete (categoryId category))
            _ <- Tree.exclude categoryTree (categoryId category)
            return ()
        validateCategories storage categoryTree
    do
        badRefs <- sampleBadRefs $ Tree.member categoryTree
        parallelFor_ badRefs $ \ref -> do
            storagePerform storage (CategorySetName ref "foo") `shouldReturn` Left NotFoundError
            storagePerform storage (CategorySetParent ref "") `shouldReturn` Left NotFoundError
            storagePerform storage (CategoryDelete ref) `shouldReturn` Left NotFoundError
            storagePerform storage (CategoryList (ListView 0 1 [FilterCategoryId ref] []))
                `shouldReturn`
                Right []
    do
        categoryList <- map toCategory <$> Tree.toList categoryTree
        parallelFor_ categoryList $ \category -> do
            storagePerform storage (CategoryList (ListView 0 1 [FilterCategoryId (categoryId category)] []))
                `shouldReturn`
                Right [category]
        storagePerform storage (CategoryList (ListView 0 maxBound [] [(OrderCategoryName, Ascending)]))
            `shouldReturn`
            Right (sortOn (\cat -> (categoryName cat, categoryId cat)) categoryList)
        parallelFor_ categoryList $ \category -> do
            strictSubcats <- map toCategory <$> Tree.childrenList categoryTree (categoryId category)
            storagePerform storage (CategoryList (ListView 0 maxBound [FilterCategoryParentId (categoryId category)] []))
                `shouldReturn`
                Right (sortOn categoryId strictSubcats)
            transitiveSubcats <- map toCategory <$> Tree.subtreeList categoryTree (categoryId category)
            storagePerform storage (CategoryList (ListView 0 maxBound [FilterCategoryTransitiveParentId (categoryId category)] []))
                `shouldReturn`
                Right (sortOn categoryId transitiveSubcats)
            storagePerform storage (CategoryList (ListView 0 maxBound [FilterCategoryTransitiveParentId (categoryId category)] [(OrderCategoryName, Ascending)]))
                `shouldReturn`
                Right (sortOn (\cat -> (categoryName cat, categoryId cat)) transitiveSubcats)
            someSubcats <- generate $ chooseFrom 2 transitiveSubcats
            parallelFor_ someSubcats $ \subcat -> do
                storagePerform storage (CategorySetParent (categoryId category) (categoryId subcat))
                    `shouldReturn`
                    Left InvalidRequestError
    return ()

validateCategories
    :: HasCallStack
    => Storage
    -> Tree.Tree (Reference Category) Text.Text
    -> IO ()
validateCategories storage categoryTree = do
    categoryList <- sortOn categoryId . map toCategory <$> Tree.toList categoryTree
    storagePerform storage (CategoryList (ListView 0 maxBound [] []))
        `shouldReturn`
        Right categoryList

toCategory :: (Reference Category, Text.Text, Reference Category) -> Category
toCategory (ref, name, parent) = Category ref name parent

generateArticles
    :: Storage -> Int
    -> Map.Map (Reference Author) Author
    -> Tree.Tree (Reference Category) Text.Text
    -> IO (Map.Map (Reference Article) (Article, Text.Text))
generateArticles storage sampleSize authorTable categoryTree = do
    articleTable <- Map.new
    authorRefs <- Map.keys authorTable
    categoryRefs <- Tree.keys categoryTree
    parallelFor_ [1 .. sampleSize] $ \_ -> do
        authorRef <- generate $ chooseOne authorRefs
        article <- assertRight =<< storagePerform storage (ArticleCreate authorRef)
        articleAuthor article `shouldBe` authorRef
        articleName article `shouldBe` ""
        articlePublicationStatus article `shouldBe` NonPublished
        articleCategory article `shouldBe` ""
        storagePerform storage (ArticleGetText (articleId article)) `shouldReturn` Right ""
        name <- generate $ randomText
        assertRight =<< storagePerform storage (ArticleSetName (articleId article) name)
        text <- generate $ randomText
        version2 <- assertRight =<< storagePerform storage (ArticleSetText (articleId article) (articleVersion article) text)
        category <- byChance 3 5
            (do
                category <- generate $ chooseOne categoryRefs
                assertRight =<< storagePerform storage (ArticleSetCategory (articleId article) category)
                return category)
            (return "")
        pubStatus <- byChance 3 5
            (do
                pubStatus <- generate $ PublishAt <$> randomTime
                assertRight =<< storagePerform storage (ArticleSetPublicationStatus (articleId article) pubStatus)
                return pubStatus)
            (return NonPublished)
        let myArticle = article
                { articleVersion = version2
                , articleName = name
                , articleCategory = category
                , articlePublicationStatus = pubStatus
                }
        Map.insert articleTable (articleId article) (myArticle, text)
    return articleTable

minceArticles
    :: Storage -> Int
    -> Map.Map (Reference User) (User, BS.ByteString)
    -> Map.Map (Reference Author) Author
    -> Relmap.Relmap (Reference User) (Reference Author)
    -> Tree.Tree (Reference Category) Text.Text
    -> Map.Map (Reference Article) (Article, Text.Text)
    -> IO ()
minceArticles storage sampleSize userTable authorTable userAuthorConnSet categoryTree articleTable = do
    validateArticles storage articleTable
    do
        authorRefs <- Map.keys authorTable
        articleList <- Map.elems articleTable
        toAlter <- generate $ chooseFrom (sampleSize `div` 5) articleList
        parallelFor_ toAlter $ \(article, text) -> do
            authorRef <- generate $ chooseOne authorRefs
            assertRight =<< storagePerform storage (ArticleSetAuthor (articleId article) authorRef)
            Map.insert articleTable (articleId article) $
                (article {articleAuthor = authorRef}, text)
        validateArticles storage articleTable
    do
        articleList <- Map.elems articleTable
        toAlter <- generate $ chooseFrom (sampleSize `div` 5) articleList
        parallelFor_ toAlter $ \(article, text) -> do
            name <- generate $ randomText
            assertRight =<< storagePerform storage (ArticleSetName (articleId article) name)
            Map.insert articleTable (articleId article) $
                (article {articleName = name}, text)
        validateArticles storage articleTable
    do
        articleList <- Map.elems articleTable
        toAlter <- generate $ chooseFrom (sampleSize `div` 5) articleList
        parallelFor_ toAlter $ \(article, _) -> do
            text <- generate $ randomText
            version2 <- assertRight =<< storagePerform storage
                (ArticleSetText (articleId article) (articleVersion article) text)
            Map.insert articleTable (articleId article) $
                (article {articleVersion = version2}, text)
        validateArticles storage articleTable
    do
        articleList <- Map.elems articleTable
        toAlter <- generate $ chooseFrom (sampleSize `div` 5) articleList
        parallelFor_ toAlter $ \(article, text) -> do
            pubStatus <- byChance 3 5
                (generate $ PublishAt <$> randomTime)
                (return NonPublished)
            assertRight =<< storagePerform storage (ArticleSetPublicationStatus (articleId article) pubStatus)
            Map.insert articleTable (articleId article) $
                (article {articlePublicationStatus = pubStatus}, text)
        validateArticles storage articleTable
    do
        categoryRefs <- Tree.keys categoryTree
        articleList <- Map.elems articleTable
        toAlter <- generate $ chooseFrom (sampleSize `div` 5) articleList
        parallelFor_ toAlter $ \(article, text) -> do
            category <- byChance 3 5
                (generate $ chooseOne categoryRefs)
                (return "")
            assertRight =<< storagePerform storage (ArticleSetCategory (articleId article) category)
            Map.insert articleTable (articleId article) $
                (article {articleCategory = category}, text)
        validateArticles storage articleTable
    do
        articleList <- Map.elems articleTable
        toDelete <- generate $ chooseFrom (sampleSize `div` 5) articleList
        parallelFor_ toDelete $ \(article, _) -> do
            assertRight =<< storagePerform storage (ArticleDelete (articleId article))
            Map.delete articleTable (articleId article)
        validateArticles storage articleTable
    do
        articleList <- Map.elems articleTable
        parallelFor_ articleList $ \(article, _) -> do
            badVersion <- generate $ randomVersion `suchThat` (/= articleVersion article)
            storagePerform storage (ArticleSetText (articleId article) badVersion "bar") `shouldReturn` Left VersionError
        badRefs <- sampleBadRefs $ Map.member articleTable
        parallelFor_ badRefs $ \ref -> do
            storagePerform storage (ArticleSetAuthor ref "") `shouldReturn` Left NotFoundError
            storagePerform storage (ArticleSetName ref "foo") `shouldReturn` Left NotFoundError
            storagePerform storage (ArticleSetText ref "" "bar") `shouldReturn` Left NotFoundError
            storagePerform storage (ArticleSetCategory ref "") `shouldReturn` Left NotFoundError
            storagePerform storage (ArticleSetPublicationStatus ref NonPublished) `shouldReturn` Left NotFoundError
            storagePerform storage (ArticleDelete ref) `shouldReturn` Left NotFoundError
            storagePerform storage (ArticleList True (ListView 0 1 [FilterArticleId ref] []))
                `shouldReturn`
                Right []
    do
        authorRefs <- Map.keys authorTable
        authorsToDelete <- generate $ chooseFrom (sampleSize `div` 5) authorRefs
        parallelFor_ authorsToDelete $ \authorRef -> do
            assertRight =<< storagePerform storage (AuthorDelete authorRef)
            Map.delete authorTable authorRef
            Relmap.removeRight userAuthorConnSet authorRef
        articleList <- Map.elems articleTable
        parallelFor_ articleList $ \(article, text) -> do
            if articleAuthor article `elem` authorsToDelete
                then case articlePublicationStatus article of
                    PublishAt _ -> Map.insert articleTable (articleId article) $
                        (article {articleAuthor = ""}, text)
                    NonPublished -> Map.delete articleTable (articleId article)
                else return ()
        validateArticles storage articleTable
    do
        categoryList <- map toCategory <$> Tree.toList categoryTree
        categoriesToDelete <- generate $ chooseFrom (sampleSize `div` 5) categoryList
        parallelFor_ categoriesToDelete $ \category -> do
            assertRight =<< storagePerform storage (CategoryDelete (categoryId category))
            Just parentRef <- Tree.exclude categoryTree (categoryId category)
            Map.modify articleTable $ \(article, text) ->
                if articleCategory article == categoryId category
                    then Just (article {articleCategory = parentRef}, text)
                    else Nothing
        validateArticles storage articleTable
    do
        articleList <- sortOn (articleId . fst) <$> Map.elems articleTable
        let (articleInfoList, articleTextList) = unzip articleList
        parallelFor_ articleList $ \(article, text) -> do
            storagePerform storage (ArticleList True (ListView 0 1 [FilterArticleId (articleId article)] []))
                `shouldReturn`
                Right [article]
            storagePerform storage (ArticleList False (ListView 0 1 [FilterArticleId (articleId article)] []))
                `shouldReturn`
                Right [article | articlePublicationStatus article >= PublishAt timeGenBase]
            storagePerform storage (ArticleGetText (articleId article))
                `shouldReturn`
                Right text
        authorRefs <- Map.keys authorTable
        parallelFor_ ("" : authorRefs) $ \authorRef -> do
            storagePerform storage (ArticleList True (ListView 0 maxBound [FilterArticleAuthorId authorRef] []))
                `shouldReturn`
                Right (filter (\article -> articleAuthor article == authorRef) articleInfoList)
        userAuthorGroups <- Multimap.toGroupsWith (Relmap.left userAuthorConnSet) userTable
        parallelFor_ userAuthorGroups $ \(userRef, userAuthors) -> do
            storagePerform storage (ArticleList True (ListView 0 maxBound [FilterArticleUserId userRef] []))
                `shouldReturn`
                Right (filter (\article -> articleAuthor article `elem` userAuthors) articleInfoList)
        authorUserGroups <- Multimap.toGroupsWith (Relmap.right userAuthorConnSet) authorTable
        let orphanAuthors = "" : map fst (filter (null . snd) authorUserGroups)
        storagePerform storage (ArticleList True (ListView 0 maxBound [FilterArticleUserId ""] []))
            `shouldReturn`
            Right (filter (\article -> articleAuthor article `elem` orphanAuthors) articleInfoList)
        categoryRefs <- Tree.keys categoryTree
        parallelFor_ ("" : categoryRefs) $ \categoryRef -> do
            storagePerform storage (ArticleList True (ListView 0 maxBound [FilterArticleCategoryId categoryRef] []))
                `shouldReturn`
                Right (filter (\article -> articleCategory article == categoryRef) articleInfoList)
            case categoryRef of
                "" -> do
                    storagePerform storage (ArticleList True (ListView 0 maxBound [FilterArticleTransitiveCategoryId categoryRef] []))
                        `shouldReturn`
                        Right (filter (\article -> articleCategory article == "") articleInfoList)
                _ -> do
                    subcatRefs <- map (\(k, _, _) -> k) <$> Tree.subtreeList categoryTree categoryRef
                    storagePerform storage (ArticleList True (ListView 0 maxBound [FilterArticleTransitiveCategoryId categoryRef] []))
                        `shouldReturn`
                        Right (filter (\article -> articleCategory article `elem` subcatRefs) articleInfoList)
        let timeA = addUTCTime (86400 * (-500)) timeGenBase
        let timeB = addUTCTime (86400 * (365 * 10 + 500)) timeGenBase
        storagePerform storage (ArticleList True (ListView 0 maxBound [FilterArticlePublishedBefore timeB] []))
            `shouldReturn`
            Right (filter (publishedBefore timeB) articleInfoList)
        storagePerform storage (ArticleList False (ListView 0 maxBound [FilterArticlePublishedBefore timeB] []))
            `shouldReturn`
            Right (filter (publishedBefore timeGenBase) articleInfoList)
        storagePerform storage (ArticleList True (ListView 0 maxBound [FilterArticlePublishedAfter timeA] []))
            `shouldReturn`
            Right (filter (publishedAfter timeA) articleInfoList)
        storagePerform storage (ArticleList False (ListView 0 maxBound [FilterArticlePublishedAfter timeA] []))
            `shouldReturn`
            Right (filter (publishedWithin timeA timeGenBase) articleInfoList)
        storagePerform storage (ArticleList True (ListView 0 maxBound [FilterArticlePublishedAfter timeA, FilterArticlePublishedBefore timeB] []))
            `shouldReturn`
            Right (filter (publishedWithin timeA timeB) articleInfoList)
        storagePerform storage (ArticleList False (ListView 0 maxBound [FilterArticlePublishedAfter timeA, FilterArticlePublishedBefore timeB] []))
            `shouldReturn`
            Right (filter (publishedWithin timeA timeGenBase) articleInfoList)
        storagePerform storage (ArticleList True (ListView 0 maxBound [] [(OrderArticleName, Ascending)]))
            `shouldReturn`
            Right (sortOn articleName articleInfoList)
        storagePerform storage (ArticleList True (ListView 0 maxBound [] [(OrderArticleDate, Ascending)]))
            `shouldReturn`
            Right (sortOn articlePublicationStatus articleInfoList)
        articleAuthorNameList <- parallelFor articleInfoList $ \article -> do
            case articleAuthor article of
                "" -> return Nothing
                _ -> do
                    author <- Map.lookup' authorTable (articleAuthor article)
                    return $ Just $ authorName author
        let articleListByAuthorName = sortOnList articleInfoList articleAuthorNameList
        storagePerform storage (ArticleList True (ListView 0 maxBound [] [(OrderArticleAuthorName, Ascending)]))
            `shouldReturn`
            Right articleListByAuthorName
        articleCategoryNameList <- parallelFor articleInfoList $ \article -> do
            case articleCategory article of
                "" -> return Nothing
                _ -> do
                    (_, name, _) <- Tree.lookup' categoryTree (articleCategory article)
                    return $ Just name
        let articleListByCategoryName = sortOnList articleInfoList articleCategoryNameList
        storagePerform storage (ArticleList True (ListView 0 maxBound [] [(OrderArticleCategoryName, Ascending)]))
            `shouldReturn`
            Right articleListByCategoryName
    return ()
  where
    publishedBefore ta article = articlePublicationStatus article >= PublishAt ta
    publishedAfter ta article = articlePublicationStatus article <= PublishAt ta
    publishedWithin ta tb article = publishedAfter ta article && publishedBefore tb article
    sortOnList as bs = map fst $ sortOn snd $ zip as bs

validateArticles
    :: HasCallStack
    => Storage
    -> Map.Map (Reference Article) (Article, Text.Text)
    -> IO ()
validateArticles storage articleTable = do
    articleList <- sortOn (articleId . fst) <$> Map.elems articleTable
    storagePerform storage (ArticleList True (ListView 0 maxBound [] []))
        `shouldReturn`
        Right (map fst articleList)
    storagePerform storage (ArticleList False (ListView 0 maxBound [] []))
        `shouldReturn`
        Right (filter (\article -> articlePublicationStatus article >= PublishAt timeGenBase) $ map fst articleList)
    parallelFor_ articleList $ \(article, text) -> do
        storagePerform storage (ArticleGetText (articleId article)) `shouldReturn` Right text

generateTags
    :: Storage -> Int
    -> IO (Map.Map (Reference Tag) Tag)
generateTags storage sampleSize = do
    tagTable <- Map.new
    parallelFor_ [1 .. sampleSize] $ \_ -> do
        name <- generate $ randomText
        tag <- assertRight =<< storagePerform storage (TagCreate name)
        tagName tag `shouldBe` name
        Map.insert tagTable (tagId tag) tag
    return tagTable

minceTags
    :: Storage -> Int
    -> Map.Map (Reference Tag) Tag
    -> IO ()
minceTags storage sampleSize tagTable = do
    validateTags storage tagTable
    do
        tagList <- Map.elems tagTable
        toAlter <- generate $ chooseFrom (sampleSize `div` 5) tagList
        parallelFor_ toAlter $ \tag -> do
            name <- generate $ randomText
            assertRight =<< storagePerform storage (TagSetName (tagId tag) name)
            Map.insert tagTable (tagId tag) $
                tag {tagName = name}
        validateTags storage tagTable
    do
        tagList <- Map.elems tagTable
        toDelete <- generate $ chooseFrom (sampleSize `div` 5) tagList
        parallelFor_ toDelete $ \tag -> do
            assertRight =<< storagePerform storage (TagDelete (tagId tag))
            Map.delete tagTable (tagId tag)
        validateTags storage tagTable
    do
        badRefs <- sampleBadRefs $ Map.member tagTable
        parallelFor_ badRefs $ \ref -> do
            storagePerform storage (TagSetName ref "foo") `shouldReturn` Left NotFoundError
            storagePerform storage (TagDelete ref) `shouldReturn` Left NotFoundError
            storagePerform storage (TagList (ListView 0 1 [FilterTagId ref] []))
                `shouldReturn`
                Right []
    do
        tagList <- Map.elems tagTable
        parallelFor_ tagList $ \tag -> do
            storagePerform storage (TagList (ListView 0 1 [FilterTagId (tagId tag)] []))
                `shouldReturn`
                Right [tag]
        storagePerform storage (TagList (ListView 0 maxBound [] [(OrderTagName, Ascending)]))
            `shouldReturn`
            Right (sortOn (\tag -> (tagName tag, tagId tag)) tagList)
    return ()

validateTags
    :: HasCallStack
    => Storage
    -> Map.Map (Reference Tag) Tag
    -> IO ()
validateTags storage tagTable = do
    tagList <- sortOn tagId <$> Map.elems tagTable
    storagePerform storage (TagList (ListView 0 maxBound [] []))
        `shouldReturn`
        Right tagList

generateArticleTagRels
    :: Storage -> Int
    -> Map.Map (Reference Article) (Article, Text.Text)
    -> Map.Map (Reference Tag) Tag
    -> IO (Relmap.Relmap (Reference Article) (Reference Tag))
generateArticleTagRels storage sampleSize articleTable tagTable = do
    connSet <- Relmap.new
    sampleSubsetProduct sampleSize articleTable tagTable $ \articleRef tagRef -> do
        assertRight =<< storagePerform storage (ArticleSetTag articleRef tagRef True)
        Relmap.insert connSet articleRef tagRef
    return connSet

minceArticleTagRels
    :: Storage -> Int
    -> Map.Map (Reference Article) (Article, Text.Text)
    -> Map.Map (Reference Tag) Tag
    -> Relmap.Relmap (Reference Article) (Reference Tag)
    -> IO ()
minceArticleTagRels storage sampleSize articleTable tagTable connSet = do
    validateArticleTagRels storage articleTable tagTable connSet
    do
        sampleSubsetProduct sampleSize articleTable tagTable $ \articleRef tagRef -> do
            conn <- generate $ randomBool
            assertRight =<< storagePerform storage (ArticleSetTag articleRef tagRef conn)
            if conn
                then Relmap.insert connSet articleRef tagRef
                else Relmap.delete connSet articleRef tagRef
        validateArticleTagRels storage articleTable tagTable connSet
    do
        articleTagRels <- Multimap.toList (Relmap.left connSet)
        parallelFor_ articleTagRels $ \(articleRef, tagRef) -> do
            conn <- generate $ randomBool
            assertRight =<< storagePerform storage (ArticleSetTag articleRef tagRef conn)
            if conn
                then Relmap.insert connSet articleRef tagRef
                else Relmap.delete connSet articleRef tagRef
        validateArticleTagRels storage articleTable tagTable connSet
    do
        articleRefs <- Map.keys articleTable
        articlesToDelete <- generate $ chooseFrom (sampleSize `div` 5) articleRefs
        parallelFor_ articlesToDelete $ \articleRef -> do
            assertRight =<< storagePerform storage (ArticleDelete articleRef)
            Map.delete articleTable articleRef
            Relmap.removeLeft connSet articleRef
        validateArticleTagRels storage articleTable tagTable connSet
    do
        tagRefs <- Map.keys tagTable
        tagsToDelete <- generate $ chooseFrom (sampleSize `div` 5) tagRefs
        parallelFor_ tagsToDelete $ \tagRef -> do
            assertRight =<< storagePerform storage (TagDelete tagRef)
            Map.delete tagTable tagRef
            Relmap.removeRight connSet tagRef
        validateArticleTagRels storage articleTable tagTable connSet
    do
        tagRefs <- Map.keys tagTable
        parallelFor_ [1..sampleSize] $ \_ -> do
            tags <- generate $ do
                n <- randomWithin 1 (sampleSize `div` 5)
                chooseFrom n tagRefs
            articleGroups <- forM tags $ Multimap.lookupAll (Relmap.right connSet)
            let articleRefsUnion = foldl1' union articleGroups
            articlesUnion <- parallelFor (sort articleRefsUnion) $ \aref -> do
                fst <$> Map.lookup' articleTable aref
            storagePerform storage (ArticleList True (ListView 0 maxBound (tagSumFilter tags) []))
                `shouldReturn`
                Right articlesUnion
            let articleRefsIntersection = foldl1' intersect articleGroups
            articlesIntersection <- parallelFor (sort articleRefsIntersection) $ \aref -> do
                fst <$> Map.lookup' articleTable aref
            storagePerform storage (ArticleList True (ListView 0 maxBound (tagProductFilter tags) []))
                `shouldReturn`
                Right articlesIntersection
    return ()
  where
    tagSumFilter tags = [FilterArticleTagIds tags]
    tagProductFilter tags = map (\tag -> FilterArticleTagIds [tag]) tags

validateArticleTagRels
    :: HasCallStack
    => Storage
    -> Map.Map (Reference Article) (Article, Text.Text)
    -> Map.Map (Reference Tag) Tag
    -> Relmap.Relmap (Reference Article) (Reference Tag)
    -> IO ()
validateArticleTagRels storage articleTable tagTable connSet = do
    articleTagGroups <- Multimap.toGroupsWith (Relmap.left connSet) articleTable
    parallelFor_ articleTagGroups $ \(articleRef, articleTags) -> do
        tagList <- parallelFor (sort articleTags) $ Map.lookup' tagTable
        storagePerform storage (TagList (ListView 0 maxBound [FilterTagArticleId articleRef] []))
            `shouldReturn`
            Right tagList
    tagArticleGroups <- Multimap.toGroupsWith (Relmap.right connSet) tagTable
    parallelFor_ tagArticleGroups $ \(tagRef, tagArticles) -> do
        articleList <- parallelFor (sort tagArticles) $ \aref -> do
            fst <$> Map.lookup' articleTable aref
        storagePerform storage (ArticleList True (ListView 0 maxBound [FilterArticleTagIds [tagRef]] []))
            `shouldReturn`
            Right articleList

generateComments
    :: Storage -> Int
    -> Map.Map (Reference User) (User, BS.ByteString)
    -> Map.Map (Reference Article) (Article, Text.Text)
    -> IO (Map.Map (Reference Comment) Comment)
generateComments storage sampleSize userTable articleTable = do
    commentTable <- Map.new
    sampleSubsetProduct sampleSize userTable articleTable $ \userRef articleRef -> do
        text <- generate $ randomText
        comment <- assertRight =<< storagePerform storage (CommentCreate articleRef userRef text)
        commentArticle comment `shouldBe` articleRef
        commentUser comment `shouldBe` userRef
        commentText comment `shouldBe` text
        commentEditDate comment `shouldBe` Nothing
        Map.insert commentTable (commentId comment) comment
    return commentTable

minceComments
    :: Storage -> Int
    -> Map.Map (Reference User) (User, BS.ByteString)
    -> Map.Map (Reference Article) (Article, Text.Text)
    -> Map.Map (Reference Comment) Comment
    -> IO ()
minceComments storage sampleSize userTable articleTable commentTable = do
    validateComments storage commentTable
    do
        commentList <- Map.elems commentTable
        toAlter <- generate $ chooseFrom (sampleSize `div` 5) commentList
        parallelFor_ toAlter $ \comment -> do
            text <- generate $ randomText
            assertRight =<< storagePerform storage (CommentSetText (commentId comment) text)
            Right [comment2] <- storagePerform storage (CommentList (ListView 0 1 [FilterCommentId (commentId comment)] []))
            commentId comment2 `shouldBe` commentId comment
            commentArticle comment2 `shouldBe` commentArticle comment
            commentUser comment2 `shouldBe` commentUser comment
            commentText comment2 `shouldBe` text
            commentDate comment2 `shouldBe` commentDate comment
            commentEditDate comment2 `shouldSatisfy` isJust
            commentEditDate comment2 `shouldSatisfy` \(Just editDate) -> editDate >= commentDate comment
            Map.insert commentTable (commentId comment) comment2
        validateComments storage commentTable
    do
        commentList <- Map.elems commentTable
        toDelete <- generate $ chooseFrom (sampleSize `div` 5) commentList
        parallelFor_ toDelete $ \comment -> do
            assertRight =<< storagePerform storage (CommentDelete (commentId comment))
            Map.delete commentTable (commentId comment)
        validateComments storage commentTable
    do
        userRefs <- Map.keys userTable
        usersToDelete <- generate $ chooseFrom (sampleSize `div` 5) userRefs
        parallelFor_ usersToDelete $ \userRef -> do
            assertRight =<< storagePerform storage (UserDelete userRef)
            Map.delete userTable userRef
        commentList <- Map.elems commentTable
        parallelFor_ commentList $ \comment -> do
            if commentUser comment `elem` usersToDelete
                then Map.insert commentTable (commentId comment) $ comment {commentUser = ""}
                else return ()
        validateComments storage commentTable
    do
        articleRefs <- Map.keys articleTable
        articlesToDelete <- generate $ chooseFrom (sampleSize `div` 5) articleRefs
        parallelFor_ articlesToDelete $ \articleRef -> do
            assertRight =<< storagePerform storage (ArticleDelete articleRef)
            Map.delete articleTable articleRef
        commentList <- Map.elems commentTable
        parallelFor_ commentList $ \comment -> do
            if commentArticle comment `elem` articlesToDelete
                then Map.delete commentTable (commentId comment)
                else return ()
        validateComments storage commentTable
    do
        badRefs <- sampleBadRefs $ Map.member commentTable
        parallelFor_ badRefs $ \ref -> do
            storagePerform storage (CommentSetText ref "foo") `shouldReturn` Left NotFoundError
            storagePerform storage (CommentDelete ref) `shouldReturn` Left NotFoundError
            storagePerform storage (CommentList (ListView 0 1 [FilterCommentId ref] []))
                `shouldReturn`
                Right []
    do
        userRefs <- Map.keys userTable
        articleRefs <- Map.keys articleTable
        commentList <- sortOn commentId <$> Map.elems commentTable
        parallelFor_ commentList $ \comment -> do
            storagePerform storage (CommentList (ListView 0 1 [FilterCommentId (commentId comment)] []))
                `shouldReturn`
                Right [comment]
        parallelFor_ ("" : userRefs) $ \userRef -> do
            storagePerform storage (CommentList (ListView 0 maxBound [FilterCommentUserId userRef] []))
                `shouldReturn`
                Right (filter (isOfUser userRef) commentList)
            storagePerform storage (CommentList (ListView 0 maxBound [FilterCommentUserId userRef] [(OrderCommentDate, Ascending)]))
                `shouldReturn`
                Right (sortOn (Down . commentDate) $ filter (isOfUser userRef) commentList)
        parallelFor_ articleRefs $ \articleRef -> do
            storagePerform storage (CommentList (ListView 0 maxBound [FilterCommentArticleId articleRef] []))
                `shouldReturn`
                Right (filter (isOfArticle articleRef) commentList)
            storagePerform storage (CommentList (ListView 0 maxBound [FilterCommentArticleId articleRef] [(OrderCommentDate, Ascending)]))
                `shouldReturn`
                Right (sortOn (Down . commentDate) $ filter (isOfArticle articleRef) commentList)
    return ()
  where
    isOfUser userRef comment = commentUser comment == userRef
    isOfArticle articleRef comment = commentArticle comment == articleRef

validateComments
    :: HasCallStack
    => Storage
    -> Map.Map (Reference Comment) Comment
    -> IO ()
validateComments storage commentTable = do
    commentList <- sortOn commentId <$> Map.elems commentTable
    storagePerform storage (CommentList (ListView 0 maxBound [] []))
        `shouldReturn`
        Right commentList

generateFiles
    :: Storage -> Int
    -> Map.Map (Reference User) (User, BS.ByteString)
    -> Map.Map (Reference Article) (Article, Text.Text)
    -> IO (Map.Map (Reference FileInfo) (FileInfo, LBS.ByteString))
generateFiles storage sampleSize userTable articleTable = do
    fileTable <- Map.new
    sampleSubsetProduct sampleSize userTable articleTable $ \userRef articleRef -> do
        name <- generate randomText
        mimeType <- generate randomText
        chunks <- do
            chunkCount <- generate $ randomWithin 1 sampleSize
            replicateM chunkCount $ do
                len <- generate $ randomWithin 1 sampleSize
                generate $ randomByteString len
        finfo <- assertRight =<< storageUpload storage name mimeType articleRef userRef (uploader chunks)
        fileName finfo `shouldBe` name
        fileMimeType finfo `shouldBe` mimeType
        fileArticle finfo `shouldBe` articleRef
        fileUser finfo `shouldBe` userRef
        Map.insert fileTable (fileId finfo) $
            (finfo, LBS.fromChunks chunks)
    return fileTable
  where
    uploader [] finfo = return $ UploadFinish finfo
    uploader (x : xs) finfo = return $ UploadChunk x $ uploader xs finfo

minceFiles
    :: Storage -> Int
    -> Map.Map (Reference User) (User, BS.ByteString)
    -> Map.Map (Reference Article) (Article, Text.Text)
    -> Map.Map (Reference FileInfo) (FileInfo, LBS.ByteString)
    -> IO ()
minceFiles storage sampleSize userTable articleTable fileTable = do
    do
        userRef <- generate . chooseOne =<< Map.keys userTable
        articleRef <- generate . chooseOne =<< Map.keys articleTable
        uret <- storageUpload storage "foo" "bar" articleRef userRef $ \_ -> do
            return $ UploadChunk "baz" $ do
                return $ UploadAbort "quux"
        uret `shouldBe` Right "quux"
        validateFiles storage fileTable
    do
        fileList <- Map.elems fileTable
        toAlter <- generate $ chooseFrom (sampleSize `div` 5) fileList
        parallelFor_ toAlter $ \(finfo, content) -> do
            name <- generate randomText
            assertRight =<< storagePerform storage (FileSetName (fileId finfo) name)
            Map.insert fileTable (fileId finfo) $
                (finfo {fileName = name}, content)
        validateFiles storage fileTable
    do
        fileList <- Map.elems fileTable
        toAlter <- generate $ chooseFrom (sampleSize `div` 5) fileList
        parallelFor_ toAlter $ \(finfo, content) -> do
            index <- generate $ do
                b <- randomBool
                if b
                    then random
                    else return 0
            assertRight =<< storagePerform storage (FileSetIndex (fileId finfo) index)
            Map.insert fileTable (fileId finfo) $
                (finfo {fileIndex = index}, content)
    do
        fileList <- Map.elems fileTable
        toDelete <- generate $ chooseFrom (sampleSize `div` 5) fileList
        parallelFor_ toDelete $ \(finfo, _) -> do
            assertRight =<< storagePerform storage (FileDelete (fileId finfo))
            Map.delete fileTable (fileId finfo)
        validateFiles storage fileTable
    do
        userRefs <- Map.keys userTable
        usersToDelete <- generate $ chooseFrom (sampleSize `div` 5) userRefs
        parallelFor_ usersToDelete $ \userRef -> do
            assertRight =<< storagePerform storage (UserDelete userRef)
            Map.delete userTable userRef
        fileList <- Map.elems fileTable
        parallelFor_ fileList $ \(finfo, content) -> do
            if fileUser finfo `elem` usersToDelete
                then Map.insert fileTable (fileId finfo) $
                    (finfo {fileUser = ""}, content)
                else return ()
        validateFiles storage fileTable
    do
        articleRefs <- Map.keys articleTable
        articlesToDelete <- generate $ chooseFrom (sampleSize `div` 5) articleRefs
        parallelFor_ articlesToDelete $ \articleRef -> do
            assertRight =<< storagePerform storage (ArticleDelete articleRef)
            Map.delete articleTable articleRef
        fileList <- Map.elems fileTable
        parallelFor_ fileList $ \(finfo, _) -> do
            if fileArticle finfo `elem` articlesToDelete
                then Map.delete fileTable (fileId finfo)
                else return ()
        validateFiles storage fileTable
    do
        badRefs <- sampleBadRefs $ Map.member fileTable
        parallelFor_ badRefs $ \ref -> do
            storagePerform storage (FileSetName ref "foo") `shouldReturn` Left NotFoundError
            storagePerform storage (FileSetIndex ref 15) `shouldReturn` Left NotFoundError
            storagePerform storage (FileDelete ref) `shouldReturn` Left NotFoundError
            storagePerform storage (FileList (ListView 0 1 [FilterFileId ref] []))
                `shouldReturn`
                Right []
            downloadFile storage ref `shouldReturn` Left NotFoundError
    do
        userRefs <- Map.keys userTable
        articleRefs <- Map.keys articleTable
        fileList <- sortOn (fileId . fst) <$> Map.elems fileTable
        parallelFor_ fileList $ \(finfo, content) -> do
            storagePerform storage (FileList (ListView 0 1 [FilterFileId (fileId finfo)] []))
                `shouldReturn`
                Right [finfo]
            downloadFile storage (fileId finfo) `shouldReturn` Right (fileMimeType finfo, content)
        parallelFor_ ("" : userRefs) $ \userRef -> do
            let userFiles = filter (\finfo -> fileUser finfo == userRef) $ map fst fileList
            storagePerform storage (FileList (ListView 0 maxBound [FilterFileUserId userRef] []))
                `shouldReturn`
                Right userFiles
            storagePerform storage (FileList (ListView 0 maxBound [FilterFileUserId userRef] [(OrderFileUploadDate, Ascending)]))
                `shouldReturn`
                Right (sortOn (Down . fileUploadDate) userFiles)
        parallelFor_ articleRefs $ \articleRef -> do
            let articleFiles = filter (\finfo -> fileArticle finfo == articleRef) $ map fst fileList
            storagePerform storage (FileList (ListView 0 maxBound [FilterFileArticleId articleRef] []))
                `shouldReturn`
                Right articleFiles
            storagePerform storage (FileList (ListView 0 maxBound [FilterFileArticleId articleRef] [(OrderFileIndex, Ascending)]))
                `shouldReturn`
                Right (sortOn fileIndex articleFiles)
        storagePerform storage (FileList (ListView 0 maxBound [] [(OrderFileName, Ascending)]))
            `shouldReturn`
            Right (sortOn fileName $ map fst fileList)
    return ()

validateFiles
    :: HasCallStack
    => Storage
    -> Map.Map (Reference FileInfo) (FileInfo, LBS.ByteString)
    -> IO ()
validateFiles storage fileTable = do
    fileList <- Map.elems fileTable
    let finfoList = sortOn fileId $ map fst fileList
    storagePerform storage (FileList (ListView 0 maxBound [] []))
        `shouldReturn`
        Right finfoList

downloadFile :: Storage -> Reference FileInfo -> IO (Either StorageError (Text.Text, LBS.ByteString))
downloadFile storage fileRef = do
    storageDownload storage fileRef onError onStream
  where
    onError err = return $ Left err
    onStream fsize ftype inner = do
        buf <- newIORef mempty
        inner $ \chunk -> modifyIORef' buf (<> Builder.byteString chunk)
        content <- Builder.toLazyByteString <$> readIORef buf
        fsize `shouldBe` LBS.length content
        return $ Right (ftype, content)
