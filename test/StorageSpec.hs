module StorageSpec
    ( spec
    ) where

import Control.Concurrent
import Control.Concurrent.MVar
import Control.Concurrent.QSemN
import Control.Exception
import Control.Monad
import Data.Aeson hiding (Result)
import Data.Int
import Data.List
import Data.Maybe
import Data.Time.Clock
import Data.Yaml
import System.IO.Unsafe
import Test.Hspec
import qualified Data.ByteString as BS
import qualified Data.Char as Char
import qualified Data.Map as Map
import qualified Data.Text as Text
import qualified System.IO as IO
import Gen
import Sql.Query
import Tuple
import qualified Logger
import qualified Sql.Database as Db
import qualified Sql.Database.Config as Db
import Storage
import Storage.Model

data TestConfig = TestConfig
    { databaseConfig :: Db.Config
    , confSampleSize :: Int
    }

instance FromJSON TestConfig where
    parseJSON = withObject "TestConfig" $ \v -> do
        TestConfig
            <$> v .: "database"
            <*> v .: "sample-size"

clearDatabase :: Db.Handle -> IO ()
clearDatabase db = do
    mapM_ (Db.queryMaybe db . DropTable) $ reverse $
        [ "sn_metadata"
        , "sn_users"
        , "sn_access_keys"
        , "sn_authors"
        , "sn_author2user"
        , "sn_articles"
        , "sn_files"
        , "sn_file_chunks"
        ]

randomText :: Gen Text.Text
randomText = fmap Text.pack $ randomPrintableString =<< randomWithin 4 32

randomByteString :: Int -> Gen BS.ByteString
randomByteString n = BS.pack <$> replicateM n random

dateTimeGenBase :: UTCTime
dateTimeGenBase = case unsafePerformIO getCurrentTime of
    UTCTime day _ -> UTCTime day 0
{-# NOINLINE dateTimeGenBase #-}

randomDateTime :: Gen UTCTime
randomDateTime = do
    seconds <- randomWithin (-86400*1000) (-86400)
    epoch <- randomWithin 0 1
    let delta = fromInteger seconds + nominalDay * (fromInteger (epoch * 365 * 10))
    return $ addUTCTime delta dateTimeGenBase

randomReference :: Gen (Reference a)
randomReference = Reference <$> randomByteString 16

randomVersion :: Gen (Version a)
randomVersion = Version <$> randomByteString 16

randomUser :: Gen User
randomUser = User <$> randomText <*> randomText <*> randomDateTime <*> randomBool

randomAuthor :: Gen Author
randomAuthor = Author <$> randomText <*> randomText

randomPublicationStatus :: Gen PublicationStatus
randomPublicationStatus = do
    i <- randomWithin 1 3 :: Gen Int
    if i <= 1
        then return NonPublished
        else PublishAt <$> randomDateTime

randomArticleOf :: Reference Author -> Gen Article
randomArticleOf authorRef = Article authorRef <$> randomText <*> randomText <*> randomPublicationStatus

parallelFor :: [a] -> (a -> IO b) -> IO [b]
parallelFor source act = do
    threads <- forM source $ \x -> do
        me <- newEmptyMVar
        tid <- forkFinally
            (yield >> act x)
            (putMVar me)
        return (me, tid)
    forM threads $ \(mvar, _) -> do
        outcome <- takeMVar mvar
        either throwIO return outcome

parallelFor_ :: [a] -> (a -> IO ()) -> IO ()
parallelFor_ source act = parallelFor source act >> return ()

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

convertTimeChar :: Char -> Char
convertTimeChar ':' = '-'
convertTimeChar '.' = '-'
convertTimeChar c = c

spec :: Spec
spec = do
    beforeAll (decodeFileThrow "test-config.yaml" :: IO [TestConfig]) $ do
        describe "Storage" $ do
            it "handles objects" $ \testconfList -> do
                forM_ testconfList $ \testconf -> do
                    time <- getCurrentTime
                    let tstr = map convertTimeChar $ show time
                    Logger.withFileLogger (".testlogs/" ++ tstr ++ ".log") $ \logger -> do
                        Db.withDatabase (databaseConfig testconf) logger $ \db -> do
                            clearDatabase db
                            upgradeSchema logger db `shouldReturn` Right ()
                            withSqlStorage logger db
                                (expectationFailure . show)
                                $ \storage -> do
                                    let sampleSize = confSampleSize testconf
                                    model <- newStorageModel
                                    testCrud storage model sampleSize randomUser
                                        spawnUser getUser setUser deleteUser
                                        modelSetUser modelDeleteUser modelListUsers
                                        verifyUsers
                                    testAccessKeys storage model sampleSize
                                    testCrud storage model sampleSize randomAuthor
                                        spawnAuthor getAuthor setAuthor deleteAuthor
                                        modelSetAuthor modelDeleteAuthor modelListAuthors
                                        verifyAuthors
                                    testAuthorUserConns storage model sampleSize
                                    testArticles storage model sampleSize
                                    return ()

testCrud
    :: (Show obj, Eq obj)
    => Handle
    -> Model
    -> Int
    -> Gen obj
    -> (Handle -> obj -> IO (Result (Reference obj)))
    -> (Handle -> Reference obj -> IO (Result obj))
    -> (Handle -> Reference obj -> obj -> IO (Result ()))
    -> (Handle -> Reference obj -> IO (Result ()))
    -> (Model -> Reference obj -> obj -> IO ())
    -> (Model -> Reference obj -> IO ())
    -> (Model -> IO [(Reference obj, obj)])
    -> (Model -> Handle -> IO ())
    -> IO ()
testCrud storage model sampleSize randomObject
        spawnObject getObject setObject deleteObject
        modelSet modelDelete modelList
        verifyObjects = do
    do
        parallelFor_ [1 .. sampleSize] $ \_ -> do
            obj <- generate $ randomObject
            ref <- assertOk =<< spawnObject storage obj
            modelSet model ref obj
        verifyObjects model storage
    do
        parallelFor_ [1 .. sampleSize] $ \_ -> do
            obj <- generate $ randomObject
            ref <- assertOk =<< spawnObject storage obj
            modelSet model ref obj
        verifyObjects model storage
    do
        objList <- modelList model
        toAlter <- generate $ chooseFrom (sampleSize `div` 5) objList
        parallelFor_ toAlter $ \(ref, _) -> do
            obj <- generate $ randomObject
            assertOk =<< setObject storage ref obj
            modelSet model ref obj
        verifyObjects model storage
    do
        objList <- modelList model
        toDelete <- generate $ chooseFrom (sampleSize `div` 5) objList
        parallelFor_ toDelete $ \(ref, _) -> do
            assertOk =<< deleteObject storage ref
            modelDelete model ref
        verifyObjects model storage
    do
        objList <- modelList model
        parallelFor_ objList $ \(ref, objA) -> do
            objB <- assertOk =<< getObject storage ref
            objB `shouldBe` objA
        let refList = map fst objList
        badRefs <- generate $
            replicateM 20 $
                randomReference `suchThat` (`notElem` refList)
        parallelFor_ badRefs $ \ref -> do
            obj <- generate $ randomObject
            getObject storage ref `shouldReturn` NotFoundError
            setObject storage ref obj `shouldReturn` NotFoundError
            deleteObject storage ref `shouldReturn` NotFoundError

testAccessKeys :: Handle -> Model -> Int -> IO ()
testAccessKeys storage model sampleSize = do
    users <- modelListUsers model
    let userRefs = map fst users
    do
        parallelFor_ userRefs $ \ref -> do
            repeatSome_ 1 3 $ do
                akey <- assertOk =<< spawnAccessKey storage ref
                modelSetAccessKey model ref akey
        verifyAccessKeys model storage
    do
        userKeys <- modelListAccessKeys model
        let allKeys = do
                (userRef, akeys) <- userKeys
                key <- akeys
                [(userRef, key)]
        toDelete <- generate $ chooseFrom (sampleSize `div` 5) allKeys
        parallelFor_ toDelete $ \(userRef, akey) -> do
            assertOk =<< deleteAccessKey storage userRef (accessKeyId akey)
            modelDeleteAccessKey model userRef akey
        verifyAccessKeys model storage
    do
        parallelFor_ userRefs $ \ref -> do
            repeatSome_ 1 3 $ do
                akey <- assertOk =<< spawnAccessKey storage ref
                modelSetAccessKey model ref akey
        verifyAccessKeys model storage
    do
        userKeys <- modelListAccessKeys model
        let oneKeys = do
                (userRef, oneKey:_) <- userKeys
                [(userRef, oneKey)]
        let (users, keys) = unzip oneKeys
        let badKeys = zip users (drop 1 keys)
        toDeleteBad <- generate $ chooseFrom (sampleSize `div` 5) badKeys
        parallelFor_ toDeleteBad $ \(userRef, akey) -> do
            deleteAccessKey storage userRef (accessKeyId akey) `shouldReturn` NotFoundError
        verifyAccessKeys model storage
    return ()

testAuthorUserConns :: Handle -> Model -> Int -> IO ()
testAuthorUserConns storage model sampleSize = do
    userBoxes <- modelListUsers model
    authorBoxes <- modelListAuthors model
    let userRefs = map fst userBoxes
    let authorRefs = map fst authorBoxes
    do
        parallelFor_ authorRefs $ \authorRef -> do
            myUsers <- generate $ randomWithin 1 3 >>= \n -> chooseFrom n userRefs
            parallelFor_ myUsers $ \userRef -> do
                assertOk =<< connectUserAuthor storage userRef authorRef
                byChance 1 5
                    (assertOk =<< connectUserAuthor storage userRef authorRef)
                    (return ())
                modelConnectUserAuthor model userRef authorRef
        verifyUserAuthorConns model storage
    do
        parallelFor_ authorRefs $ \authorRef -> do
            myUserBoxes <- modelListUsersOfAuthor model authorRef
            let myUsers = map fst myUserBoxes
            parallelFor_ myUsers $ \userRef -> do
                byChance 1 5
                    (do
                        assertOk =<< disconnectUserAuthor storage userRef authorRef
                        byChance 1 5
                            (assertOk =<< disconnectUserAuthor storage userRef authorRef)
                            (return ())
                        modelDisconnectUserAuthor model userRef authorRef)
                    (return ())
        verifyUserAuthorConns model storage
    do
        usersToDelete <- generate $ chooseFrom (sampleSize `div` 5) userRefs
        parallelFor_ usersToDelete $ \userRef -> do
            assertOk =<< deleteUser storage userRef
            modelDeleteUser model userRef
        verifyUserAuthorConns model storage
    do
        authorsToDelete <- generate $ chooseFrom (sampleSize `div` 5) authorRefs
        parallelFor_ authorsToDelete $ \authorRef -> do
            assertOk =<< deleteAuthor storage authorRef
            modelDeleteAuthor model authorRef
        verifyUserAuthorConns model storage
    return ()

testArticles :: Handle -> Model -> Int -> IO ()
testArticles storage model sampleSize = do
    authorBoxes <- modelListAuthors model
    let authorRefs = map fst authorBoxes
    do
        parallelFor_ [1 .. sampleSize] $ \_ -> do
            [authorRef] <- generate $ chooseFrom 1 authorRefs
            article <- generate $ randomArticleOf authorRef
            (articleRef, articleVersion) <- assertOk =<< spawnArticle storage article
            modelSetArticle model articleRef article articleVersion
        verifyArticles model storage (PublishAt dateTimeGenBase)
    do
        parallelFor_ [1 .. sampleSize] $ \_ -> do
            [authorRef] <- generate $ chooseFrom 1 authorRefs
            article <- generate $ randomArticleOf authorRef
            (articleRef, articleVersion) <- assertOk =<< spawnArticle storage article
            modelSetArticle model articleRef article articleVersion
        verifyArticles model storage (PublishAt dateTimeGenBase)
    do
        articleList <- modelListArticleVersions model
        toAlter <- generate $ chooseFrom (sampleSize `div` 5) articleList
        parallelFor_ toAlter $ \(articleRef, oldArticle, oldVersion) -> do
            [authorRef] <- byChance 1 5
                (generate $ chooseFrom 1 authorRefs)
                (return [articleAuthor oldArticle])
            newArticle <- generate $ randomArticleOf authorRef
            newVersion <- assertOk =<< setArticle storage articleRef newArticle oldVersion
            modelSetArticle model articleRef newArticle newVersion
        verifyArticles model storage (PublishAt dateTimeGenBase)
    do
        articleList <- modelListArticleVersions model
        toDelete <- generate $ chooseFrom (sampleSize `div` 5) articleList
        parallelFor_ toDelete $ \(articleRef, _, _) -> do
            assertOk =<< deleteArticle storage articleRef
            modelDeleteArticle model articleRef
        verifyArticles model storage (PublishAt dateTimeGenBase)
    do
        articleList <- modelListArticleVersions model
        parallelFor_ articleList $ \(articleRef, articleA, articleVersionA) -> do
            (articleB, articleVersionB) <- assertOk =<< getArticle storage articleRef
            articleB `shouldBe` articleA
            articleVersionB `shouldBe` articleVersionA
            badVersion <- generate $ randomVersion `suchThat` (/= articleVersionA)
            setArticle storage articleRef articleA badVersion `shouldReturn` NotFoundError
        let refList = map (\(ref, _, _) -> ref) articleList
        badRefs <- generate $
            replicateM 20 $
                randomReference `suchThat` (`notElem` refList)
        parallelFor_ badRefs $ \ref -> do
            [authorRef] <- generate $ chooseFrom 1 authorRefs
            article <- generate $ randomArticleOf authorRef
            version <- generate $ randomVersion
            getArticle storage ref `shouldReturn` NotFoundError
            setArticle storage ref article version `shouldReturn` NotFoundError
            deleteArticle storage ref `shouldReturn` NotFoundError
    do
        authorsToDelete <- generate $ chooseFrom (sampleSize `div` 5) authorRefs
        parallelFor_ authorsToDelete $ \authorRef -> do
            assertOk =<< deleteAuthor storage authorRef
            modelDeleteAuthor model authorRef
        verifyArticles model storage (PublishAt dateTimeGenBase)
    return ()
