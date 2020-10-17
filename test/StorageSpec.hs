module StorageSpec
    ( spec
    ) where

import Control.Concurrent
import Control.Concurrent.MVar
import Control.Concurrent.QSemN
import Control.Exception
import Control.Monad
import Data.Aeson
import Data.Int
import Data.Maybe
import Data.Time.Clock
import Data.Yaml
import System.IO.Unsafe
import System.Random.Stateful
import Test.Hspec
import qualified Data.Char as Char
import qualified Data.Map as Map
import qualified Data.Text as Text
import Gen
import Sql.Query
import Tuple
import qualified Logger
import qualified Sql.Database as Db
import qualified Sql.Database.Config as Db
import qualified Storage

data TestConfig = TestConfig
    { databaseConfig :: Db.Config
    , sampleSize :: Int
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
    return ()

randomText :: Gen Text.Text
randomText = fmap Text.pack $ randomPrintableString =<< randomWithin 4 32

dateTimeGenBase :: UTCTime
dateTimeGenBase = case unsafePerformIO getCurrentTime of
    UTCTime day _ -> UTCTime day 0

randomDateTime :: Gen UTCTime
randomDateTime = do
    seconds <- randomWithin (-86400*1000) 0
    return $ addUTCTime (fromInteger seconds) dateTimeGenBase

randomUser :: Gen Storage.User
randomUser = Storage.User <$> randomText <*> randomText <*> randomDateTime <*> randomBool

randomAuthor :: Gen Storage.Author
randomAuthor = Storage.Author <$> randomText <*> randomText

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

assertJust :: HasCallStack => Maybe a -> IO a
assertJust (Just x) = return x
assertJust Nothing = expectationFailure "Just expected, got Nothing" >> undefined

spec :: Spec
spec = do
    beforeAll (decodeFileThrow "test-config.yaml" :: IO [TestConfig]) $ do
        describe "Storage" $ do
            it "handles objects" $ \testconfList -> do
                forM_ testconfList $ \testconf -> do
                    Logger.withTestLogger $ \logger -> do
                        Db.withDatabase (databaseConfig testconf) logger $ \db -> do
                            clearDatabase db
                            Storage.upgradeSchema logger db `shouldReturn` Right ()
                            Storage.withSqlStorage logger db
                                (expectationFailure . show)
                                $ \storage -> do
                                    let totalLimit = fromIntegral $ sampleSize testconf * 10
                                    {- test user objects -}
                                    userBoxes <- testCrud storage testconf randomUser
                                        Storage.spawnUser
                                        Storage.getUser
                                        Storage.setUser
                                        Storage.deleteUser
                                        Storage.listUsers
                                    {- test access keys -}
                                    {- generate some access keys for users -}
                                    let userRefs = map fst userBoxes
                                    userAccessKeysA <- parallelFor userRefs $ \ref -> do
                                        n <- generate $ randomWithin 1 3 :: IO Int
                                        replicateM n $ assertJust =<< Storage.spawnAccessKey storage ref
                                    parallelFor_ (zip userRefs userAccessKeysA) $ \(userRef, akeysA) -> do
                                        forM_ akeysA $ \akey -> do
                                            ref2 <- assertJust =<< Storage.lookupAccessKey storage akey
                                            ref2 `shouldBe` userRef
                                        akeyRefsA <- assertJust =<< Storage.listAccessKeysOf storage userRef 0 totalLimit
                                        akeyRefsA `shouldMatchList` map Storage.accessKeyId akeysA
                                        return ()
                                    {- delete some keys -}
                                    userAccessKeysB <- parallelFor userAccessKeysA $ \akeysA -> do
                                        akeysB <- fmap catMaybes $ parallelFor akeysA $ \akey@(Storage.AccessKey front _) -> do
                                            i <- generate $ randomWithin 1 10 :: IO Int
                                            if i <= 1
                                                then do
                                                    assertJust =<< Storage.deleteAccessKey storage (Storage.Reference front)
                                                    return Nothing
                                                else return $ Just akey
                                        return akeysB
                                    parallelFor_ (zip userRefs userAccessKeysB) $ \(userRef, akeysB) -> do
                                        akeyRefsB <- assertJust =<< Storage.listAccessKeysOf storage userRef 0 totalLimit
                                        akeyRefsB `shouldMatchList` map Storage.accessKeyId akeysB
                                    {- spawn some additional keys -}
                                    userAccessKeysC <- parallelFor (zip userRefs userAccessKeysB) $ \(userRef, akeysB) -> do
                                        i <- generate $ randomWithin 1 10 :: IO Int
                                        if i <= 1
                                            then do
                                                newkey <- assertJust =<< Storage.spawnAccessKey storage userRef
                                                return $ newkey:akeysB
                                            else return akeysB
                                    parallelFor_ (zip userRefs userAccessKeysC) $ \(userRef, akeysC) -> do
                                        akeyRefsC <- assertJust =<< Storage.listAccessKeysOf storage userRef 0 totalLimit
                                        akeyRefsC `shouldMatchList` map Storage.accessKeyId akeysC
                                    {- test author objects -}
                                    authorBoxes <- testCrud storage testconf randomAuthor
                                        Storage.spawnAuthor
                                        Storage.getAuthor
                                        Storage.setAuthor
                                        Storage.deleteAuthor
                                        Storage.listAuthors
                                    let authorRefs = map fst authorBoxes
                                    {- connect some users with some authors -}
                                    userToAuthorListA <- parallelFor userRefs $ \userRef -> do
                                        userAuthors <- generate $ chooseFrom authorRefs =<< randomWithin 0 3
                                        parallelFor userAuthors $ \authorRef -> do
                                            assertJust =<< Storage.connectUserAuthor storage userRef authorRef
                                            return authorRef
                                    parallelFor_ (zip userRefs userToAuthorListA) $ \(userRef, userAuthors1) -> do
                                        userAuthors2 <- fmap (map fst) $ assertJust =<< Storage.listAuthorsOfUser storage userRef 0 totalLimit
                                        userAuthors2 `shouldMatchList` userAuthors1
                                    parallelFor_ authorRefs $ \authorRef -> do
                                        let authorUsers1 = mapMaybe
                                                (\(userRef, userAuthors) -> do
                                                    guard $ authorRef `elem` userAuthors
                                                    Just userRef)
                                                (zip userRefs userToAuthorListA)
                                        authorUsers2 <- fmap (map fst) $ assertJust =<< Storage.listUsersOfAuthor storage authorRef 0 totalLimit
                                        authorUsers2 `shouldMatchList` authorUsers1
                                    {- disconnect some users and authors -}
                                    userToAuthorListB <- parallelFor (zip userRefs userToAuthorListA) $ \(userRef, userAuthorsA) -> do
                                        fmap catMaybes $ parallelFor userAuthorsA $ \authorRef -> do
                                            i <- generate $ randomWithin 1 3 :: IO Int
                                            if i <= 1
                                                then do
                                                    assertJust =<< Storage.disconnectUserAuthor storage userRef authorRef
                                                    return Nothing
                                                else return $ Just authorRef
                                    parallelFor_ (zip userRefs userToAuthorListB) $ \(userRef, userAuthors1) -> do
                                        userAuthors2 <- fmap (map fst) $ assertJust =<< Storage.listAuthorsOfUser storage userRef 0 totalLimit
                                        userAuthors2 `shouldMatchList` userAuthors1
                                    parallelFor_ authorRefs $ \authorRef -> do
                                        let authorUsers1 = mapMaybe
                                                (\(userRef, userAuthors) -> do
                                                    guard $ authorRef `elem` userAuthors
                                                    Just userRef)
                                                (zip userRefs userToAuthorListB)
                                        authorUsers2 <- fmap (map fst) $ assertJust =<< Storage.listUsersOfAuthor storage authorRef 0 totalLimit
                                        authorUsers2 `shouldMatchList` authorUsers1
                                    return ()

testCrud
    :: (Show obj, Eq obj)
    => Storage.Handle
    -> TestConfig
    -> Gen obj
    -> (Storage.Handle -> obj -> IO (Maybe (Storage.Reference obj)))
    -> (Storage.Handle -> Storage.Reference obj -> IO (Maybe obj))
    -> (Storage.Handle -> Storage.Reference obj -> obj -> IO (Maybe ()))
    -> (Storage.Handle -> Storage.Reference obj -> IO (Maybe ()))
    -> (Storage.Handle -> Int64 -> Int64 -> IO [(Storage.Reference obj, obj)])
    -> IO [(Storage.Reference obj, obj)]
testCrud storage testconf randomObject
        spawnObject getObject setObject deleteObject listObjects = do
    let totalLimit = fromIntegral $ sampleSize testconf * 10
    {- create a batch of objects -}
    listA1 <- generate $ replicateM (sampleSize testconf) randomObject
    refsA <- parallelFor listA1 $ \objA -> assertJust =<< spawnObject storage objA
    listA2 <- parallelFor refsA $ \ref -> assertJust =<< getObject storage ref
    listA2 `shouldBe` listA1
    let boxesA1 = zip refsA listA1
    boxesA2 <- listObjects storage 0 totalLimit
    boxesA2 `shouldMatchList` boxesA1
    {- create a second batch of objects -}
    listB1 <- generate $ replicateM (sampleSize testconf) randomObject
    refsB <- parallelFor listB1 $ \objB -> assertJust =<< spawnObject storage objB
    listB2 <- parallelFor refsB $ \ref -> assertJust =<< getObject storage ref
    listB2 `shouldBe` listB1
    let boxesC1 = boxesA1 ++ zip refsB listB1
    boxesC2 <- listObjects storage 0 totalLimit
    boxesC2 `shouldMatchList` boxesC1
    {- replace some objects -}
    boxesD1 <- parallelFor boxesC1 $ \boxC@(ref, _) -> do
        i <- generate $ randomWithin 1 10 :: IO Int
        if i <= 1
            then do
                objD <- generate $ randomObject
                assertJust =<< setObject storage ref objD
                return (ref, objD)
            else return boxC
    boxesD2 <- listObjects storage 0 totalLimit
    boxesD2 `shouldMatchList` boxesD1
    {- delete some objects -}
    boxesE1 <- fmap catMaybes $ parallelFor boxesD1 $ \boxD@(ref, _) -> do
        i <- generate $ randomWithin 1 10 :: IO Int
        if i <= 1
            then do
                assertJust =<< deleteObject storage ref
                return Nothing
            else return $ Just boxD
    boxesE2 <- listObjects storage 0 totalLimit
    boxesE2 `shouldMatchList` boxesE1
    return boxesE2
