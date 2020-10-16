module StorageSpec
    ( spec
    ) where

import Control.Concurrent
import Control.Concurrent.MVar
import Control.Concurrent.QSemN
import Control.Exception
import Control.Monad
import Data.Aeson
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
                                    {- create a batch of users -}
                                    userListA1 <- generate $ replicateM (sampleSize testconf) randomUser
                                    userRefsA <- parallelFor userListA1 $ \userA -> assertJust =<< Storage.spawnUser storage userA
                                    userListA2 <- parallelFor userRefsA $ \ref -> assertJust =<< Storage.getUser storage ref
                                    userListA2 `shouldBe` userListA1
                                    let userBoxesA1 = zip userRefsA userListA1
                                    userBoxesA2 <- Storage.listUsers storage 0 totalLimit
                                    userBoxesA2 `shouldMatchList` userBoxesA1
                                    {- create a second batch of users -}
                                    userListB1 <- generate $ replicateM (sampleSize testconf) randomUser
                                    userRefsB <- parallelFor userListB1 $ \userB -> assertJust =<< Storage.spawnUser storage userB
                                    userListB2 <- parallelFor userRefsB $ \ref -> assertJust =<< Storage.getUser storage ref
                                    userListB2 `shouldBe` userListB1
                                    let userBoxesC1 = userBoxesA1 ++ zip userRefsB userListB1
                                    userBoxesC2 <- Storage.listUsers storage 0 totalLimit
                                    userBoxesC2 `shouldMatchList` userBoxesC1
                                    {- change some users' info -}
                                    userBoxesD1 <- parallelFor userBoxesC1 $ \boxC@(ref, _) -> do
                                        i <- generate $ randomWithin 1 10 :: IO Int
                                        if i <= 1
                                            then do
                                                userD <- generate $ randomUser
                                                assertJust =<< Storage.setUser storage ref userD
                                                return (ref, userD)
                                            else return boxC
                                    userBoxesD2 <- Storage.listUsers storage 0 totalLimit
                                    userBoxesD2 `shouldMatchList` userBoxesD1
                                    {- delete some users -}
                                    userBoxesE1 <- fmap catMaybes $ parallelFor userBoxesD1 $ \boxD@(ref, _) -> do
                                        i <- generate $ randomWithin 1 10 :: IO Int
                                        if i <= 1
                                            then do
                                                assertJust =<< Storage.deleteUser storage ref
                                                return Nothing
                                            else return $ Just boxD
                                    userBoxesE2 <- Storage.listUsers storage 0 totalLimit
                                    userBoxesE2 `shouldMatchList` userBoxesE1
                                    {- generate some access keys for users -}
                                    let userRefsE = map fst userBoxesE1
                                    userAccessKeysA <- parallelFor userRefsE $ \ref -> do
                                        n <- generate $ randomWithin 1 3 :: IO Int
                                        replicateM n $ assertJust =<< Storage.spawnAccessKey storage ref
                                    parallelFor_ (zip userRefsE userAccessKeysA) $ \(userRef, akeysA) -> do
                                        forM_ akeysA $ \akey -> do
                                            ref2 <- assertJust =<< Storage.lookupAccessKey storage akey
                                            ref2 `shouldBe` userRef
                                        akeyRefsA <- assertJust =<< Storage.listAccessKeysOf storage userRef
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
                                    parallelFor_ (zip userRefsE userAccessKeysB) $ \(userRef, akeysB) -> do
                                        akeyRefsB <- assertJust =<< Storage.listAccessKeysOf storage userRef
                                        akeyRefsB `shouldMatchList` map Storage.accessKeyId akeysB
                                    {- spawn some additional keys -}
                                    userAccessKeysC <- parallelFor (zip userRefsE userAccessKeysB) $ \(userRef, akeysB) -> do
                                        i <- generate $ randomWithin 1 10 :: IO Int
                                        if i <= 1
                                            then do
                                                newkey <- assertJust =<< Storage.spawnAccessKey storage userRef
                                                return $ newkey:akeysB
                                            else return akeysB
                                    parallelFor_ (zip userRefsE userAccessKeysC) $ \(userRef, akeysC) -> do
                                        akeyRefsC <- assertJust =<< Storage.listAccessKeysOf storage userRef
                                        akeyRefsC `shouldMatchList` map Storage.accessKeyId akeysC
                                    return ()
