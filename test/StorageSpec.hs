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
import qualified Data.Text as Text
import Data.Yaml
import qualified Logger
import Sql.Query
import qualified Sql.Database as Db
import qualified Sql.Database.Config as Db
import qualified Storage
import Test.Hspec
import Test.QuickCheck
import Test.QuickCheck.Gen
import Tuple

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
    _ <- Db.queryMaybe db $ DropTable "sn_metadata"
    _ <- Db.queryMaybe db $ DropTable "sn_users"
    return ()

randomText :: Gen Text.Text
randomText = Text.pack . getPrintableString <$> arbitrary

randomUser :: Gen Storage.User
randomUser = Storage.User <$> randomText <*> randomText

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

assertJust :: HasCallStack => Maybe a -> IO a
assertJust (Just x) = return x
assertJust Nothing = expectationFailure "Just expected, got Nothing" >> undefined

spec :: Spec
spec = do
    beforeAll (decodeFileThrow "test-config.yaml" :: IO [TestConfig]) $ do
        describe "Storage" $ do
            it "handles user objects" $ \testconfList -> do
                forM_ testconfList $ \testconf -> do
                    Logger.withTestLogger $ \logger -> do
                        Db.withDatabase (databaseConfig testconf) logger $ \db -> do
                            clearDatabase db
                            Storage.upgradeSchema logger db `shouldReturn` Right ()
                            Storage.withSqlStorage logger db
                                (expectationFailure . show)
                                $ \storage -> do
                                    userListA1 <- generate $ vectorOf (sampleSize testconf) randomUser
                                    userRefsA <- parallelFor userListA1 $ assertJust <=< Storage.spawnUser storage
                                    userListA2 <- parallelFor userRefsA $ assertJust <=< Storage.getUser storage
                                    userListA2 `shouldBe` userListA1
                                    let userBoxesA1 = zip userRefsA userListA1
                                    userBoxesA2 <- Storage.listUsers storage 0 (-1)
                                    userBoxesA2 `shouldMatchList` userBoxesA1
                                    userListB1 <- generate $ vectorOf (sampleSize testconf) randomUser
                                    userRefsB <- parallelFor userListB1 $ assertJust <=< Storage.spawnUser storage
                                    userListB2 <- parallelFor userRefsB $ assertJust <=< Storage.getUser storage
                                    userListB2 `shouldBe` userListB1
                                    let userBoxesC1 = userBoxesA1 ++ zip userRefsB userListB1
                                    userBoxesC2 <- Storage.listUsers storage 0 (-1)
                                    userBoxesC2 `shouldMatchList` userBoxesC1
                                    userReplacement <- forM userBoxesC1 $ \(ref, userC) -> do
                                        i <- generate $ choose (1,10)
                                        if (i :: Int) <= 1
                                            then do
                                                userD <- generate $ randomUser
                                                return $ Just $ (ref, userD)
                                            else return Nothing
                                    userBoxesD1 <- parallelFor (zip userBoxesC1 userReplacement) $ \(boxC, mreplace) -> do
                                        case mreplace of
                                            Nothing -> return boxC
                                            Just boxD@(ref, userD) -> do
                                                assertJust =<< Storage.setUser storage ref userD
                                                return boxD
                                    userBoxesD2 <- Storage.listUsers storage 0 (-1)
                                    userBoxesD2 `shouldMatchList` userBoxesD1
                                    userErasure <- forM userBoxesD1 $ \(ref, _) -> do
                                        i <- generate $ choose (1,10)
                                        if (i :: Int) <= 1
                                            then do
                                                return $ Just $ ref
                                            else return Nothing
                                    userBoxesE1Maybes <- parallelFor (zip userBoxesD1 userErasure) $ \(boxD, merase) -> do
                                        case merase of
                                            Nothing -> return $ Just boxD
                                            Just ref -> do
                                                assertJust =<< Storage.deleteUser storage ref
                                                return Nothing
                                    let userBoxesE1 = catMaybes userBoxesE1Maybes
                                    userBoxesE2 <- Storage.listUsers storage 0 (-1)
                                    userBoxesE2 `shouldMatchList` userBoxesE1
