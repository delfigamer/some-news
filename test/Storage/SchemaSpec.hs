module Storage.SchemaSpec
    ( spec
    ) where

import Storage.Schema
import Data.Aeson
import Data.Text (Text)
import qualified Logger
import Sql.Query
import Sql.Database.Test
import Test.Hspec
import Tuple

spec :: Spec
spec = do
    describe "Storage.Schema" $ do
        it "matches current schema" $ do
            Logger.withTestLogger $ \logger -> do
                withTestDatabase $ \actionCheckpoint db -> do
                    actionCheckpoint
                        [ Query (Select "sn_metadata" (FString "mvalue" :+ E) (Just (Condition "mkey = 'schema_version'" E)) Nothing Nothing)
                            |>> Nothing
                        ]
                    matchCurrentSchema logger db (return . Left) (return $ Right ())
                        `shouldReturn` Left InitFailureEmpty
                    actionCheckpoint
                        [ Query (Select "sn_metadata" (FString "mvalue" :+ E) (Just (Condition "mkey = 'schema_version'" E)) Nothing Nothing)
                            |>> Just []
                        ]
                    matchCurrentSchema logger db (return . Left) (return $ Right ())
                        `shouldReturn` Left InitFailureInvalidSchema
                    actionCheckpoint
                        [ Query (Select "sn_metadata" (FString "mvalue" :+ E) (Just (Condition "mkey = 'schema_version'" E)) Nothing Nothing)
                            |>> Just ["asdfasdf" :* E]
                        ]
                    matchCurrentSchema logger db (return . Left) (return $ Right ())
                        `shouldReturn` Left InitFailureInvalidSchema
                    actionCheckpoint
                        [ Query (Select "sn_metadata" (FString "mvalue" :+ E) (Just (Condition "mkey = 'schema_version'" E)) Nothing Nothing)
                            |>> Just ["SomeNewsSchema 0 0" :* E]
                        ]
                    matchCurrentSchema logger db (return . Left) (return $ Right ())
                        `shouldReturn` Left (InitFailureObsoleteSchema $ SomeNewsSchema 0 0)
                    actionCheckpoint
                        [ Query (Select "sn_metadata" (FString "mvalue" :+ E) (Just (Condition "mkey = 'schema_version'" E)) Nothing Nothing)
                            |>> Just ["SomeNewsSchema 0 123456" :* E]
                        ]
                    matchCurrentSchema logger db (return . Left) (return $ Right ())
                        `shouldReturn` Left (InitFailureAdvancedSchema $ SomeNewsSchema 0 123456)
                    actionCheckpoint
                        [ Query (Select "sn_metadata" (FString "mvalue" :+ E) (Just (Condition "mkey = 'schema_version'" E)) Nothing Nothing)
                            |>> Just ["SomeNewsSchema 123456 123456" :* E]
                        ]
                    matchCurrentSchema logger db (return . Left) (return $ Right ())
                        `shouldReturn` Left (InitFailureIncompatibleSchema $ SomeNewsSchema 123456 123456)
                    actionCheckpoint
                        [ Query (Select "sn_metadata" (FString "mvalue" :+ E) (Just (Condition "mkey = 'schema_version'" E)) Nothing Nothing)
                            |>> Just ["SomeNewsSchema 0 1" :* E]
                        ]
                    matchCurrentSchema logger db (return . Left) (return $ Right ())
                        `shouldReturn` Right ()
        it "creates a database from scratch" $ do
            Logger.withTestLogger $ \logger -> do
                withTestDatabase $ \actionCheckpoint db -> do
                    actionCheckpoint
                        [ Query (Select "sn_metadata" (FString "mvalue" :+ E) (Just (Condition "mkey = 'schema_version'" E)) Nothing Nothing)
                            |>> Nothing
                        , BeginTransaction
                            |>> ()
                        , Query
                            (CreateTable "sn_metadata"
                                [ ColumnDecl (FString "mkey") "PRIMARY KEY"
                                , ColumnDecl (FString "mvalue") ""
                                ]
                                [])
                            |>> Just ()
                        , Query
                            (Insert
                                "sn_metadata"
                                (FString "mkey" :+ FString "mvalue" :+ E)
                                [VString "schema_version" :+ VString "SomeNewsSchema 0 1" :+ E])
                            |>> Just ()
                        , Query
                            (CreateTable "sn_users"
                                [ ColumnDecl (FInteger "user_id") "PRIMARY KEY"
                                , ColumnDecl (FString "user_name") ""
                                , ColumnDecl (FString "user_surname") ""
                                ]
                                [])
                            |>> Just ()
                        , CommitTransaction
                            |>> ()
                        ]
                    upgradeSchema logger db
                        `shouldReturn` Right ()
                    actionCheckpoint
                        [ Query (Select "sn_metadata" (FString "mvalue" :+ E) (Just (Condition "mkey = 'schema_version'" E)) Nothing Nothing)
                            |>> Just []
                        ]
                    upgradeSchema logger db
                        `shouldReturn` Left InitFailureInvalidSchema
                    actionCheckpoint
                        [ Query (Select "sn_metadata" (FString "mvalue" :+ E) (Just (Condition "mkey = 'schema_version'" E)) Nothing Nothing)
                            |>> Just ["asdfasdf" :* E]
                        ]
                    upgradeSchema logger db
                        `shouldReturn` Left InitFailureInvalidSchema
                    actionCheckpoint
                        [ Query (Select "sn_metadata" (FString "mvalue" :+ E) (Just (Condition "mkey = 'schema_version'" E)) Nothing Nothing)
                            |>> Just ["SomeNewsSchema 0 0" :* E]
                        ]
                    upgradeSchema logger db
                        `shouldReturn` Left (InitFailureIncompatibleSchema $ SomeNewsSchema 0 0)
                    actionCheckpoint
                        [ Query (Select "sn_metadata" (FString "mvalue" :+ E) (Just (Condition "mkey = 'schema_version'" E)) Nothing Nothing)
                            |>> Just ["SomeNewsSchema 0 123456" :* E]
                        ]
                    upgradeSchema logger db
                        `shouldReturn` Left (InitFailureAdvancedSchema $ SomeNewsSchema 0 123456)
                    actionCheckpoint
                        [ Query (Select "sn_metadata" (FString "mvalue" :+ E) (Just (Condition "mkey = 'schema_version'" E)) Nothing Nothing)
                            |>> Just ["SomeNewsSchema 123456 123456" :* E]
                        ]
                    upgradeSchema logger db
                        `shouldReturn` Left (InitFailureIncompatibleSchema $ SomeNewsSchema 123456 123456)
                    actionCheckpoint
                        [ Query (Select "sn_metadata" (FString "mvalue" :+ E) (Just (Condition "mkey = 'schema_version'" E)) Nothing Nothing)
                            |>> Just ["SomeNewsSchema 0 1" :* E]
                        ]
                    upgradeSchema logger db
                        `shouldReturn` Right ()
