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
                        [ QueryMaybe (Select "sn_metadata" (fText "mvalue" :* E) (Just (Condition "mkey = 'schema_version'" E)) Nothing Nothing)
                            |>> Nothing
                        ]
                    matchCurrentSchema logger db (return . Left) (return $ Right ())
                        `shouldReturn` Left InitFailureEmpty
                    actionCheckpoint
                        [ QueryMaybe (Select "sn_metadata" (fText "mvalue" :* E) (Just (Condition "mkey = 'schema_version'" E)) Nothing Nothing)
                            |>> Just []
                        ]
                    matchCurrentSchema logger db (return . Left) (return $ Right ())
                        `shouldReturn` Left InitFailureInvalidSchema
                    actionCheckpoint
                        [ QueryMaybe (Select "sn_metadata" (fText "mvalue" :* E) (Just (Condition "mkey = 'schema_version'" E)) Nothing Nothing)
                            |>> Just [VText "asdfasdf" :* E]
                        ]
                    matchCurrentSchema logger db (return . Left) (return $ Right ())
                        `shouldReturn` Left InitFailureInvalidSchema
                    actionCheckpoint
                        [ QueryMaybe (Select "sn_metadata" (fText "mvalue" :* E) (Just (Condition "mkey = 'schema_version'" E)) Nothing Nothing)
                            |>> Just [VText "SomeNewsSchema 0 0" :* E]
                        ]
                    matchCurrentSchema logger db (return . Left) (return $ Right ())
                        `shouldReturn` Left (InitFailureObsoleteSchema $ SomeNewsSchema 0 0)
                    actionCheckpoint
                        [ QueryMaybe (Select "sn_metadata" (fText "mvalue" :* E) (Just (Condition "mkey = 'schema_version'" E)) Nothing Nothing)
                            |>> Just [VText "SomeNewsSchema 0 123456" :* E]
                        ]
                    matchCurrentSchema logger db (return . Left) (return $ Right ())
                        `shouldReturn` Left (InitFailureAdvancedSchema $ SomeNewsSchema 0 123456)
                    actionCheckpoint
                        [ QueryMaybe (Select "sn_metadata" (fText "mvalue" :* E) (Just (Condition "mkey = 'schema_version'" E)) Nothing Nothing)
                            |>> Just [VText "SomeNewsSchema 123456 123456" :* E]
                        ]
                    matchCurrentSchema logger db (return . Left) (return $ Right ())
                        `shouldReturn` Left (InitFailureIncompatibleSchema $ SomeNewsSchema 123456 123456)
                    actionCheckpoint
                        [ QueryMaybe (Select "sn_metadata" (fText "mvalue" :* E) (Just (Condition "mkey = 'schema_version'" E)) Nothing Nothing)
                            |>> Just [VText "SomeNewsSchema 0 1" :* E]
                        ]
                    matchCurrentSchema logger db (return . Left) (return $ Right ())
                        `shouldReturn` Right ()
        it "creates a database from scratch" $ do
            Logger.withTestLogger $ \logger -> do
                withTestDatabase $ \actionCheckpoint db -> do
                    actionCheckpoint
                        [ QueryMaybe (Select "sn_metadata" (fText "mvalue" :* E) (Just (Condition "mkey = 'schema_version'" E)) Nothing Nothing)
                            |>> Just []
                        ]
                    upgradeSchema logger db
                        `shouldReturn` Left InitFailureInvalidSchema
                    actionCheckpoint
                        [ QueryMaybe (Select "sn_metadata" (fText "mvalue" :* E) (Just (Condition "mkey = 'schema_version'" E)) Nothing Nothing)
                            |>> Just [VText "asdfasdf" :* E]
                        ]
                    upgradeSchema logger db
                        `shouldReturn` Left InitFailureInvalidSchema
                    actionCheckpoint
                        [ QueryMaybe (Select "sn_metadata" (fText "mvalue" :* E) (Just (Condition "mkey = 'schema_version'" E)) Nothing Nothing)
                            |>> Just [VText "SomeNewsSchema 0 0" :* E]
                        ]
                    upgradeSchema logger db
                        `shouldReturn` Left (InitFailureIncompatibleSchema $ SomeNewsSchema 0 0)
                    actionCheckpoint
                        [ QueryMaybe (Select "sn_metadata" (fText "mvalue" :* E) (Just (Condition "mkey = 'schema_version'" E)) Nothing Nothing)
                            |>> Just [VText "SomeNewsSchema 0 123456" :* E]
                        ]
                    upgradeSchema logger db
                        `shouldReturn` Left (InitFailureAdvancedSchema $ SomeNewsSchema 0 123456)
                    actionCheckpoint
                        [ QueryMaybe (Select "sn_metadata" (fText "mvalue" :* E) (Just (Condition "mkey = 'schema_version'" E)) Nothing Nothing)
                            |>> Just [VText "SomeNewsSchema 123456 123456" :* E]
                        ]
                    upgradeSchema logger db
                        `shouldReturn` Left (InitFailureIncompatibleSchema $ SomeNewsSchema 123456 123456)
                    {- no schema -}
                    actionCheckpoint
                        [ QueryMaybe (Select "sn_metadata" (fText "mvalue" :* E) (Just (Condition "mkey = 'schema_version'" E)) Nothing Nothing)
                            |>> Nothing
                        , BeginTransaction
                            |>> ()
                        , QueryMaybe
                            (CreateTable "sn_metadata"
                                [ ColumnDecl (FText "mkey") [CPrimaryKey]
                                , ColumnDecl (FText "mvalue") []
                                ]
                                [])
                            |>> Just ()
                        , QueryMaybe
                            (Insert
                                "sn_metadata"
                                (fText "mkey" :* fText "mvalue" :* E)
                                [Val "schema_version" :* Val "SomeNewsSchema 0 1" :* E])
                            |>> Just ()
                        , QueryMaybe
                            (CreateTable "sn_users"
                                [ ColumnDecl (FInt "user_id") [CIntegerId]
                                , ColumnDecl (FInt "user_salt") [CIntegerSalt]
                                , ColumnDecl (FText "user_name") []
                                , ColumnDecl (FText "user_surname") []
                                ]
                                [])
                            |>> Just ()
                        , CommitTransaction
                            |>> ()
                        ]
                    upgradeSchema logger db
                        `shouldReturn` Right ()
                    {- SomeNewsSchema 0 1 -}
                    actionCheckpoint
                        [ QueryMaybe (Select "sn_metadata" (fText "mvalue" :* E) (Just (Condition "mkey = 'schema_version'" E)) Nothing Nothing)
                            |>> Just [VText "SomeNewsSchema 0 1" :* E]
                        ]
                    upgradeSchema logger db
                        `shouldReturn` Right ()
