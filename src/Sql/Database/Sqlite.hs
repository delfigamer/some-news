{-# LANGUAGE FlexibleInstances #-}

module Sql.Database.Sqlite
    ( withSqlite
    ) where

import Control.Concurrent.MVar
import Control.Exception
import Data.IORef
import Data.List
import Data.String
import qualified Data.Text as Text
import Database.SQLite.Simple
import Database.SQLite.Simple.FromRow
import Database.SQLite.Simple.ToField
import qualified Logger
import qualified Sql.Database as Db
import qualified Sql.Query as Q
import Tuple

withSqlite :: String -> Logger.Handle -> (Db.Handle -> IO r) -> IO r
withSqlite path logger body = do
    bracket
        (open path >>= newMVar)
        (\mvconn -> takeMVar mvconn >>= close)
        (\mvconn -> do
            body $ Db.Handle
                { Db.query = \query -> withMVar mvconn $ \conn -> sqliteQuery logger conn query
                , Db.withTransaction = \act -> withMVar mvconn $ \conn -> sqliteWithTransaction logger conn act
                })

sqliteQuery :: Logger.Handle -> Connection -> Q.Query result -> IO (Maybe result)
sqliteQuery logger conn queryData = do
    let queryText = Db.renderQueryTemplate queryData
    Logger.debug logger $ "Sqlite: " <> Text.pack (show queryData)
    Logger.info logger $ "Sqlite: " <> Text.pack queryText
    let sqlQuery = fromString queryText :: Query
    eresult <- try $ case queryData of
        Q.CreateTable {} -> execute_ conn sqlQuery
        Q.AddTableColumn {} -> execute_ conn sqlQuery
        Q.DropTable {} -> execute_ conn sqlQuery
        Q.Select _ fields mcond _ _ -> withConditionValues mcond $ queryWith (tupleParser fields) conn sqlQuery
        Q.Insert _ _ rows -> executeMany conn sqlQuery rows
        Q.InsertReturning table fields values rets -> sqliteInsertReturning logger conn table fields values rets
        Q.Update _ _ values mcond -> withConditionValues mcond $ \condvals -> execute conn sqlQuery $ joinTuple values condvals
    case eresult of
        Left err -> do
            Logger.warn logger $ "Sqlite: " <> Text.pack (displayException (err :: SomeException))
            return Nothing
        Right result -> return $ Just result

sqliteInsertReturning :: Logger.Handle -> Connection -> Q.TableName -> TupleT Q.Field vs -> TupleT Q.Value vs -> TupleT Q.Field rs -> IO (Maybe (Tuple rs))
sqliteInsertReturning logger conn table fields values rets = do
    let query1 = Db.renderQueryTemplate $ Q.Insert table fields [values]
    Logger.debug logger $ "Sqlite: " <> Text.pack (show query1)
    execute conn (fromString query1) values
    count <- changes conn
    if count < 1
        then return $ Nothing
        else do
            lastRowId <- lastInsertRowId conn
            let query2 = Db.renderQueryTemplate $ Q.Select table rets (Just (Q.Condition "_rowid_ = ?" E)) Nothing Nothing
            Logger.debug logger $ "Sqlite: " <> Text.pack (show query2) <> "; -- " <> Text.pack (show lastRowId)
            results <- queryWith (tupleParser rets) conn (fromString query2) $ Only $ SQLInteger lastRowId
            case results of
                result:_ -> return $ Just result
                [] -> return $ Nothing

sqliteWithTransaction :: Logger.Handle -> Connection -> IO r -> IO r
sqliteWithTransaction logger conn act = do
    Logger.info logger $ "Sqlite: BEGIN TRANSACTION"
    r <- withTransaction conn act
    Logger.info logger $ "Sqlite: COMMIT TRANSACTION"
    return r

withConditionValues :: Maybe Q.Condition -> (forall ts. TupleT Q.Value ts -> r) -> r
withConditionValues Nothing f = f E
withConditionValues (Just (Q.Condition _ values)) f = f values

tupleParser :: TupleT Q.Field ts -> RowParser (Tuple ts)
tupleParser E = return E
tupleParser (Q.FInteger _ :+ xs) = (:*) <$> field <*> tupleParser xs
tupleParser (Q.FString _ :+ xs) = (:*) <$> field <*> tupleParser xs

instance ToRow (TupleT Q.Value ts) where
    toRow = mapTuple toField

instance ToField (Q.Value a) where
    toField (Q.VInteger x) = toField x
    toField (Q.VString x) = toField x
