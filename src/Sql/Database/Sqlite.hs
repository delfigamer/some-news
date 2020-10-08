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
        (initialize path >>= newMVar)
        (\mvconn -> takeMVar mvconn >>= close)
        (\mvconn -> do
            body $ Db.Handle
                { Db.queryMaybe = sqliteQueryMaybe logger mvconn
                , Db.withTransaction = sqliteWithTransaction logger mvconn
                })

initialize :: String -> IO Connection
initialize path = do
    conn <- open path
    execute_ conn "PRAGMA foreign_keys = ON"
    execute_ conn "PRAGMA journal_mode = WAL"
    return conn

sqliteQueryMaybe :: Logger.Handle -> MVar Connection -> Q.Query result -> IO (Maybe result)
sqliteQueryMaybe logger mvconn queryData = do
    withMVar mvconn $ \conn -> do
        let queryText = Db.renderQueryTemplate queryData
        Logger.debug logger $ "Sqlite: " <> Text.pack (show queryData)
        Logger.info logger $ "Sqlite: " <> Text.pack queryText
        let sqlQuery = fromString queryText :: Query
        eresult <- try $ case queryData of
            Q.CreateTable {} -> execute_ conn sqlQuery
            Q.AddTableColumn {} -> execute_ conn sqlQuery
            Q.DropTable {} -> execute_ conn sqlQuery
            Q.Select _ fields mcond _ _ -> Db.withConditionValues mcond $ queryWith (tupleParser fields) conn sqlQuery
            Q.Insert _ _ rows -> executeMany conn sqlQuery rows
            Q.InsertReturning table fields values rets -> sqliteInsertReturning logger conn table fields values rets
            Q.Update _ _ values mcond -> Db.withConditionValues mcond $ \condvals -> execute conn sqlQuery $ joinTuple values condvals
            Q.Delete _ mcond -> Db.withConditionValues mcond $ execute conn sqlQuery
        case eresult of
            Left err -> do
                Logger.warn logger $ "Sqlite: " <> Text.pack (displayException (err :: SomeException))
                return Nothing
            Right result -> return $ Just result

sqliteInsertReturning :: Logger.Handle -> Connection -> Q.TableName -> TupleT Q.Field vs -> TupleT Q.Value vs -> TupleT Q.Field rs -> IO (TupleT Q.Value rs)
sqliteInsertReturning logger conn table fields values rets = do
    let query1 = Db.renderQueryTemplate $ Q.Insert table fields [values]
    Logger.debug logger $ "Sqlite: " <> Text.pack (show query1)
    execute conn (fromString query1) values
    lastRowId <- lastInsertRowId conn
    let query2 = Db.renderQueryTemplate $ Q.Select table rets (Just (Q.Condition "_rowid_ = ?" E)) Nothing Nothing
    Logger.debug logger $ "Sqlite: " <> Text.pack (show query2) <> "; -- " <> Text.pack (show lastRowId)
    [result] <- queryWith (tupleParser rets) conn (fromString query2) $ Only $ SQLInteger lastRowId
    return result

sqliteWithTransaction :: Logger.Handle -> MVar Connection -> IO r -> IO r
sqliteWithTransaction logger mvconn act = do
    withMVar mvconn $ \conn -> do
        Logger.info logger $ "Sqlite: BEGIN TRANSACTION"
        r <- withTransaction conn $ do
            bracket_
                (putMVar mvconn conn)
                (takeMVar mvconn >> return ())
                act
        Logger.info logger $ "Sqlite: COMMIT TRANSACTION"
        return r

tupleParser :: TupleT Q.Field ts -> RowParser (TupleT Q.Value ts)
tupleParser E = return E
tupleParser (Q.FInt _ :* xs) = (:*) . Q.VInt <$> field <*> tupleParser xs
tupleParser (Q.FString _ :* xs) = (:*) . Q.VString <$> field <*> tupleParser xs
tupleParser (Q.FText _ :* xs) = (:*) . Q.VText <$> field <*> tupleParser xs

instance ToRow (TupleT Q.Value ts) where
    toRow = mapTuple toField

instance ToField (Q.Value a) where
    toField (Q.VInt x) = toField x
    toField (Q.VString x) = toField x
    toField (Q.VText x) = toField x
