{-# LANGUAGE FlexibleInstances #-}

module Sql.Database.Sqlite
    ( withSqlite
    ) where

import Control.Concurrent.MVar
import Control.Exception
import Control.Monad
import qualified Data.ByteString as BS
import Data.IORef
import Data.Int
import Data.List
import Data.String
import qualified Data.Text as Text
import Database.SQLite.Simple
import Database.SQLite.Simple.FromField
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
        let queryText = Db.renderQueryTemplate renderConstranits queryData
        Logger.debug logger $ "Sqlite: " <> Text.pack (show queryData)
        Logger.info logger $ "Sqlite: " <> Text.pack queryText
        let sqlQuery = fromString queryText :: Query
        eresult <- try $ case queryData of
            Q.CreateTable {} -> execute_ conn sqlQuery
            Q.AddTableColumn {} -> execute_ conn sqlQuery
            Q.DropTable {} -> execute_ conn sqlQuery
            Q.Select _ fields mcond _ _ -> Db.withConditionValues mcond $ queryWith (valueParser fields) conn sqlQuery
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
    let query1 = Db.renderQueryTemplate renderConstranits $ Q.Insert table fields [values]
    Logger.debug logger $ "Sqlite: " <> Text.pack (show query1)
    execute conn (fromString query1) values
    lastRowId <- lastInsertRowId conn
    let query2 = Db.renderQueryTemplate renderConstranits $ Q.Select table rets (Just (Q.Condition "_rowid_ = ?" E)) Nothing Nothing
    Logger.debug logger $ "Sqlite: " <> Text.pack (show query2) <> "; -- " <> Text.pack (show lastRowId)
    [result] <- queryWith (valueParser rets) conn (fromString query2) $ Only $ SQLInteger lastRowId
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

renderConstranits :: [Q.ColumnConstraint a] -> String
renderConstranits [] = ""
renderConstranits (Q.CPrimaryKey:cs) = " PRIMARY KEY" ++ renderConstranits cs
renderConstranits (Q.CIntegerId:cs) = " PRIMARY KEY" ++ renderConstranits cs

valueParser :: TupleT Q.Field ts -> RowParser (TupleT Q.Value ts)
valueParser fields = Q.decode fields <$> primParser (Q.primFields fields)

primParser :: TupleT Q.PrimField ts -> RowParser (TupleT Q.PrimValue ts)
primParser E = return E
primParser (Q.FInt _ :* fs) = (:*) <$> field <*> primParser fs
primParser (Q.FFloat _ :* fs) = (:*) <$> field <*> primParser fs
primParser (Q.FText _ :* fs) = (:*) <$> field <*> primParser fs
primParser (Q.FBlob _ :* fs) = (:*) <$> field <*> primParser fs

instance FromField (Q.PrimValue Int64) where
    fromField f = (Q.VInt <$> fromField f) `mplus` return Q.VNull

instance FromField (Q.PrimValue Double) where
    fromField f = (Q.VFloat <$> fromField f) `mplus` return Q.VNull

instance FromField (Q.PrimValue Text.Text) where
    fromField f = (Q.VText <$> fromField f) `mplus` return Q.VNull

instance FromField (Q.PrimValue BS.ByteString) where
    fromField f = (Q.VBlob <$> fromField f) `mplus` return Q.VNull

instance ToRow (TupleT Q.Value ts) where
    toRow = mapTuple toField . Q.encode

instance ToField (Q.PrimValue a) where
    toField (Q.VInt x) = SQLInteger x
    toField (Q.VFloat x) = SQLFloat x
    toField (Q.VText x) = SQLText x
    toField (Q.VBlob x) = SQLBlob x
    toField Q.VNull = SQLNull
