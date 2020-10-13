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
import Data.Time.Clock
import Data.Time.Format.ISO8601
import Database.SQLite.Simple
import Database.SQLite.Simple.FromField
import Database.SQLite.Simple.FromRow
import Database.SQLite.Simple.ToField
import qualified Logger
import qualified Sql.Database as Db
import qualified Sql.Query as Q
import qualified Sql.Query.Render as Q
import Tuple

withSqlite :: String -> Logger.Handle -> (Db.Handle -> IO r) -> IO r
withSqlite path logger body = do
    bracket
        (initialize path >>= newMVar)
        (\mvconn -> takeMVar mvconn >>= close)
        (\mvconn -> do
            body $ Db.Handle
                { Db.queryMaybe = sqliteQueryMaybe logger mvconn
                , Db.foldQuery = sqliteFoldQuery logger mvconn
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
        Q.withQueryRender sqliteDetailRenderer queryData $ \queryValues queryText -> do
            Logger.debug logger $ "Sqlite: " <> Text.pack queryText <> "; -- " <> Text.pack (show queryValues)
            let sqlQuery = fromString queryText :: Query
            eresult <- try $ case queryData of
                Q.CreateTable {} -> execute_ conn sqlQuery
                Q.CreateIndex {} -> execute_ conn sqlQuery
                Q.DropTable {} -> execute_ conn sqlQuery
                Q.Select _ fields _ _ _ -> queryWith (valueParser fields) conn sqlQuery queryValues
                Q.Insert _ _ _ E -> execute conn sqlQuery queryValues >> return E
                Q.Insert table fields values rets -> sqliteInsertReturning logger conn table fields values rets
                Q.Update {} -> execute conn sqlQuery queryValues
                Q.Delete {} -> execute conn sqlQuery queryValues
            case eresult of
                Left err -> do
                    Logger.warn logger $ "Sqlite: " <> Text.pack (displayException (err :: SomeException))
                    return Nothing
                Right result -> return $ Just result

sqliteInsertReturning :: Logger.Handle -> Connection -> Q.TableName -> TupleT Q.Field vs -> TupleT Q.Value vs -> TupleT Q.Field rs -> IO (TupleT Q.Value rs)
sqliteInsertReturning logger conn table fields values rets = do
    let queryData1 = Q.Insert table fields values E
    Q.withQueryRender sqliteDetailRenderer queryData1 $ \queryValues1 queryText1 -> do
        Logger.debug logger $ "Sqlite: " <> Text.pack queryText1 <> "; -- " <> Text.pack (show queryValues1)
        execute conn (fromString queryText1) queryValues1
        lastRowId <- lastInsertRowId conn
        let queryData2 = Q.Select [table] rets [Q.Where "_rowid_ = ?" $ Q.Val lastRowId :* E] [] Q.AllRows
        Q.withQueryRender sqliteDetailRenderer queryData2 $ \queryValues2 queryText2 -> do
            Logger.debug logger $ "Sqlite: " <> Text.pack queryText2 <> "; -- " <> Text.pack (show queryValues2)
            [result] <- queryWith (valueParser rets) conn (fromString queryText2) queryValues2
            return result

sqliteFoldQuery :: Logger.Handle -> MVar Connection -> Q.Query [row] -> a -> (a -> row -> IO a) -> IO a
sqliteFoldQuery logger mvconn queryData seed foldf = do
    withMVar mvconn $ \conn -> do
        undefined

sqliteWithTransaction :: Logger.Handle -> MVar Connection -> IO r -> IO r
sqliteWithTransaction logger mvconn act = do
    withMVar mvconn $ \conn -> do
        Logger.debug logger $ "Sqlite: BEGIN TRANSACTION"
        r <- withTransaction conn $ do
            bracket_
                (putMVar mvconn conn)
                (takeMVar mvconn >> return ())
                act
        Logger.debug logger $ "Sqlite: COMMIT TRANSACTION"
        return r

sqliteDetailRenderer :: Q.DetailRenderer
sqliteDetailRenderer = Q.DetailRenderer
    { Q.renderFieldType = \field -> case field of
        Q.FInt _ -> " INTEGER"
        Q.FFloat _ -> " REAL"
        Q.FText _ -> " TEXT"
        Q.FBlob _ -> " BLOB"
        Q.FDateTime _ -> " TEXT"
    }

valueParser :: TupleT Q.Field ts -> RowParser (TupleT Q.Value ts)
valueParser fields = Q.decode fields <$> primParser (Q.primFields fields)

primParser :: TupleT Q.PrimField ts -> RowParser (TupleT Q.PrimValue ts)
primParser E = return E
primParser (Q.FInt _ :* fs) = (:*) <$> field <*> primParser fs
primParser (Q.FFloat _ :* fs) = (:*) <$> field <*> primParser fs
primParser (Q.FText _ :* fs) = (:*) <$> field <*> primParser fs
primParser (Q.FBlob _ :* fs) = (:*) <$> field <*> primParser fs
primParser (Q.FDateTime _ :* fs) = (:*) <$> field <*> primParser fs

instance FromField (Q.PrimValue Int64) where
    fromField f = (Q.VInt <$> fromField f) `mplus` return Q.VNull

instance FromField (Q.PrimValue Double) where
    fromField f = (Q.VFloat <$> fromField f) `mplus` return Q.VNull

instance FromField (Q.PrimValue Text.Text) where
    fromField f = (Q.VText <$> fromField f) `mplus` return Q.VNull

instance FromField (Q.PrimValue BS.ByteString) where
    fromField f = (Q.VBlob <$> fromField f) `mplus` return Q.VNull

instance FromField (Q.PrimValue UTCTime) where
    fromField f = (Q.VDateTime <$> fromField f) `mplus` return Q.VNull

instance ToRow (TupleT Q.PrimValue ts) where
    toRow = mapTuple toField

instance ToField (Q.PrimValue a) where
    toField (Q.VInt x) = toField x
    toField (Q.VFloat x) = toField x
    toField (Q.VText x) = toField x
    toField (Q.VBlob x) = toField x
    toField (Q.VDateTime x) = toField x
    toField Q.VNull = SQLNull
