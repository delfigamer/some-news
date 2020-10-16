{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE ScopedTypeVariables #-}

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
import Data.Proxy
import Data.String
import qualified Data.Text as Text
import Data.Time.Clock
import Database.SQLite.Simple
import Database.SQLite.Simple.FromField
import Database.SQLite.Simple.FromRow
import Database.SQLite.Simple.ToField
import Database.SQLite.Simple.ToRow
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
            Logger.debug logger $ "Sqlite: " <> Text.pack queryText <> "; -- " <> Text.pack (Q.showPrimValues 0 queryValues "")
            let sqlQuery = fromString queryText :: Query
            eresult <- try $ case queryData of
                Q.CreateTable {} -> execute_ conn sqlQuery
                Q.CreateIndex {} -> execute_ conn sqlQuery
                Q.DropTable {} -> execute_ conn sqlQuery
                Q.Select {} -> query conn sqlQuery queryValues
                Q.Insert_ {} -> execute conn sqlQuery queryValues
                Q.Insert table fields values rets -> sqliteInsertReturning logger conn table fields values rets
                Q.Update {} -> execute conn sqlQuery queryValues
                Q.Delete {} -> execute conn sqlQuery queryValues
            case eresult of
                Left err -> do
                    Logger.warn logger $ "Sqlite: " <> Text.pack (displayException (err :: SomeException))
                    return Nothing
                Right result -> return $ Just result

sqliteInsertReturning
    :: (All Q.IsValue vs, All Q.IsValue rs)
    => Logger.Handle -> Connection
    -> Q.TableName -> HList Q.Field vs -> HList Maybe vs -> HList Q.Field rs -> IO (HList Maybe rs)
sqliteInsertReturning logger conn table fields values rets = do
    let queryData1 = Q.Insert_ table fields values
    Q.withQueryRender sqliteDetailRenderer queryData1 $ \queryValues1 queryText1 -> do
        Logger.debug logger $ "Sqlite: " <> Text.pack queryText1 <> "; -- " <> Text.pack (Q.showPrimValues 0 queryValues1 "")
        execute conn (fromString queryText1) queryValues1
        lastRowId <- lastInsertRowId conn
        let queryData2 = Q.Select [table] rets [Q.Where "_rowid_ = ?" $ Just lastRowId :/ E] [] Q.AllRows
        Q.withQueryRender sqliteDetailRenderer queryData2 $ \queryValues2 queryText2 -> do
            Logger.debug logger $ "Sqlite: " <> Text.pack queryText2 <> "; -- " <> Text.pack (Q.showPrimValues 0 queryValues2 "")
            [result] <- query conn (fromString queryText2) queryValues2
            return result

sqliteFoldQuery :: Logger.Handle -> MVar Connection -> Q.Query [row] -> a -> (a -> row -> IO a) -> IO a
sqliteFoldQuery logger mvconn queryData seed foldf = do
    withMVar mvconn $ \conn -> do
        Q.withQueryRender sqliteDetailRenderer queryData $ \queryValues queryText -> do
            Logger.debug logger $ "Sqlite: (streaming) " <> Text.pack queryText <> "; -- " <> Text.pack (Q.showPrimValues 0 queryValues "")
            let sqlQuery = fromString queryText :: Query
            case queryData of
                Q.Select {} -> fold conn sqlQuery queryValues seed foldf

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
        Q.FReal _ -> " REAL"
        Q.FText _ -> " TEXT"
        Q.FBlob _ -> " BLOB"
        Q.FTime _ -> " TEXT"
    }

instance forall ts. (All Q.IsValue ts) => FromRow (HList Maybe ts) where
    fromRow = traverseHListGiven
        (Proxy :: Proxy Q.IsValue)
        (proxyHList (Proxy :: Proxy ts))
        parseValue

parseValue :: forall a. Q.IsValue a => Proxy a -> RowParser (Maybe a)
parseValue _ = fmap Q.primDecode $ traverseHListGiven
    (Proxy :: Proxy Q.IsPrimType)
    (proxyHList (Proxy :: Proxy (Q.Prims a)))
    parsePrimValue

parsePrimValue :: Q.IsPrimType a => Proxy a -> RowParser (Q.PrimValue a)
parsePrimValue proxy = Q.matchPrimType proxy
    field field field field field

instance FromField (Q.PrimValue 'Q.TInt) where
    fromField f = (Q.VInt <$> fromField f) `mplus` return Q.VNull

instance FromField (Q.PrimValue 'Q.TReal) where
    fromField f = (Q.VReal <$> fromField f) `mplus` return Q.VNull

instance FromField (Q.PrimValue 'Q.TText) where
    fromField f = (Q.VText <$> fromField f) `mplus` return Q.VNull

instance FromField (Q.PrimValue 'Q.TBlob) where
    fromField f = (Q.VBlob <$> fromField f) `mplus` return Q.VNull

instance FromField (Q.PrimValue 'Q.TTime) where
    fromField f = (Q.VTime <$> fromField f) `mplus` return Q.VNull

instance ToRow (HList Q.PrimValue ts) where
    toRow = homogenize toField

instance ToField (Q.PrimValue a) where
    toField (Q.VInt x) = toField x
    toField (Q.VReal x) = toField x
    toField (Q.VText x) = toField x
    toField (Q.VBlob x) = toField x
    toField (Q.VTime x) = toField x
    toField Q.VNull = SQLNull
