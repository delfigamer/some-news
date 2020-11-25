{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Sql.Database.Sqlite
    ( withSqlite
    ) where

import Control.Concurrent.MVar
import Control.Exception
import Control.Monad
import Data.IORef
import Data.Int
import Data.List
import Data.Proxy
import Data.String
import Data.Time.Clock
import Database.SQLite.Simple
import Database.SQLite.Simple.FromField
import Database.SQLite.Simple.FromRow
import Database.SQLite.Simple.ToField
import Database.SQLite.Simple.ToRow
import qualified Data.ByteString as BS
import qualified Data.Text as Text
import Logger
import ResourcePool
import Tuple
import qualified Sql.Database as Db
import qualified Sql.Query as Q
import qualified Sql.Query.Render as Q

withSqlite :: String -> Logger -> (Db.Database -> IO r) -> IO r
withSqlite path logger body = do
    nameCounter <- newIORef (1 :: Int)
    withResourcePool 1
        (if path == "" || path == ":memory:"then 1 else 16)
        (do
            newName <- fmap (Text.pack . show) $ atomicModifyIORef' nameCounter $ \x -> (x+1, x)
            logInfo logger $ "Sqlite: " <<|| newName << ": open connection"
            newConn <- open path
            execute_ newConn "PRAGMA foreign_keys = ON"
            execute_ newConn "PRAGMA journal_mode = WAL"
            return (newName, newConn))
        (\(name, conn) -> do
            logInfo logger $ "Sqlite: " <<|| name << ": close connection"
            close conn)
        (\(name, _) err -> do
            logErr logger $ "Sqlite: " <<|| name << ": " <<|| displayException err)
        (\pool -> do
            body $ Db.Database
                { Db.execute = \level body ->
                    withResource pool $ \(connName, conn) ->
                        sqliteExecute logger connName conn level body
                })

sqliteExecute
    :: Logger -> Text.Text -> Connection
    -> Db.TransactionLevel -> Db.Transaction r r -> IO (Either Db.QueryError r)
sqliteExecute logger connName conn _ body = do
    sqlitePcall logger connName
        (withCancelableTransaction $ do
            Db.runTransactionBody body
                (sqliteHandleQuery logger connName conn)
                (return (False, Left Db.QueryError))
                (return (False, Left Db.SerializationError))
                (\x -> return (False, Right x))
                (\x -> return (True, Right x))
                )
        (return $ Left Db.QueryError)
        (return $ Left Db.SerializationError)
        (\case
            Left ex -> throwIO (ex :: SomeException)
            Right (_, r) -> return r)
  where
    withCancelableTransaction act = do
        mask $ \restore -> do
            doQuery "BEGIN TRANSACTION"
            aresult <- try $ restore act
            case aresult of
                Right (True, _) -> doQuery "COMMIT TRANSACTION"
                _ -> doQuery "ROLLBACK TRANSACTION"
            return aresult
    doQuery str = do
        logDebug logger $ "Sqlite: " <<|| connName << ": " <<|| str
        execute_ conn (fromString str)

sqliteHandleQuery
    :: Logger -> Text.Text -> Connection
    -> Q.Query result -> IO r -> IO r -> (result -> IO r) -> IO r
sqliteHandleQuery logger connName conn queryData kabort kretry knext = do
    Q.withQueryRender sqliteRenderDetail queryData $ \queryValues queryText -> do
        logDebug logger $ "Sqlite: " <<|| connName << ": " <<|| queryText << "; -- " <<|| Q.showPrimValues 0 queryValues ""
        let sqlQuery = fromString queryText :: Query
        case queryData of
            Q.CreateTable {} -> sqlitePcall logger connName (execute_ conn sqlQuery) kabort kretry knext
            Q.CreateIndex {} -> sqlitePcall logger connName (execute_ conn sqlQuery) kabort kretry knext
            Q.DropTable {} -> sqlitePcall logger connName (execute_ conn sqlQuery) kabort kretry knext
            Q.Select {} -> sqlitePcall logger connName (query conn sqlQuery queryValues) kabort kretry knext
            Q.Insert {} -> sqlitePcall logger connName
                (execute conn sqlQuery queryValues >> changes conn)
                kabort kretry $ \case
                    0 -> knext False
                    1 -> knext True
                    _ -> kabort
            Q.Update {} -> sqlitePcall logger connName
                (execute conn sqlQuery queryValues >> changes conn) kabort kretry (knext . fromIntegral)
            Q.Delete {} -> sqlitePcall logger connName
                (execute conn sqlQuery queryValues >> changes conn) kabort kretry (knext . fromIntegral)

sqlitePcall :: Logger -> Text.Text -> IO a -> IO r -> IO r -> (a -> IO r) -> IO r
sqlitePcall logger connName act kabort kretry knext = do
    eret <- try act
    case eret of
        Right r -> knext r
        Left ex -> do
            let canRetry = case fromException ex of
                    Nothing -> False
                    Just SQLError {sqlError = se} -> case se of
                        ErrorBusy -> True
                        ErrorLocked -> True
                        _ -> False
            if canRetry
                then do
                    logDebug logger $ "Sqlite: " <<|| connName << ": Serialization failure: " <<|| displayException ex
                    kretry
                else do
                    logWarn logger $ "Sqlite: " <<|| connName << ": Error: " <<|| displayException ex
                    kabort

sqliteRenderDetail :: Q.RenderDetail
sqliteRenderDetail = Q.RenderDetail
    { Q.detailFieldType = \case
        Q.FInt _ -> " INTEGER"
        Q.FReal _ -> " REAL"
        Q.FBool _ -> " INTEGER"
        Q.FText _ -> " TEXT"
        Q.FBlob _ -> " BLOB"
        Q.FTime _ -> " TEXT"
    , Q.detailInsertLeft = " OR IGNORE"
    , Q.detailInsertRight = ""
    , Q.detailNullEquality = " IS "
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
    field field field field field field
    {- each `field` is called with a different equality constraint, like (a ~ 'TInt) and (a ~ 'TReal) -}
    {- this makes the compiler to choose the corresponding FromField instance based on the proxy type -}

instance FromField (Q.PrimValue 'Q.TInt) where
    fromField f = (Q.VInt <$> fromField f) `mplus` return Q.VNull

instance FromField (Q.PrimValue 'Q.TReal) where
    fromField f = (Q.VReal <$> fromField f) `mplus` return Q.VNull

instance FromField (Q.PrimValue 'Q.TBool) where
    fromField f = (Q.VBool <$> fromField f) `mplus` return Q.VNull

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
    toField (Q.VBool x) = toField x
    toField (Q.VText x) = toField x
    toField (Q.VBlob x) = toField x
    toField (Q.VTime x) = toField x
    toField Q.VTPosInf = toField ("infinity" :: String)
    toField Q.VTNegInf = toField ("-infinity" :: String)
    toField Q.VNull = SQLNull
