{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
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
import ResourcePool
import Tuple
import qualified Logger
import qualified Sql.Database as Db
import qualified Sql.Query as Q
import qualified Sql.Query.Render as Q

withSqlite :: String -> Logger.Handle -> (Db.Handle -> IO r) -> IO r
withSqlite path logger body = do
    nameCounter <- newIORef (1 :: Int)
    withResourcePool 1
        (if path == "" || path == ":memory:"then 1 else 16)
        (do
            newName <- fmap (Text.pack . show) $ atomicModifyIORef' nameCounter $ \x -> (x+1, x)
            Logger.info logger $ "Sqlite: " <> newName <> ": open connection"
            newConn <- initialize path
            return (newName, newConn))
        (\(name, conn) -> do
            Logger.info logger $ "Sqlite: " <> name <> ": close connection"
            close conn)
        (\(name, _) err -> do
            Logger.err logger $ "Sqlite: " <> name <> ": " <> Text.pack (displayException err))
        (\pool -> do
            body $ Db.Handle
                { Db.makeQuery  = \query ->
                    withResource pool $ \(connName, conn) ->
                        sqliteMakeQuery logger connName conn query
                , Db.withTransaction = \level body ->
                    withResource pool $ \(connName, conn) ->
                        sqliteWithTransaction logger connName conn level body
                })

initialize :: String -> IO Connection
initialize path = do
    conn <- open path
    execute_ conn "PRAGMA foreign_keys = ON"
    execute_ conn "PRAGMA journal_mode = WAL"
    return conn

sqliteMakeQuery
    :: Logger.Handle -> Text.Text -> Connection
    -> Q.Query result -> IO (Either Db.QueryError result)
sqliteMakeQuery logger connName conn queryData = do
    Q.withQueryRender sqliteRenderDetail queryData $ \queryValues queryText -> do
        Logger.debug logger $ "Sqlite: " <> connName <> ": " <> Text.pack queryText <> "; -- " <> Text.pack (Q.showPrimValues 0 queryValues "")
        let sqlQuery = fromString queryText :: Query
        case queryData of
            Q.CreateTable {} -> sqlitePcall logger connName $ execute_ conn sqlQuery
            Q.CreateIndex {} -> sqlitePcall logger connName $ execute_ conn sqlQuery
            Q.DropTable {} -> sqlitePcall logger connName $ execute_ conn sqlQuery
            Q.Select {} -> sqlitePcall logger connName $ query conn sqlQuery queryValues
            Q.Insert _ _ _ E -> sqlitePcall logger connName $ do
                execute conn sqlQuery queryValues
                n <- changes conn
                case n of
                    0 -> return Nothing
                    _ -> return (Just E)
            Q.Insert table fields values rets -> sqlitePcall logger connName $ do
                sqliteInsertReturning logger connName conn table fields values rets
            Q.Update {} -> sqlitePcall logger connName $ do
                execute conn sqlQuery queryValues
                fromIntegral <$> changes conn
            Q.Delete {} -> sqlitePcall logger connName $ do
                execute conn sqlQuery queryValues
                fromIntegral <$> changes conn

sqliteInsertReturning
    :: (All Q.IsValue rs, AllWith Q.Value Show vs)
    => Logger.Handle -> Text.Text -> Connection
    -> Q.TableName -> HList Q.Field vs -> HList Q.Value vs -> HList Q.Field rs -> IO (Maybe (HList Maybe rs))
sqliteInsertReturning logger connName conn table fields values rets = do
    let queryData1 = Q.Insert table fields values E
    Q.withQueryRender sqliteRenderDetail queryData1 $ \queryValues1 queryText1 -> do
        Logger.debug logger $ "Sqlite: " <> connName <> ": " <> Text.pack queryText1 <> "; -- " <> Text.pack (Q.showPrimValues 0 queryValues1 "")
        execute conn (fromString queryText1) queryValues1
        n <-changes conn
        case n of
            0 -> return Nothing
            _ -> do
                lastRowId <- lastInsertRowId conn
                let queryData2 = Q.Select [Q.TableSource table] rets [Q.WhereWith lastRowId "_rowid_ = ?"] [] Q.AllRows
                Q.withQueryRender sqliteRenderDetail queryData2 $ \queryValues2 queryText2 -> do
                    Logger.debug logger $ "Sqlite: " <> connName <> ": " <> Text.pack queryText2 <> "; -- " <> Text.pack (Q.showPrimValues 0 queryValues2 "")
                    rets <- query conn (fromString queryText2) queryValues2
                    case rets of
                        [result] -> return $ Just result
                        _ -> return Nothing

sqliteWithTransaction
    :: Logger.Handle -> Text.Text -> Connection
    -> Db.TransactionLevel -> (Db.Handle -> IO (Either Db.QueryError r)) -> IO (Either Db.QueryError r)
sqliteWithTransaction logger connName conn _ body = do
    tresult <- sqlitePcall logger connName $ do
        withCancelableTransaction $ do
            body $ Db.Handle
                { Db.makeQuery = sqliteMakeQuery logger connName conn
                , Db.withTransaction = error "Sql.Database.Sqlite: nested transaction"
                }
    case tresult of
        Left transFailure -> return $ Left transFailure
        Right (Left userException) -> throwIO (userException :: SomeException)
        Right (Right eresult) -> return eresult
  where
    withCancelableTransaction act = do
        mask $ \restore -> do
            doQuery "BEGIN TRANSACTION"
            aresult <- try $ restore act
            case aresult of
                Right (Right _) -> doQuery "COMMIT TRANSACTION"
                _ -> doQuery "ROLLBACK TRANSACTION"
            return aresult
    doQuery str = do
        Logger.debug logger $ "Sqlite: " <> connName <> ": " <> Text.pack str
        execute_ conn (fromString str)

sqliteRenderDetail :: Q.RenderDetail
sqliteRenderDetail = Q.RenderDetail
    { Q.detailFieldType = \field -> case field of
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

sqlitePcall :: Logger.Handle -> Text.Text -> IO a -> IO (Either Db.QueryError a)
sqlitePcall logger connName act = do
    eret <- try act
    case eret of
        Right r -> return $ Right r
        Left ex -> do
            let ecode = case fromException ex of
                    Nothing -> Db.QueryError
                    Just SQLError {sqlError = se} -> case se of
                        ErrorBusy -> Db.SerializationError
                        ErrorLocked -> Db.SerializationError
                        _ -> Db.QueryError
            case ecode of
                Db.SerializationError -> Logger.debug logger $
                    "Sqlite: " <> connName <> ": Serialization failure: " <> Text.pack (displayException ex)
                Db.QueryError -> Logger.warn logger $
                    "Sqlite: " <> connName <> ": Error: " <> Text.pack (displayException ex)
            return $ Left ecode

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
