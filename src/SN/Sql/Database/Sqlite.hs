{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE LambdaCase #-}

module SN.Sql.Database.Sqlite
    ( withSqlite
    ) where

import Control.Exception
import Control.Monad
import Data.IORef
import Data.String
import Database.SQLite.Simple
import Database.SQLite.Simple.FromField
import Database.SQLite.Simple.FromRow
import Database.SQLite.Simple.ToField
import Database.SQLite.Simple.Types
import qualified Data.Text as Text
import SN.Control.ResourcePool
import SN.Data.HList
import SN.Logger
import qualified SN.Sql.Database as Db
import qualified SN.Sql.Query as Q
import qualified SN.Sql.Query.Render as Q

withSqlite :: String -> Logger -> (Db.Database -> IO r) -> IO r
withSqlite path logger body = do
    nameCounter <- newIORef (1 :: Int)
    withResourcePool 1
        (if path == "" || path == ":memory:" then 1 else 16)
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
        logDebug logger $ "Sqlite: " <<|| connName << ": " <<|| queryText << "; -- " << hfoldr (\x r -> showLog x << " :/ " << r) "E" queryValues
        let sqlQuery = fromString queryText :: Query
        case queryData of
            Q.CreateTable {} -> sqlitePcall logger connName (execute_ conn sqlQuery) kabort kretry knext
            Q.CreateIndex {} -> sqlitePcall logger connName (execute_ conn sqlQuery) kabort kretry knext
            Q.DropTable {} -> sqlitePcall logger connName (execute_ conn sqlQuery) kabort kretry knext
            Q.Select {} -> sqlitePcall logger connName (queryWith (hlistParser $ Q.selectFields queryData) conn sqlQuery queryValues) kabort kretry knext
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
        Q.FBool _ -> " INTEGER"
        Q.FText _ -> " TEXT"
        Q.FBlob _ -> " BLOB"
        Q.FTime _ -> " TEXT"
    , Q.detailInsertLeft = " OR IGNORE"
    , Q.detailInsertRight = ""
    , Q.detailNullEquality = " IS "
    }

hlistParser :: HList Q.Field ts -> RowParser (HList Maybe ts)
hlistParser E = pure E
hlistParser (h :/ hs) = (:/) <$> fieldWith (fieldParser h) <*> hlistParser hs

fieldParser :: Q.Field a -> FieldParser (Maybe a)
fieldParser (Q.Field prim) f = fmap Q.fromPrim $ primParser prim f

primParser :: Q.PrimField a -> FieldParser (Q.PrimValue a)
primParser _ f | fieldData f == SQLNull = pure Q.VNull
primParser (Q.FInt _) f = fmap Q.VInt (fromField f)
primParser (Q.FBool _) f = fmap Q.VBool (fromField f)
primParser (Q.FText _) f = fmap Q.VText (fromField f)
primParser (Q.FBlob _) f = fmap Q.VBlob (fromField f)
primParser (Q.FTime _) f
    | fd == SQLText "infinity" = pure Q.VTPosInf
    | fd == SQLText "-infinity" = pure Q.VTNegInf
    | otherwise = fmap Q.VTime (fromField f)
  where
    fd = fieldData f

instance ToRow (HList Q.PrimValue ts) where
    toRow = homogenize toField

instance ToField (Q.PrimValue a) where
    toField (Q.VInt x) = toField x
    toField (Q.VBool x) = toField x
    toField (Q.VText x) = toField x
    toField (Q.VBlob x) = toField x
    toField (Q.VTime x) = toField x
    toField Q.VTPosInf = toField ("infinity" :: Text.Text)
    toField Q.VTNegInf = toField ("-infinity" :: Text.Text)
    toField Q.VNull = SQLNull
