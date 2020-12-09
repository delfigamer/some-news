{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE LambdaCase #-}

module SN.Sql.Database.Postgres
    ( withPostgres
    ) where

import Control.Exception
import Control.Monad
import Data.IORef
import Data.String
import Database.PostgreSQL.Simple
import Database.PostgreSQL.Simple.FromField hiding (Binary)
import Database.PostgreSQL.Simple.FromRow
import Database.PostgreSQL.Simple.ToField
import Database.PostgreSQL.Simple.ToRow
import Database.PostgreSQL.Simple.Types
import qualified Data.ByteString as BS
import qualified Data.Text as Text
import qualified Data.Text.Encoding as Text
import SN.Control.ResourcePool
import SN.Data.HList
import SN.Logger
import qualified SN.Sql.Database as Db
import qualified SN.Sql.Query as Q
import qualified SN.Sql.Query.Render as Q

type AbortRetryCont a = forall r. IO r -> IO r -> (a -> IO r) -> IO r

withPostgres :: String -> Logger -> (Db.Database -> IO r) -> IO r
withPostgres conf logger body = do
    let pgconfig = Text.encodeUtf8 $ Text.pack conf
    nameCounter <- newIORef (1 :: Int)
    withResourcePool 4 16
        (do
            newName <- fmap (Text.pack . show) $ atomicModifyIORef' nameCounter $ \x -> (x+1, x)
            logInfo logger $ "Postgres: " <<|| newName << ": open connection"
            newConn <- connectPostgreSQL pgconfig
            return (newName, newConn))
        (\(name, conn) -> do
            logInfo logger $ "Postgres: " <<|| name << ": close connection"
            close conn)
        (\(name, _) err -> do
            logErr logger $ "Postgres: " <<|| name << ": " <<|| displayException err)
        (\pool -> do
            body $ Db.Database
                { Db.execute = \level body ->
                    withResource pool $ \(connName, conn) ->
                        postgresExecute logger connName conn level body
                })

postgresExecute
    :: Logger -> Text.Text -> Connection
    -> Db.TransactionLevel -> Db.Transaction r r -> IO (Either Db.QueryError r)
postgresExecute logger connName conn level body = do
    postgresPcall logger connName
        (withCancelableTransaction $ do
            Db.runTransactionBody body
                (postgresHandleQuery logger connName conn)
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
            case level of
                Db.ReadCommited -> doQuery "BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED"
                Db.RepeatableRead -> doQuery "BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ"
                Db.Serializable -> doQuery "BEGIN TRANSACTION ISOLATION LEVEL SERIALIZABLE"
            aresult <- try $ restore act
            case aresult of
                Right (True, _) -> doQuery "COMMIT TRANSACTION"
                _ -> doQuery "ROLLBACK TRANSACTION"
            return aresult
    doQuery str = do
        logDebug logger $ "Postgres: " <<|| connName << ": " <<|| str
        execute_ conn (fromString str)

postgresHandleQuery
    :: Logger -> Text.Text -> Connection
    -> Q.Query result -> AbortRetryCont result
postgresHandleQuery logger connName conn queryData kabort kretry knext = do
    Q.withQueryRender postgresRenderDetail queryData $ \queryValues queryText -> do
        logDebug logger $ "Postgres: " <<|| connName << ": " <<|| queryText << "; -- " << hfoldr (\x r -> showLog x << " :/ " << r) "E" queryValues
        let sqlQuery = fromString queryText :: Query
        case queryData of
            Q.CreateTable {} -> postgresPcall_ logger connName (execute_ conn sqlQuery) kabort kretry knext
            Q.CreateIndex {} -> postgresPcall_ logger connName (execute_ conn sqlQuery) kabort kretry knext
            Q.DropTable {} -> postgresPcall_ logger connName (execute_ conn sqlQuery) kabort kretry knext
            Q.Select {} -> postgresPcall logger connName (queryWith (hlistParser $ Q.selectFields queryData) conn sqlQuery queryValues) kabort kretry knext
            Q.Insert {} -> postgresPcall logger connName (execute conn sqlQuery queryValues) kabort kretry $ \case
                0 -> knext False
                1 -> knext True
                _ -> kabort
            Q.Update {} -> postgresPcall logger connName (execute conn sqlQuery queryValues) kabort kretry knext
            Q.Delete {} -> postgresPcall logger connName (execute conn sqlQuery queryValues) kabort kretry knext

postgresPcall :: Logger -> Text.Text -> IO a -> AbortRetryCont a
postgresPcall logger connName act kabort kretry knext = do
    eret <- try act
    case eret of
        Right r -> knext r
        Left ex -> do
            let canRetry = case fromException ex of
                    Nothing -> False
                    Just SqlError {sqlState = state} -> if BS.isPrefixOf "40" state
                        then True
                        else False
            if canRetry
                then do
                    logDebug logger $ "Postgres: " <<|| connName << ": Serialization failure: " <<|| displayException ex
                    kretry
                else do
                    logWarn logger $ "Postgres: " <<|| connName << ": Error: " <<|| displayException ex
                    kabort

postgresPcall_ :: Logger -> Text.Text -> IO a -> AbortRetryCont ()
postgresPcall_ logger connName act kabort kretry knext = do
    postgresPcall logger connName act kabort kretry (\_ -> knext ())

postgresRenderDetail :: Q.RenderDetail
postgresRenderDetail = Q.RenderDetail
    { Q.detailFieldType = \case
        Q.FInt _ -> " BIGINT"
        Q.FBool _ -> " BOOLEAN"
        Q.FText _ -> " TEXT COLLATE ucs_basic"
        Q.FBlob _ -> " BYTEA"
        Q.FTime _ -> " TIMESTAMPTZ"
    , Q.detailInsertLeft = ""
    , Q.detailInsertRight = " ON CONFLICT DO NOTHING"
    , Q.detailNullEquality = " IS NOT DISTINCT FROM "
    }

hlistParser :: HList Q.Field ts -> RowParser (HList Maybe ts)
hlistParser E = pure E
hlistParser (h :/ hs) = (:/) <$> fieldWith (fieldParser h) <*> hlistParser hs

fieldParser :: Q.Field a -> FieldParser (Maybe a)
fieldParser (Q.Field prim) f b = fmap Q.fromPrim $ primParser prim f b

primParser :: Q.PrimField a -> FieldParser (Q.PrimValue a)
primParser _ _ Nothing = pure Q.VNull
primParser (Q.FInt _) f b = fmap Q.VInt (fromField f b)
primParser (Q.FBool _) f b = fmap Q.VBool (fromField f b)
primParser (Q.FText _) f b = fmap Q.VText (fromField f b)
primParser (Q.FBlob _) f b = fmap Q.VBlob (fromField f b)
primParser (Q.FTime _) f (Just "infinity") = pure Q.VTPosInf
primParser (Q.FTime _) f (Just "-infinity") = pure Q.VTNegInf
primParser (Q.FTime _) f b = fmap Q.VTime (fromField f b)

instance ToRow (HList Q.PrimValue ts) where
    toRow = homogenize toField

instance ToField (Q.PrimValue a) where
    toField (Q.VInt x) = toField x
    toField (Q.VBool x) = toField x
    toField (Q.VText x) = toField x
    toField (Q.VBlob x) = toField (Binary x)
    toField (Q.VTime x) = toField x
    toField Q.VTPosInf = toField ("infinity" :: Text.Text)
    toField Q.VTNegInf = toField ("-infinity" :: Text.Text)
    toField Q.VNull = toField Null
