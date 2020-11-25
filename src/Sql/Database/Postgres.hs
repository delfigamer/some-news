{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Sql.Database.Postgres
    ( withPostgres
    ) where

import Control.Exception
import Control.Monad
import Data.IORef
import Data.Int
import Data.List
import Data.Proxy
import Data.String
import Data.Time.Clock
import Database.PostgreSQL.Simple
import Database.PostgreSQL.Simple.FromField hiding (Binary)
import Database.PostgreSQL.Simple.FromRow
import Database.PostgreSQL.Simple.ToField
import Database.PostgreSQL.Simple.ToRow
import Database.PostgreSQL.Simple.Types
import System.IO.Unsafe
import qualified Data.ByteString as BS
import qualified Data.Text as Text
import qualified Data.Text.Encoding as Text
import Logger
import ResourcePool
import Tuple
import qualified Sql.Database as Db
import qualified Sql.Query as Q
import qualified Sql.Query.Render as Q

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
        logDebug logger $ "Postgres: " <<|| connName << ": " <<|| queryText << "; -- " <<|| Q.showPrimValues 0 queryValues ""
        let sqlQuery = fromString queryText :: Query
        case queryData of
            Q.CreateTable {} -> postgresPcall_ logger connName (execute_ conn sqlQuery) kabort kretry knext
            Q.CreateIndex {} -> postgresPcall_ logger connName (execute_ conn sqlQuery) kabort kretry knext
            Q.DropTable {} -> postgresPcall_ logger connName (execute_ conn sqlQuery) kabort kretry knext
            Q.Select {} -> postgresPcall logger connName (query conn sqlQuery queryValues) kabort kretry knext
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

postgresPcall_ :: Logger.Logger -> Text.Text -> IO a -> AbortRetryCont ()
postgresPcall_ logger connName act kabort kretry knext = do
    postgresPcall logger connName act kabort kretry (\_ -> knext ())

postgresRenderDetail :: Q.RenderDetail
postgresRenderDetail = Q.RenderDetail
    { Q.detailFieldType = \case
        Q.FInt _ -> " BIGINT"
        Q.FReal _ -> " REAL"
        Q.FBool _ -> " BOOLEAN"
        Q.FText _ -> " TEXT COLLATE ucs_basic"
        Q.FBlob _ -> " BYTEA"
        Q.FTime _ -> " TIMESTAMPTZ"
    , Q.detailInsertLeft = ""
    , Q.detailInsertRight = " ON CONFLICT DO NOTHING"
    , Q.detailNullEquality = " IS NOT DISTINCT FROM "
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
    fromField f b = (Q.VInt <$> fromField f b) `mplus` return Q.VNull

instance FromField (Q.PrimValue 'Q.TReal) where
    fromField f b = (Q.VReal <$> fromField f b) `mplus` return Q.VNull

instance FromField (Q.PrimValue 'Q.TBool) where
    fromField f b = (Q.VBool <$> fromField f b) `mplus` return Q.VNull

instance FromField (Q.PrimValue 'Q.TText) where
    fromField f b = (Q.VText <$> fromField f b) `mplus` return Q.VNull

instance FromField (Q.PrimValue 'Q.TBlob) where
    fromField f b = (Q.VBlob . fromBinary <$> fromField f b) `mplus` return Q.VNull

instance FromField (Q.PrimValue 'Q.TTime) where
    fromField f b = (Q.VTime <$> fromField f b) `mplus` return Q.VNull

instance ToRow (HList Q.PrimValue ts) where
    toRow = homogenize toField

instance ToField (Q.PrimValue a) where
    toField (Q.VInt x) = toField x
    toField (Q.VReal x) = toField x
    toField (Q.VBool x) = toField x
    toField (Q.VText x) = toField x
    toField (Q.VBlob x) = toField (Binary x)
    toField (Q.VTime x) = toField x
    toField Q.VTPosInf = toField ("infinity" :: String)
    toField Q.VTNegInf = toField ("-infinity" :: String)
    toField Q.VNull = toField Null
