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
import ResourcePool
import Tuple
import qualified Logger
import qualified Sql.Database as Db
import qualified Sql.Query as Q
import qualified Sql.Query.Render as Q

withPostgres :: String -> Logger.Handle -> (Db.Handle -> IO r) -> IO r
withPostgres conf logger body = do
    let pgconfig = Text.encodeUtf8 $ Text.pack conf
    nameCounter <- newIORef (1 :: Int)
    withResourcePool 4 16
        (do
            newName <- fmap (Text.pack . show) $ atomicModifyIORef' nameCounter $ \x -> (x+1, x)
            Logger.info logger $ "Postgres: " <> newName <> ": open connection"
            newConn <- connectPostgreSQL pgconfig
            return (newName, newConn))
        (\(name, conn) -> do
            Logger.info logger $ "Postgres: " <> name <> ": close connection"
            close conn)
        (\(name, _) err -> do
            Logger.err logger $ "Postgres: " <> name <> ": " <> Text.pack (displayException err))
        (\pool -> do
            body $ Db.Handle
                { Db.makeQuery = \query ->
                    withResource pool $ \(connName, conn) ->
                        postgresMakeQuery logger connName conn query
                , Db.withTransaction = \level body ->
                    withResource pool $ \(connName, conn) ->
                        postgresWithTransaction logger connName conn level body
                })

postgresMakeQuery
    :: Logger.Handle -> Text.Text -> Connection
    -> Q.Query result -> IO (Either Db.QueryError result)
postgresMakeQuery logger connName conn queryData = do
    Q.withQueryRender postgresRenderDetail queryData $ \queryValues queryText -> do
        Logger.debug logger $ "Postgres: " <> connName <> ": " <> Text.pack queryText <> "; -- " <> Text.pack (Q.showPrimValues 0 queryValues "")
        let sqlQuery = fromString queryText :: Query
        case queryData of
            Q.CreateTable {} -> postgresPcall_ logger connName $ execute_ conn sqlQuery
            Q.CreateIndex {} -> postgresPcall_ logger connName $ execute_ conn sqlQuery
            Q.DropTable {} -> postgresPcall_ logger connName $ execute_ conn sqlQuery
            Q.Select {} -> postgresPcall logger connName $ query conn sqlQuery queryValues
            Q.Insert _ _ _ E -> do
                n <- postgresPcall logger connName $ execute conn sqlQuery queryValues
                return $ fmap foldInsertE n
            Q.Insert {} -> do
                rets <- postgresPcall logger connName $ query conn sqlQuery queryValues
                return $ fmap foldInsert rets
            Q.Update {} -> postgresPcall logger connName $ execute conn sqlQuery queryValues
            Q.Delete {} -> postgresPcall logger connName $ execute conn sqlQuery queryValues
  where
    foldInsertE 1 = Just E
    foldInsertE _ = Nothing
    foldInsert [r] = Just r
    foldInsert _ = Nothing

postgresWithTransaction
    :: Logger.Handle -> Text.Text -> Connection
    -> Db.TransactionLevel -> (Db.Handle -> IO (Either Db.QueryError r)) -> IO (Either Db.QueryError r)
postgresWithTransaction logger connName conn level body = do
    tresult <- postgresPcall logger connName $ do
        withCancelableTransaction $ do
            body $ Db.Handle
                { Db.makeQuery = postgresMakeQuery logger connName conn
                , Db.withTransaction = error "Sql.Database.Postgres: nested transaction"
                }
    case tresult of
        Left transFailure -> return $ Left transFailure
        Right (Left userException) -> throwIO (userException :: SomeException)
        Right (Right eresult) -> return eresult
  where
    withCancelableTransaction act = do
        mask $ \restore -> do
            case level of
                Db.ReadCommited -> doQuery "BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED"
                Db.RepeatableRead -> doQuery "BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ"
                Db.Serializable -> doQuery "BEGIN TRANSACTION ISOLATION LEVEL SERIALIZABLE"
            aresult <- try $ restore act
            case aresult of
                Right (Right _) -> doQuery "COMMIT TRANSACTION"
                _ -> doQuery "ROLLBACK TRANSACTION"
            return aresult
    doQuery str = do
        Logger.debug logger $ "Postgres: " <> connName <> ": " <> Text.pack str
        execute_ conn (fromString str)

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

postgresPcall :: Logger.Handle -> Text.Text -> IO a -> IO (Either Db.QueryError a)
postgresPcall logger connName act = do
    eret <- try act
    case eret of
        Right r -> return $ Right r
        Left ex -> do
            let ecode = case fromException ex of
                    Nothing -> Db.QueryError
                    Just SqlError {sqlState = state} -> if BS.isPrefixOf "40" state
                        then Db.SerializationError
                        else Db.QueryError
            case ecode of
                Db.SerializationError -> Logger.debug logger $
                    "Postgres: " <> connName <> ": Serialization failure: " <> Text.pack (displayException ex)
                Db.QueryError -> Logger.warn logger $
                    "Postgres: " <> connName <> ": Error: " <> Text.pack (displayException ex)
            return $ Left ecode

postgresPcall_ :: Logger.Handle -> Text.Text -> IO a -> IO (Either Db.QueryError ())
postgresPcall_ logger connName act = do
    r <- postgresPcall logger connName act
    return $ r >> Right ()

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
