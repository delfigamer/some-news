{-# LANGUAGE FlexibleInstances #-}

module Sql.Database.Postgres
    ( withPostgres
    ) where

import Control.Exception
import Control.Monad
import qualified Data.ByteString as BS
import Data.IORef
import Data.Int
import Data.List
import Data.String
import qualified Data.Text as Text
import qualified Data.Text.Encoding as Text
import Data.Time.Clock
import Database.PostgreSQL.Simple
import Database.PostgreSQL.Simple.FromField hiding (Binary)
import Database.PostgreSQL.Simple.FromRow
import Database.PostgreSQL.Simple.ToField
import Database.PostgreSQL.Simple.ToRow
import Database.PostgreSQL.Simple.Types
import qualified Logger
import qualified Sql.Database as Db
import qualified Sql.Query as Q
import qualified Sql.Query.Render as Q
import Tuple

withPostgres :: String -> Logger.Handle -> (Db.Handle -> IO r) -> IO r
withPostgres conf logger body = do
    bracket
        (Logger.debug logger "Postgres: open connection" >> connectPostgreSQL (Text.encodeUtf8 (Text.pack conf)))
        (\conn -> Logger.debug logger "Postgres: close connection" >> close conn)
        (\conn -> do
            body $ Db.Handle
                { Db.queryMaybe = postgresQueryMaybe logger conn
                , Db.foldQuery = undefined
                , Db.withTransaction = postgresWithTransaction logger conn
                })

postgresQueryMaybe :: Logger.Handle -> Connection -> Q.Query result -> IO (Maybe result)
postgresQueryMaybe logger conn queryData = do
    Q.withQueryRender postgresDetailRenderer queryData $ \queryValues queryText -> do
        Logger.debug logger $ "Postgres: " <> Text.pack queryText <> "; -- " <> Text.pack (show queryValues)
        let sqlQuery = fromString queryText :: Query
        eresult <- try $ case queryData of
            Q.CreateTable {} -> do
                _ <- execute_ conn sqlQuery
                return ()
            Q.CreateIndex {} -> do
                _ <- execute_ conn sqlQuery
                return ()
            Q.DropTable {} -> do
                _ <- execute_ conn sqlQuery
                return ()
            Q.Select _ fields _ _ _ -> queryWith (valueParser fields) conn sqlQuery queryValues
            Q.Insert _ _ _ E -> do
                _ <- execute conn sqlQuery queryValues
                return E
            Q.Insert _ _ _ rets -> do
                [r] <- queryWith (valueParser rets) conn sqlQuery queryValues
                return r
            Q.Update _ _ _ _ -> do
                _ <- execute conn sqlQuery queryValues
                return ()
            Q.Delete _ _ -> do
                _ <- execute conn sqlQuery queryValues
                return ()
        case eresult of
            Left err -> do
                Logger.warn logger $ "Postgres: " <> Text.pack (displayException (err :: SomeException))
                return Nothing
            Right result -> return $ Just result

postgresWithTransaction :: Logger.Handle -> Connection -> IO r -> IO r
postgresWithTransaction logger conn act = do
    Logger.debug logger $ "Postgres: BEGIN TRANSACTION"
    r <- withTransaction conn act
    Logger.debug logger $ "Postgres: COMMIT TRANSACTION"
    return r

postgresDetailRenderer :: Q.DetailRenderer
postgresDetailRenderer = Q.DetailRenderer
    { Q.renderFieldType = \field -> case field of
        Q.FInt _ -> " BIGINT"
        Q.FFloat _ -> " REAL"
        Q.FText _ -> " TEXT"
        Q.FBlob _ -> " BYTEA"
        Q.FDateTime _ -> " TIMESTAMPTZ"
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
    fromField f b = (Q.VInt <$> fromField f b) `mplus` return Q.VNull

instance FromField (Q.PrimValue Double) where
    fromField f b = (Q.VFloat <$> fromField f b) `mplus` return Q.VNull

instance FromField (Q.PrimValue Text.Text) where
    fromField f b = (Q.VText <$> fromField f b) `mplus` return Q.VNull

instance FromField (Q.PrimValue BS.ByteString) where
    fromField f b = (Q.VBlob . fromBinary <$> fromField f b) `mplus` return Q.VNull

instance FromField (Q.PrimValue UTCTime) where
    fromField f b = (Q.VDateTime <$> fromField f b) `mplus` return Q.VNull

instance ToRow (TupleT Q.PrimValue ts) where
    toRow = mapTuple toField

instance ToField (Q.PrimValue a) where
    toField (Q.VInt x) = toField x
    toField (Q.VFloat x) = toField x
    toField (Q.VText x) = toField x
    toField (Q.VBlob x) = toField (Binary x)
    toField (Q.VDateTime x) = toField x
    toField Q.VNull = toField Null
