{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Sql.Database.Postgres
    ( withPostgres
    ) where

import Control.Exception
import Control.Monad
import qualified Data.ByteString as BS
import Data.IORef
import Data.Int
import Data.List
import Data.Proxy
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
                , Db.foldQuery = postgresFoldQuery logger conn
                , Db.withTransaction = postgresWithTransaction logger conn
                })

postgresQueryMaybe :: Logger.Handle -> Connection -> Q.Query result -> IO (Maybe result)
postgresQueryMaybe logger conn queryData = do
    Q.withQueryRender postgresDetailRenderer queryData $ \queryValues queryText -> do
        Logger.debug logger $ "Postgres: " <> Text.pack queryText <> "; -- " <> Text.pack (Q.showPrimValues 0 queryValues "")
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
            Q.Select {} -> query conn sqlQuery queryValues
            Q.Insert _ _ _ E -> do
                n <- execute conn sqlQuery queryValues
                case n of
                    1 -> return $ Just E
                    _ -> return Nothing
            Q.Insert {} -> do
                rets <- query conn sqlQuery queryValues
                case rets of
                    [result] -> return $ Just result
                    _ -> return Nothing
            Q.Update {} -> do
                fromIntegral <$> execute conn sqlQuery queryValues
            Q.Delete {} -> do
                fromIntegral <$> execute conn sqlQuery queryValues
        case eresult of
            Left err -> do
                Logger.warn logger $ "Postgres: " <> Text.pack (displayException (err :: SomeException))
                return Nothing
            Right result -> return $ Just result

postgresFoldQuery :: Logger.Handle -> Connection -> Q.Query [row] -> a -> (a -> row -> IO a) -> IO a
postgresFoldQuery logger conn queryData seed foldf = do
    Q.withQueryRender postgresDetailRenderer queryData $ \queryValues queryText -> do
        Logger.debug logger $ "Postgres: (streaming) " <> Text.pack queryText <> "; -- " <> Text.pack (Q.showPrimValues 0 queryValues "")
        let sqlQuery = fromString queryText :: Query
        case queryData of
            Q.Select {} -> fold conn sqlQuery queryValues seed foldf

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
        Q.FReal _ -> " REAL"
        Q.FText _ -> " TEXT"
        Q.FBlob _ -> " BYTEA"
        Q.FTime _ -> " TIMESTAMPTZ"
    , Q.renderInsertPart = \inner -> "INSERT INTO " ++ inner ++ " ON CONFLICT DO NOTHING"
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
    fromField f b = (Q.VInt <$> fromField f b) `mplus` return Q.VNull

instance FromField (Q.PrimValue 'Q.TReal) where
    fromField f b = (Q.VReal <$> fromField f b) `mplus` return Q.VNull

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
    toField (Q.VText x) = toField x
    toField (Q.VBlob x) = toField (Binary x)
    toField (Q.VTime x) = toField x
    toField Q.VTPosInf = toField ("infinity" :: String)
    toField Q.VTNegInf = toField ("-infinity" :: String)
    toField Q.VNull = toField Null
