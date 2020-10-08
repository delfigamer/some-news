{-# LANGUAGE FlexibleInstances #-}

module Sql.Database.Postgres
    ( withPostgres
    ) where

import Control.Exception
import Data.IORef
import Data.List
import Data.String
import qualified Data.Text as Text
import Database.SQLite.Simple
import Database.SQLite.Simple.FromRow
import Database.SQLite.Simple.ToField
import qualified Logger
import qualified Sql.Database as Db
import qualified Sql.Query as Q
import Tuple

withPostgres :: String -> Logger.Handle -> (Db.Handle -> IO r) -> IO r
withPostgres path logger body = do
    bracket
        (Logger.info logger "Postgres: open connection" >> open path)
        (\conn -> Logger.info logger "Postgres: close connection" >> close conn)
        (\conn -> do
            body $ Db.Handle
                { Db.queryMaybe = postgresQueryMaybe logger conn
                , Db.withTransaction = postgresWithTransaction logger conn
                })

postgresQueryMaybe :: Logger.Handle -> Connection -> Q.Query result -> IO (Maybe result)
postgresQueryMaybe logger conn queryData = do
    let queryText = Db.renderQueryTemplate queryData
    Logger.debug logger $ "Postgres: " <> Text.pack (show queryData)
    Logger.info logger $ "Postgres: " <> Text.pack queryText
    let sqlQuery = fromString queryText :: Query
    eresult <- try $ case queryData of
        Q.CreateTable {} -> execute_ conn sqlQuery
        Q.AddTableColumn {} -> execute_ conn sqlQuery
        Q.DropTable {} -> execute_ conn sqlQuery
        Q.Select _ fields mcond _ _ -> Db.withConditionValues mcond $ queryWith (tupleParser fields) conn sqlQuery
        Q.Insert _ _ rows -> executeMany conn sqlQuery rows
        Q.InsertReturning _ _ values rets -> do
            [r] <- queryWith (tupleParser rets) conn sqlQuery values
            return r
        Q.Update _ _ values mcond -> Db.withConditionValues mcond $ \condvals -> execute conn sqlQuery $ joinTuple values condvals
        Q.Delete _ mcond -> Db.withConditionValues mcond $ execute conn sqlQuery
    case eresult of
        Left err -> do
            Logger.warn logger $ "Postgres: " <> Text.pack (displayException (err :: SomeException))
            return Nothing
        Right result -> return $ Just result

postgresWithTransaction :: Logger.Handle -> Connection -> IO r -> IO r
postgresWithTransaction logger conn act = do
    Logger.info logger $ "Postgres: BEGIN TRANSACTION"
    r <- withTransaction conn act
    Logger.info logger $ "Postgres: COMMIT TRANSACTION"
    return r

tupleParser :: TupleT Q.Field ts -> RowParser (TupleT Q.Value ts)
tupleParser E = return E
tupleParser (Q.FInt _ :* xs) = (:*) . Q.VInt <$> field <*> tupleParser xs
tupleParser (Q.FString _ :* xs) = (:*) . Q.VString <$> field <*> tupleParser xs
tupleParser (Q.FText _ :* xs) = (:*) . Q.VText <$> field <*> tupleParser xs

instance ToRow (TupleT Q.Value ts) where
    toRow = mapTuple toField

instance ToField (Q.Value a) where
    toField (Q.VInt x) = toField x
    toField (Q.VString x) = toField x
    toField (Q.VText x) = toField x
