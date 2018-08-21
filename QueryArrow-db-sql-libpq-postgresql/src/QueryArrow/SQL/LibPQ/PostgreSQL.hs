{-# LANGUAGE GADTs, FlexibleInstances, MultiParamTypeClasses, OverloadedStrings, TypeFamilies, DeriveGeneric, OverloadedStrings #-}
module QueryArrow.SQL.LibPQ.PostgreSQL where

import QueryArrow.DB.DB
import QueryArrow.DB.GenericDatabase
import QueryArrow.SQL.Mapping
import QueryArrow.Config
import QueryArrow.SQL.SQL
import Data.Aeson
import GHC.Generics
import QueryArrow.Plugin
import QueryArrow.DB.AbstractDatabaseList
import Data.Maybe
import Data.IORef
import Data.Monoid ((<>))
import Data.ByteString.Char8 (pack, ByteString)

import QueryArrow.SQL.LibPQ
import Database.PostgreSQL.LibPQ

newtype PostgreSQLDB = PostgreSQLDB PostgreSQLDBConfig

instance IDatabase2 (GenericDatabase  SQLTrans PostgreSQLDB) where
    newtype ConnectionType (GenericDatabase  SQLTrans PostgreSQLDB) = P LibPQDBConnection
    dbOpen (GenericDatabase  _ (PostgreSQLDB ps) _ _) = do
        conn <- connectdb ("host=" <> pack (db_host ps) <> " port=" <> pack (show (db_port ps)) <> " dbname=" <> pack (db_name ps) <> " user=" <> pack (db_username ps) <> " password=" <> pack (db_password ps))
        sid <- newIORef 0
        return (P (LibPQDBConnection  conn (db_two_phase_commit ps) sid))

instance IDBConnection0 (ConnectionType (GenericDatabase  SQLTrans PostgreSQLDB)) where
    dbBegin (P conn) = dbBegin conn
    dbPrepare (P conn) = dbPrepare conn
    dbRollback (P conn) = dbRollback conn
    dbCommit (P conn) = dbCommit conn
    dbClose (P conn) = dbClose conn

instance IDBConnection (ConnectionType (GenericDatabase  SQLTrans PostgreSQLDB)) where
    type QueryType (ConnectionType (GenericDatabase  SQLTrans PostgreSQLDB)) = QueryType LibPQDBConnection
    type StatementType (ConnectionType (GenericDatabase  SQLTrans PostgreSQLDB)) = StatementType LibPQDBConnection
    prepareQuery (P conn) = prepareQuery conn

instance IDatabase (GenericDatabase  SQLTrans PostgreSQLDB) where


instance FromJSON PostgreSQLDBConfig
instance ToJSON PostgreSQLDBConfig

data PostgreSQLDBConfig = PostgreSQLDBConfig {
  db_name :: String,
  db_namespace :: String,
  db_host :: String,
  db_password :: String,
  db_port :: Int,
  db_username :: String,
  db_predicates :: String,
  db_sql_mapping :: String,
  db_two_phase_commit :: Bool
} deriving (Show, Generic)

data PostgreSQLPlugin = PostgreSQLPlugin

instance Plugin PostgreSQLPlugin MapResultRow where
  getDB _ _ ps = do
      let fsconf = getDBSpecificConfig ps
      db <- makeICATSQLDBAdapter (db_namespace fsconf) (db_predicates fsconf) (db_sql_mapping fsconf) (Just "nextid") (PostgreSQLDB fsconf)
      return (AbstractDatabase db)
