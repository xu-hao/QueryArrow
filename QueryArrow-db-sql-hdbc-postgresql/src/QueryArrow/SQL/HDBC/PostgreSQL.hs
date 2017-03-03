{-# LANGUAGE GADTs, FlexibleInstances, MultiParamTypeClasses, OverloadedStrings, TypeFamilies, DeriveGeneric #-}
module QueryArrow.SQL.HDBC.PostgreSQL where

import QueryArrow.DB.DB
import QueryArrow.SQL.HDBC
import QueryArrow.DB.GenericDatabase
import QueryArrow.SQL.ICAT
import QueryArrow.Config
import QueryArrow.SQL.SQL
import Data.Aeson
import GHC.Generics
import QueryArrow.Plugin
import QueryArrow.Data.Heterogeneous.List
import QueryArrow.DB.AbstractDatabaseList
import Data.Maybe

import Database.HDBC.PostgreSQL

newtype PostgreSQLDB = PostgreSQLDB PostgreSQLDBConfig

instance IDatabase2 (GenericDatabase  SQLTrans PostgreSQLDB) where
    newtype ConnectionType (GenericDatabase  SQLTrans PostgreSQLDB) = P HDBCDBConnection
    dbOpen (GenericDatabase  _ (PostgreSQLDB ps) _ _) = do
        conn <- connectPostgreSQL ("host="++db_host ps++ " port="++show (db_port ps)++" dbname="++db_name ps++" user="++db_username ps++" password="++db_password ps)
        return (P (HDBCDBConnection conn))

instance IDBConnection0 (ConnectionType (GenericDatabase  SQLTrans PostgreSQLDB)) where
    dbBegin (P conn) = dbBegin conn
    dbPrepare (P conn) = dbPrepare conn
    dbRollback (P conn) = dbRollback conn
    dbCommit (P conn) = dbCommit conn
    dbClose (P conn) = dbClose conn

instance IDBConnection (ConnectionType (GenericDatabase  SQLTrans PostgreSQLDB)) where
    type QueryType (ConnectionType (GenericDatabase  SQLTrans PostgreSQLDB)) = QueryType HDBCDBConnection
    type StatementType (ConnectionType (GenericDatabase  SQLTrans PostgreSQLDB)) = StatementType HDBCDBConnection
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
  db_sql_mapping :: String
} deriving (Show, Generic)

data PostgreSQLPlugin = PostgreSQLPlugin

instance Plugin PostgreSQLPlugin MapResultRow where
  getDB _ _ ps = do
      let fsconf = getDBSpecificConfig ps
      db <- makeICATSQLDBAdapter (db_namespace fsconf) (db_predicates fsconf) (db_sql_mapping fsconf) (Just "nextid") (PostgreSQLDB fsconf)
      return (AbstractDatabase db)
