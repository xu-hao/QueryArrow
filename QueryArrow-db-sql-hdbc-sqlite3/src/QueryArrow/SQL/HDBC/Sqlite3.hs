{-# LANGUAGE TypeFamilies, FlexibleInstances, TypeSynonymInstances, MultiParamTypeClasses, DeriveGeneric #-}
module QueryArrow.SQL.HDBC.Sqlite3 where

import QueryArrow.DB.GenericDatabase
import QueryArrow.DB.DB
import QueryArrow.Config
import QueryArrow.SQL.HDBC
import QueryArrow.SQL.SQL
import QueryArrow.SQL.ICAT
import QueryArrow.Plugin
import QueryArrow.Semantics.ResultValue.AbstractResultValue
import QueryArrow.Semantics.ResultRow.VectorResultRow
import QueryArrow.Semantics.ResultSet.VectorResultSetTransformer
import QueryArrow.Data.Heterogeneous.List
import QueryArrow.DB.AbstractDatabaseList

import Data.Aeson
import GHC.Generics
import Database.HDBC.Sqlite3
import Data.Maybe

newtype SQLiteDB = SQLiteDB ICATDBConnInfo2

instance IDatabase2 (GenericDatabase  SQLTrans SQLiteDB) where
    newtype ConnectionType (GenericDatabase  SQLTrans SQLiteDB) = SQLite HDBCDBConnection
    dbOpen (GenericDatabase  _ (SQLiteDB ps) _ _) = do
        conn <- connectSqlite3 (db_file_path ps)
        return (SQLite (HDBCDBConnection conn))

instance IDBConnection0 (ConnectionType (GenericDatabase  SQLTrans SQLiteDB)) where
    dbBegin (SQLite conn) = dbBegin conn
    dbPrepare (SQLite conn) = dbPrepare conn
    dbRollback (SQLite conn) = dbRollback conn
    dbCommit (SQLite conn) = dbCommit conn
    dbClose (SQLite conn) = dbClose conn

instance IDBConnection (ConnectionType (GenericDatabase  SQLTrans SQLiteDB)) where
    type QueryType (ConnectionType (GenericDatabase  SQLTrans SQLiteDB)) = QueryType HDBCDBConnection
    type StatementType (ConnectionType (GenericDatabase  SQLTrans SQLiteDB)) = StatementType HDBCDBConnection
    prepareQuery (SQLite conn) = prepareQuery conn

instance IDatabase (GenericDatabase  SQLTrans SQLiteDB)

instance FromJSON ICATDBConnInfo2
instance ToJSON ICATDBConnInfo2

data ICATDBConnInfo2 = ICATDBConnInfo2 {
  db_file_path :: String,
  db_namespace :: String,
  db_predicates :: String,
  db_sql_mapping :: String
} deriving (Show, Generic)

data SQLite3Plugin = SQLite3Plugin

instance Plugin SQLite3Plugin (ResultSetTransformer AbstractResultValue) (VectorResultRow AbstractResultValue) where
  getDB _ _ ps = do
      let fsconf = getDBSpecificConfig ps
      let db = makeICATSQLDBAdapter (db_namespace fsconf) (db_predicates fsconf) (db_sql_mapping fsconf) Nothing (SQLiteDB fsconf)
      AbstractDatabase <$> db
