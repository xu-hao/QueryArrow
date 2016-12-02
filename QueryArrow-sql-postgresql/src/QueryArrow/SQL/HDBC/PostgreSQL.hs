{-# LANGUAGE GADTs, FlexibleInstances, MultiParamTypeClasses, OverloadedStrings, TypeFamilies #-}
module QueryArrow.SQL.HDBC.PostgreSQL where

import QueryArrow.FO.Data
import QueryArrow.DB.DB
import QueryArrow.SQL.HDBC
import QueryArrow.DB.GenericDatabase
import QueryArrow.SQL.ICAT
import QueryArrow.Config
import QueryArrow.SQL.SQL

import Database.HDBC.PostgreSQL

newtype PostgreSQLDB = PostgreSQLDB ICATDBConnInfo

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

getDB :: ICATDBConnInfo -> IO (AbstractDatabase MapResultRow Formula)
getDB ps =
    let db = makeICATSQLDBAdapter (db_namespace ps) (db_icat ps) (Just "nextid") (PostgreSQLDB ps) in
        AbstractDatabase <$> db