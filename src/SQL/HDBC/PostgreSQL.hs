{-# LANGUAGE GADTs, FlexibleInstances, MultiParamTypeClasses, OverloadedStrings, TypeFamilies #-}
module SQL.HDBC.PostgreSQL where

import DB.DB
import FO.Data
import SQL.HDBC
import ICAT
import DB.GenericDatabase
import SQL.ICAT
import Config
import SQL.SQL
import Database.HDBC
import Control.Monad.IO.Class

import Database.HDBC.PostgreSQL

newtype PostgreSQLDB = PostgreSQLDB ICATDBConnInfo

instance IDatabase2 (GenericDatabase  SQLTrans PostgreSQLDB) where
    type ConnectionType (GenericDatabase  SQLTrans PostgreSQLDB) = HDBCDBConnection
    dbOpen (GenericDatabase  _ (PostgreSQLDB ps) _ _) = do
        conn <- connectPostgreSQL ("host="++db_host ps++ " port="++show (db_port ps)++" dbname="++db_name ps++" user="++db_username ps++" password="++db_password ps)
        return (HDBCDBConnection conn)

instance IDatabase (GenericDatabase  SQLTrans PostgreSQLDB) where

getDB :: ICATDBConnInfo -> [AbstractDatabase MapResultRow]
getDB ps =
    let db = makeICATSQLDBAdapter (db_namespace ps) (Just "nextid") (PostgreSQLDB ps) in
        [AbstractDatabase db]
