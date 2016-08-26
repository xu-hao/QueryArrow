{-# LANGUAGE GADTs, FlexibleInstances, MultiParamTypeClasses, OverloadedStrings, TypeFamilies #-}
module SQL.HDBC.PostgreSQL where

import DBQuery
import DB
import FO.Data
import SQL.HDBC
import ICAT
import SQL.ICAT
import Config
import SQL.SQL
import Database.HDBC
import Control.Monad.IO.Class

import Database.HDBC.PostgreSQL

data PostgreSQLDB = PostgreSQLDB ICATDBConnInfo

instance GenericDatabase PostgreSQLDB where
    type GenericDatabaseConnectionType PostgreSQLDB = HDBCConnectionDBConnection
    gdbOpen (PostgreSQLDB ps) = do
        conn <- connectPostgreSQL ("host="++db_host ps++ " port="++show (db_port ps)++" dbname="++db_path ps !! 0++" user="++db_username ps++" password="++db_password ps)
        return (HDBCConnectionDBConnection conn)

getDB :: ICATDBConnInfo -> [AbstractDatabase MapResultRow]
getDB ps =
    let db = makeICATSQLDBAdapter (db_name ps !! 0) (Just "nextid") (PostgreSQLDB ps) in
        [AbstractDatabase db]
