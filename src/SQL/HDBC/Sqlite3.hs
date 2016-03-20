{-# LANGUAGE GADTs, TypeFamilies, TypeSynonymInstances, MultiParamTypeClasses, FlexibleInstances #-}
module SQL.HDBC.Sqlite3 where

import DBQuery
import Config
import QueryPlan
import FO.Data
import SQL.HDBC
import ICAT
import SQL.ICAT

import Database.HDBC.Sqlite3

instance HDBCConnection Connection where
        showSQLQuery _ (vars, query, params) = show query
        showSQLInsert _ (query, params)= show query

getDB :: ICATDBConnInfo -> IO [Database DBAdapterMonad MapResultRow]
getDB ps = do
    conn <- connectSqlite3 (db_name ps)
    let db = makeICATSQLDBAdapter conn
    return [Database db]
