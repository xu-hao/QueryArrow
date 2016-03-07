{-# LANGUAGE GADTs, TypeFamilies, TypeSynonymInstances, MultiParamTypeClasses, FlexibleInstances #-}
module SQL.HDBC.Sqlite3 where

import DBQuery
import FO
import QueryPlan
import FO.Data
import SQL.HDBC
import ICAT
import SQL.ICAT

import Database.HDBC.Sqlite3

instance HDBCConnection Connection where
        showSQLQuery _ (vars, query) = show query
        showSQLInsert _ = show

getDB :: ICATDBConnInfo -> IO (Database DBAdapterMonad MapResultRow, Query -> String, Insert -> String)
getDB ps = do
    conn <- connectSqlite3 (db_name ps)
    let db = makeICATSQLDBAdapter conn
    case db of
        GenericDB _ _ _ trans -> return (Database db, show . translateQuery trans, show . translateInsert trans)
