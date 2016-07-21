{-# LANGUAGE GADTs, TypeFamilies, TypeSynonymInstances, MultiParamTypeClasses, FlexibleInstances #-}
module SQL.HDBC.Sqlite3 where

import DBQuery
import Config
import QueryPlan
import FO.Data
import SQL.HDBC
import Database.HDBC
import ICAT
import SQL.ICAT
import Control.Monad.IO.Class

import Database.HDBC.Sqlite3

instance HDBCConnection Connection where
        showSQLQuery _ (vars, query, params) = serialize query
        hdbcCommit conn = do
            liftIO $ commit conn
            return True
        hdbcPrepare conn = return True
        hdbcRollback conn = liftIO $ rollback conn

getDB :: ICATDBConnInfo -> IO [Database DBAdapterMonad MapResultRow]
getDB ps = do
    conn <- connectSqlite3 (db_name ps)
    let db = makeICATSQLDBAdapter (db_name ps) conn
    return [Database db]
