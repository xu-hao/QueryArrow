{-# LANGUAGE GADTs, FlexibleInstances, MultiParamTypeClasses, OverloadedStrings #-}
module SQL.HDBC.PostgreSQL where

import DBQuery
import QueryPlan
import FO.Data
import SQL.HDBC
import ICAT
import SQL.ICAT
import Config
import SQL.SQL
import Database.HDBC
import Control.Monad.IO.Class

import Database.HDBC.PostgreSQL

instance HDBCConnection Connection where
        hdbcCommit conn = do
            liftIO $ commit conn
            return True
        hdbcPrepare conn = return True
        hdbcRollback conn = liftIO $ rollback conn

getDB :: ICATDBConnInfo -> IO [Database DBAdapterMonad MapResultRow]
getDB ps = do
    conn <- connectPostgreSQL ("host="++db_host ps++ " port="++show (db_port ps)++" dbname="++db_path ps !! 0++" user="++db_username ps++" password="++db_password ps)
    let db = makeICATSQLDBAdapter (db_name ps !! 0) (Just "nextid") (HDBCConnectionDBConnection conn)
    return [Database db]
