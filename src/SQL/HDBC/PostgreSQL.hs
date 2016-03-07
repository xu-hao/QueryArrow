{-# LANGUAGE GADTs, FlexibleInstances, MultiParamTypeClasses #-}
module SQL.HDBC.PostgreSQL where

import DBQuery
import FO
import QueryPlan
import FO.Data
import SQL.HDBC
import ICAT
import SQL.ICAT

import Database.HDBC.PostgreSQL

instance HDBCConnection Connection where
        showSQLQuery _ (vars, query) = show query
        showSQLInsert _ = show

getDB :: ICATDBConnInfo -> IO (Database DBAdapterMonad MapResultRow, Query -> String, Insert -> String)
getDB ps = do
    conn <- connectPostgreSQL ("host="++db_host ps++ " port="++show (db_port ps)++" dbname="++db_name ps++" user="++db_username ps++" password="++db_password ps)
    let db = makeICATSQLDBAdapter conn
    case db of
        GenericDB _ _ _ trans -> return (Database db, show . translateQuery trans, show . translateInsert trans)
