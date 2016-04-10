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
        showSQLQuery _ (vars, query, params) = show query
        hdbcCommit conn = do
            liftIO $ commit conn
            return True
        hdbcPrepare conn = return True
        hdbcRollback conn = liftIO $ rollback conn

getDB :: ICATDBConnInfo -> IO [Database DBAdapterMonad MapResultRow]
getDB ps = do
    conn <- connectPostgreSQL ("host="++db_host ps++ " port="++show (db_port ps)++" dbname="++db_name ps++" user="++db_username ps++" password="++db_password ps)
    let db = makeICATSQLDBAdapter conn
    let db2 = SequenceDB conn "NextID" "nextid" NextidTrans
    return [Database db, Database db2]

data NextidTrans = NextidTrans

instance TranslateSequence NextidTrans SQLQuery where
    translateSequenceQuery trans =
        let v = Var "nextid" in
            (([v], SQLQ (SQL0 {sqlSelect = [SQLFuncExpr "nextval" [SQLStringConstExpr "R_ObjectId"]], sqlFrom = [], sqlWhere = SQLTrueCond}), []), v)
