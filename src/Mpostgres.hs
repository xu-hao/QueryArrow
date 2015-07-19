module Mpostgres where

import FO
import SQL.SQL
import SQL.HDBC
import SQL.HDBC.PostgreSQL
import DBQuery
import ICAT

import Database.HDBC

getDB :: ICATDBConnInfo -> IO (Database DBAdapterMonad MapResultRow, Query -> String)
getDB ps = do
    conn <- dbConnect (PostgreSQLDBConnInfo (DBConnInfo (db_host ps) (db_port ps) (db_username ps) (db_password ps) (db_name ps)))
    let db = makeICATSQLDBAdapter conn
    return (Database db, show . translate db)
                        
