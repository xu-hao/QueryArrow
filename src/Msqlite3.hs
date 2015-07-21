module Msqlite3 where

import FO
import SQL.HDBC
import SQL.HDBC.Sqlite3
import DBQuery
import ICAT

import Database.HDBC

getDB :: ICATDBConnInfo -> IO (Database DBAdapterMonad MapResultRow, Query -> String)
getDB ps = do
    conn <- dbConnect (Sqlite3DBConnInfo (db_name ps))
    let db = makeICATSQLDBAdapter conn
    return (Database db, show . translate db)
                        
