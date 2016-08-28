{-# LANGUAGE TypeFamilies, FlexibleContexts #-}
module DB.NoPreparedStatement where

import DB.ResultStream
import DB.DB

-- interface

class (IDBConnection0 conn, IResultRow (NPSRowType conn)) => INPSDBConnection conn where
    type NPSRowType conn
    type NPSQueryType conn
    npsdbStmtExec :: conn -> NPSQueryType conn -> DBResultStream (NPSRowType conn) -> DBResultStream (NPSRowType conn)

-- instance for IDBConnection

newtype NPSDBConnection conn = NPSDBConnection conn deriving IDBConnection0 conn) => IDBConnection0 (NPSDBConnection conn) where
    dbClose (NPSDBConnection conn) = dbClose conn
    dbBegin (NPSDBConnection conn) = dbBegin conn
    dbCommit (NPSDBConnection conn) = dbCommit conn
    dbPrepare (NPSDBConnection conn) = dbPrepare conn
    dbRollback (NPSDBConnection conn) = dbRollback conn

instance (INPSDBConnection conn) => IDBConnection (NPSDBConnection conn) where
    type QueryType (NPSDBConnection conn) = NPSQueryType conn
    type StatementType (NPSDBConnection conn) = NPSDBStatement conn (NPSQueryType conn)
    prepareQuery (NPSDBConnection conn) query = return (NPSDBStatement conn query)

-- instance for IDBStatement

data NPSDBStatement conn query = NPSDBStatement conn query

instance (INPSDBConnection conn, query ~ NPSQueryType conn) => IDBStatement (NPSDBStatement conn query) where
    type RowType (NPSDBStatement conn query) = NPSRowType conn
    dbStmtExec (NPSDBStatement conn query) = npsdbStmtExec conn query
    dbStmtClose _ = return ()
