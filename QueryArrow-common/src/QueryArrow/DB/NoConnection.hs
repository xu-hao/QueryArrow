{-# LANGUAGE TypeFamilies, FlexibleContexts, FlexibleInstances #-}
module QueryArrow.DB.NoConnection where

import QueryArrow.DB.DB

-- interface

class INoConnectionDatabase2 db where
    type NoConnectionQueryType db
    type NoConnectionRowType db
    noConnectionDBStmtExec :: db -> NoConnectionQueryType db -> DBResultStream (NoConnectionRowType db) -> DBResultStream (NoConnectionRowType db)

-- instance for IDatabase

newtype NoConnectionDatabase db = NoConnectionDatabase db

instance (IDatabase0 db) => (IDatabase0 (NoConnectionDatabase db)) where
    type DBFormulaType (NoConnectionDatabase db) = DBFormulaType db
    getName (NoConnectionDatabase db) = getName db
    getPreds (NoConnectionDatabase db) = getPreds db
    supported (NoConnectionDatabase db) = supported db
    checkQuery (NoConnectionDatabase db) = checkQuery db

instance (IDatabase1 db) => (IDatabase1 (NoConnectionDatabase db)) where
    type DBQueryType (NoConnectionDatabase db) = DBQueryType db
    translateQuery (NoConnectionDatabase db) = translateQuery db

instance (INoConnectionDatabase2 db) => IDatabase2 (NoConnectionDatabase db) where
    data ConnectionType (NoConnectionDatabase db) = NoConnectionDBConnection db
    dbOpen (NoConnectionDatabase db) = return (NoConnectionDBConnection db)

instance (IDatabase0 db, IDatabase1 db, INoConnectionDatabase2 db, DBQueryType db ~ NoConnectionQueryType db) => IDatabase (NoConnectionDatabase db) where

-- instance for IDBConnection

instance IDBConnection0 (ConnectionType (NoConnectionDatabase db)) where
    dbClose _ = return ()
    dbBegin _ = return ()
    dbCommit _ = return ()
    dbPrepare _ = return ()
    dbRollback _ = return ()

instance (INoConnectionDatabase2 db) => IDBConnection (ConnectionType (NoConnectionDatabase db)) where
    type QueryType (ConnectionType (NoConnectionDatabase db)) = NoConnectionQueryType db
    type StatementType (ConnectionType (NoConnectionDatabase db)) = NoConnectionDBStatement db
    prepareQuery (NoConnectionDBConnection db) qu = return (NoConnectionDBStatement db qu)

-- instance for IDBStatement

data NoConnectionDBStatement db = NoConnectionDBStatement db (NoConnectionQueryType db)

instance (INoConnectionDatabase2 db) => IDBStatement (NoConnectionDBStatement db) where
    type RowType (NoConnectionDBStatement db) = NoConnectionRowType db
    dbStmtClose _ = return ()
    dbStmtExec (NoConnectionDBStatement db  qu ) = noConnectionDBStmtExec db  qu
