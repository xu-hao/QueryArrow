{-# LANGUAGE TypeFamilies, MultiParamTypeClasses, FlexibleInstances, FlexibleContexts #-}
module DB.NoConnection where

import DB.DB

-- interface

class INoConnectionDatabase1 db where
    type NoConnectionQueryType db
    type NoConnectionRowType db
    noConnectionDBStmtExec :: db -> NoConnectionQueryType db -> DBResultStream (NoConnectionRowType db) -> DBResultStream (NoConnectionRowType db)

-- instance for IDatabase

newtype NoConnectionDatabase db = NoConnectionDatabase db

instance (IDatabase0 db) => (IDatabase0 (NoConnectionDatabase db)) where
    type DBQueryType (NoConnectionDatabase db) = DBQueryType db
    getName (NoConnectionDatabase db) = getName db
    getPreds (NoConnectionDatabase db) = getPreds db
    determinateVars (NoConnectionDatabase db) = determinateVars db
    supported (NoConnectionDatabase db) = supported db
    translateQuery (NoConnectionDatabase db) = translateQuery db

instance (INoConnectionDatabase1 db) => IDatabase1 (NoConnectionDatabase db) where
    type ConnectionType (NoConnectionDatabase db) = NoConnectionDBConnection db
    dbOpen (NoConnectionDatabase db) = return (NoConnectionDBConnection db)

instance (IDatabase0 db, INoConnectionDatabase1 db, DBQueryType db ~ NoConnectionQueryType db) => IDatabase (NoConnectionDatabase db) where

-- instance for IDBConnection
newtype NoConnectionDBConnection db = NoConnectionDBConnection db

instance IDBConnection0 (NoConnectionDBConnection db) where
    dbClose _ = return ()
    dbBegin _ = return ()
    dbCommit _ = return True
    dbPrepare _ = return True
    dbRollback _ = return ()

instance (INoConnectionDatabase1 db) => IDBConnection (NoConnectionDBConnection db) where
    type QueryType (NoConnectionDBConnection db) = NoConnectionQueryType db
    type StatementType (NoConnectionDBConnection db) = NoConnectionDBStatement db
    prepareQuery (NoConnectionDBConnection db) qu = return (NoConnectionDBStatement db qu)

-- instance for IDBStatement

data NoConnectionDBStatement db = NoConnectionDBStatement db (NoConnectionQueryType db)

instance (INoConnectionDatabase1 db) => IDBStatement (NoConnectionDBStatement db) where
    type RowType (NoConnectionDBStatement db) = NoConnectionRowType db
    dbStmtClose _ = return ()
    dbStmtExec (NoConnectionDBStatement db  qu ) = noConnectionDBStmtExec db  qu
