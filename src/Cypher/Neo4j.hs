{-# LANGUAGE TypeFamilies, TypeSynonymInstances, FlexibleInstances, MultiParamTypeClasses, FlexibleContexts, RankNTypes, GADTs #-}
module Cypher.Neo4j where

import FO
import DBQuery
import ICAT
import Cypher.ICAT

-- db
getDB :: ICATDBConnInfo -> IO (Database DBAdapterMonad MapResultRow, Query -> String, Insert -> String)
getDB ps = do
    conn <- return (db_host ps, db_port ps)
    let db = makeICATCypherDBAdapter conn
    case db of
      GenericDB _ _ _ trans -> return (Database db, show . translateQuery trans, show . translateInsert trans)
