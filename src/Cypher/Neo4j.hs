{-# LANGUAGE TypeFamilies, TypeSynonymInstances, FlexibleInstances, MultiParamTypeClasses, FlexibleContexts, RankNTypes, GADTs #-}
module Cypher.Neo4j where

import Config
import QueryPlan
import DBQuery
import ICAT
import Cypher.ICAT

-- db
getDB :: ICATDBConnInfo -> IO (Database DBAdapterMonad MapResultRow)
getDB ps = do
    conn <- return (db_host ps, db_port ps)
    let db = makeICATCypherDBAdapter conn
    case db of
      GenericDB _ _ _ trans -> return (Database db)
