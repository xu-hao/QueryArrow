{-# LANGUAGE TypeFamilies, TypeSynonymInstances, FlexibleInstances, MultiParamTypeClasses, FlexibleContexts, RankNTypes, GADTs #-}
module Cypher.Neo4j where

import Config
import QueryPlan
import DBQuery
import ICAT
import Cypher.ICAT

-- db
getDB :: ICATDBConnInfo -> IO [Database DBAdapterMonad MapResultRow]
getDB ps = do
    conn <- return (db_host ps, db_port ps, db_username ps, db_password ps)
    let db = makeICATCypherDBAdapter (db_name ps !! 0) conn
    case db of
      GenericDB _ _ _ trans -> return [Database db]
