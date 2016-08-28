{-# LANGUAGE TypeFamilies, TypeSynonymInstances, FlexibleInstances, MultiParamTypeClasses, FlexibleContexts, RankNTypes, GADTs #-}
module Cypher.Neo4j where

import Config
import DB.DB
import Cypher.ICAT

-- db
getDB :: ICATDBConnInfo -> IO [AbstractDatabase MapResultRow]
getDB ps = do
    let conn = (db_host ps, db_port ps, db_username ps, db_password ps)
    let db = makeICATCypherDBAdapter (db_name ps) conn
    return [AbstractDatabase db]
