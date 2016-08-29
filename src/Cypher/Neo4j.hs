{-# LANGUAGE TypeFamilies, TypeSynonymInstances, FlexibleInstances, MultiParamTypeClasses, FlexibleContexts, RankNTypes, GADTs #-}
module Cypher.Neo4j where

import FO.Data
import Config
import DB.DB
import Cypher.ICAT

-- db
getDB :: ICATDBConnInfo -> AbstractDatabase MapResultRow Formula
getDB ps =
    let conn = (db_host ps, db_port ps, db_username ps, db_password ps)
        db = makeICATCypherDBAdapter (db_name ps) conn in
        AbstractDatabase db
