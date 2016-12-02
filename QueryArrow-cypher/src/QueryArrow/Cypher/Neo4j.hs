{-# LANGUAGE TypeFamilies, TypeSynonymInstances, FlexibleInstances, MultiParamTypeClasses, FlexibleContexts, RankNTypes, GADTs #-}
module QueryArrow.Cypher.Neo4j where

import QueryArrow.FO.Data
import QueryArrow.Config
import QueryArrow.DB.DB
import QueryArrow.Cypher.ICAT

-- db
getDB :: ICATDBConnInfo -> IO (AbstractDatabase MapResultRow Formula)
getDB ps = do
    let conn = (db_host ps, db_port ps, db_username ps, db_password ps)
    db <- makeICATCypherDBAdapter (db_namespace ps) (db_icat ps) conn
    return (    AbstractDatabase db)
