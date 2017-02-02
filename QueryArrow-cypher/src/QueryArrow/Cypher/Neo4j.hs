{-# LANGUAGE TypeFamilies, TypeSynonymInstances, FlexibleInstances, MultiParamTypeClasses, FlexibleContexts, RankNTypes, GADTs, DeriveGeneric #-}
module QueryArrow.Cypher.Neo4j where

import QueryArrow.Config
import QueryArrow.DB.DB
import QueryArrow.Cypher.ICAT
import QueryArrow.Plugin
import QueryArrow.DB.AbstractDatabaseList
import QueryArrow.Data.Heterogeneous.List

import Data.Aeson
import GHC.Generics
import Data.Maybe

instance FromJSON ICATDBConnInfo2
instance ToJSON ICATDBConnInfo2

data ICATDBConnInfo2 = ICATDBConnInfo2 {
  -- db_name :: String,
  db_namespace :: String,
  db_host :: String,
  db_password :: String,
  db_port :: Int,
  db_username :: String,
  db_predicates :: String,
  db_sql_mapping :: String
} deriving (Show, Generic)


-- db
data Neo4jPlugin = Neo4jPlugin

instance Plugin Neo4jPlugin MapResultRow where
  getDB _ ps  (AbstractDBList HNil) = do
      let fsconf0 = fromJSON (fromJust (db_config ps))
      case fsconf0 of
        Error err -> error err
        Success fsconf -> do
            let conn = (db_host fsconf, db_port fsconf, db_username fsconf, db_password fsconf)
            db <- makeICATCypherDBAdapter (db_namespace fsconf) (db_predicates fsconf) (db_sql_mapping fsconf) conn
            return (    AbstractDatabase db)
  getDB _ _ _ = error "FileSystemPlugin: config error"
