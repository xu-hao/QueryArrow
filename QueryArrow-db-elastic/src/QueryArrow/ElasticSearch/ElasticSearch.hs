{-# LANGUAGE DeriveGeneric, TypeSynonymInstances, FlexibleInstances, MultiParamTypeClasses, GADTs #-}
module QueryArrow.ElasticSearch.ElasticSearch where

import Data.Char (toLower)
import Data.Aeson
import GHC.Generics

import QueryArrow.DB.DB
import QueryArrow.Semantics.ResultRow.VectorResultRow
import QueryArrow.Semantics.ResultValue.AbstractResultValue
import QueryArrow.Semantics.ResultSet.VectorResultSetTransformer
import QueryArrow.Config
import QueryArrow.Plugin
import QueryArrow.Data.Heterogeneous.List
import QueryArrow.DB.AbstractDatabaseList

import qualified QueryArrow.ElasticSearch.Query as ESQ
import QueryArrow.ElasticSearch.ICAT

import Data.Maybe

instance FromJSON ICATDBConnInfo2
instance ToJSON ICATDBConnInfo2

data ICATDBConnInfo2 = ICATDBConnInfo2 {
  db_name :: String,
  db_namespace :: String,
  db_host :: String,
  db_password :: String,
  db_port :: Int,
  db_username :: String,
  db_predicates :: String,
  db_sql_mapping :: String
} deriving (Show, Generic)

data ElasticSearchPlugin = ElasticSearchPlugin

instance Plugin ElasticSearchPlugin (ResultSetTransformer AbstractResultValue) (VectorResultRow AbstractResultValue) where
  getDB _ _ ps = do
    let fsconf = getDBSpecificConfig ps
    let conn = ESQ.ElasticSearchConnInfo (db_host fsconf) (db_port fsconf) (map toLower (db_name fsconf))
    db <-  makeElasticSearchDBAdapter (db_namespace fsconf) (db_predicates fsconf) (db_sql_mapping fsconf) conn
    return (AbstractDatabase db)
