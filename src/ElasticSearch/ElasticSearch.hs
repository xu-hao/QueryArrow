{-# LANGUAGE MultiParamTypeClasses, FlexibleInstances, FlexibleContexts #-}

module ElasticSearch.ElasticSearch where

-- http://swizec.com/blog/writing-a-rest-client-in-haskell/swizec/6152

import Prelude hiding (lookup)
import qualified Data.ByteString.Lazy.Char8 as BL8
import Data.Map.Strict hiding (map, elemAt)
import Data.Text (unpack, pack, Text)
import Data.Char (toLower)
import Data.Convertible
import Data.Scientific (toBoundedInteger)
import Data.Aeson (Value (String, Number))

import FO.Domain
import FO.Data
import DB.GenericDatabase
import QueryPlan
import DB.ResultStream
import Config

import ElasticSearch.Record
import qualified ElasticSearch.Query as ESQ
import ElasticSearch.QueryResult
import ElasticSearch.QueryResultHits
import ElasticSearch.ESQL
import ElasticSearch.ICAT

import Debug.Trace

getDB :: ICATDBConnInfo -> IO [AbstractDatabase MapResultRow]
getDB ps = do
    let db = makeElasticSearchDBAdapter (db_namespace ps) conn
    return [AbstractDatabase db]
