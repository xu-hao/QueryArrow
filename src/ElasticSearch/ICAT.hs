{-# LANGUAGE MultiParamTypeClasses, FlexibleInstances, FlexibleContexts #-}

module ElasticSearch.ICAT where

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
import DB.DB
import DB.ResultStream
import DB.NoConnection
import Config

import ElasticSearch.Record
import qualified ElasticSearch.Query as ESQ
import ElasticSearch.QueryResult
import ElasticSearch.QueryResultHits
import ElasticSearch.ESQL
import ElasticSearch.ElasticSearchConnection

esMetaPred :: String -> Pred
esMetaPred ns = Pred (QPredName ns [] "ES_META") (PredType ObjectPred [Key "Int", Key "Int", Property "String", Property "String", Property "String"])

makeElasticSearchDBAdapter :: String -> ESQ.ElasticSearchConnInfo -> NoConnectionDatabase (GenericDatabase ESTrans ElasticSearchDB)
makeElasticSearchDBAdapter ns conn = NoConnectionDatabase (GenericDatabase (ESTrans  (fromList [(esMetaPred ns, (pack "ES_META", [pack "obj_id", pack "meta_id", pack "attribute", pack "value", pack "unit"]))])) conn ns [esMetaPred ns])

getDB :: ICATDBConnInfo -> IO [AbstractDatabase MapResultRow]
getDB ps = do
    let conn = ESQ.ElasticSearchConnInfo (db_host ps) (db_port ps) (map toLower (db_name ps))
    let db = makeElasticSearchDBAdapter (db_namespace ps) conn
    return [AbstractDatabase db]
