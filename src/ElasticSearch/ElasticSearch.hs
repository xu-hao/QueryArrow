module ElasticSearch.ElasticSearch where

-- http://swizec.com/blog/writing-a-rest-client-in-haskell/swizec/6152

import Data.Char (toLower)

import FO.Data
import DB.DB
import Config

import qualified ElasticSearch.Query as ESQ
import ElasticSearch.ICAT

import Debug.Trace

getDB :: ICATDBConnInfo -> AbstractDatabase MapResultRow Formula
getDB ps =
    let conn = ESQ.ElasticSearchConnInfo (db_host ps) (db_port ps) (map toLower (db_name ps))
        db = makeElasticSearchDBAdapter (db_namespace ps) conn in
        AbstractDatabase db
