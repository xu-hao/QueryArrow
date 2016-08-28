{-# LANGUAGE MultiParamTypeClasses, FlexibleInstances, FlexibleContexts #-}

module ElasticSearch.ElasticSearch where

-- http://swizec.com/blog/writing-a-rest-client-in-haskell/swizec/6152

import Data.Char (toLower)

import DB.DB
import Config

import qualified ElasticSearch.Query as ESQ
import ElasticSearch.ICAT

import Debug.Trace

getDB :: ICATDBConnInfo -> IO [AbstractDatabase MapResultRow]
getDB ps = do
    let conn = ESQ.ElasticSearchConnInfo (db_host ps) (db_port ps) (map toLower (db_name ps))
    let db = makeElasticSearchDBAdapter (db_namespace ps) conn
    return [AbstractDatabase db]
