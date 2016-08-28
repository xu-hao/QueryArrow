{-# LANGUAGE MultiParamTypeClasses, FlexibleInstances, FlexibleContexts #-}

module ElasticSearch.ICAT where

-- http://swizec.com/blog/writing-a-rest-client-in-haskell/swizec/6152

import Prelude hiding (lookup)
import Data.Map.Strict hiding (map, elemAt)
import Data.Text (pack)

import FO.Data
import DB.GenericDatabase
import DB.NoConnection

import qualified ElasticSearch.Query as ESQ
import ElasticSearch.ESQL
import ElasticSearch.ElasticSearchConnection

esMetaPred :: String -> Pred
esMetaPred ns = Pred (QPredName ns [] "ES_META") (PredType ObjectPred [Key "Int", Key "Int", Property "String", Property "String", Property "String"])

makeElasticSearchDBAdapter :: String -> ESQ.ElasticSearchConnInfo -> NoConnectionDatabase (GenericDatabase ESTrans ElasticSearchDB)
makeElasticSearchDBAdapter ns conn = NoConnectionDatabase (GenericDatabase (ESTrans  (fromList [(esMetaPred ns, (pack "ES_META", [pack "obj_id", pack "meta_id", pack "attribute", pack "value", pack "unit"]))])) conn ns [esMetaPred ns])
