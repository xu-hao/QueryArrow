module QueryArrow.ElasticSearch.ICAT where

-- http://swizec.com/blog/writing-a-rest-client-in-haskell/swizec/6152

import Prelude hiding (lookup)
import Data.Map.Strict hiding (map, elemAt)
import Data.Text (pack)

import QueryArrow.FO.Data
import QueryArrow.DB.GenericDatabase
import QueryArrow.DB.NoConnection

import qualified QueryArrow.ElasticSearch.Query as ESQ
import QueryArrow.ElasticSearch.ESQL
import QueryArrow.ElasticSearch.ElasticSearchConnection

esMetaPred :: String -> Pred
esMetaPred ns = Pred (QPredName ns [] "ES_META") (PredType ObjectPred [Key "Int", Key "Int", Property "String", Property "String", Property "String"])

makeElasticSearchDBAdapter :: String -> [String] -> ESQ.ElasticSearchConnInfo -> IO (NoConnectionDatabase (GenericDatabase ESTrans ElasticSearchDB))
makeElasticSearchDBAdapter ns _ conn = return (NoConnectionDatabase (GenericDatabase (ESTrans  (fromList [(esMetaPred ns, (pack "ES_META", [pack "obj_id", pack "meta_id", pack "attribute", pack "value", pack "unit"]))])) conn ns [esMetaPred ns]))
