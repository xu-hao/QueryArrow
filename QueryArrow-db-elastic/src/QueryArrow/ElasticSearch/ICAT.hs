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
esMetaPred ns = Pred (esMetaPredName ns) (PredType ObjectPred [ParamType True True True NumberType, ParamType True True True NumberType, ParamType False True True TextType, ParamType False True True TextType, ParamType False True True TextType])

esMetaPredName :: String -> PredName
esMetaPredName ns = QPredName ns [] "ES_META"

makeElasticSearchDBAdapter :: String -> String -> String -> ESQ.ElasticSearchConnInfo -> IO (NoConnectionDatabase (GenericDatabase ESTrans ElasticSearchDB))
makeElasticSearchDBAdapter ns _ _ conn = return (NoConnectionDatabase (GenericDatabase (ESTrans (constructPredTypeMap [esMetaPred ns]) (fromList [(esMetaPredName ns, (pack "ES_META", [pack "obj_id", pack "meta_id", pack "attribute", pack "value", pack "unit"]))])) conn ns [esMetaPred ns]))
