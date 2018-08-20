module QueryArrow.ElasticSearch.ICAT where

-- http://swizec.com/blog/writing-a-rest-client-in-haskell/swizec/6152

import Prelude hiding (lookup)
import Data.Map.Strict hiding (map, elemAt)
import Data.Text (pack)

import QueryArrow.Syntax.Term
import QueryArrow.Syntax.Type
import QueryArrow.DB.GenericDatabase
import QueryArrow.DB.NoConnection

import qualified QueryArrow.ElasticSearch.Query as ESQ
import QueryArrow.ElasticSearch.ESQL
import QueryArrow.ElasticSearch.ElasticSearchConnection

esMetaPred :: String -> Pred
esMetaPred ns = Pred (esMetaPredName ns) (PredType ObjectPred [ParamType True True True False Int64Type, ParamType True True True False Int64Type, ParamType False True True False TextType, ParamType False True True False TextType, ParamType False True True False TextType])

esMetaPredName :: String -> PredName
esMetaPredName ns = QPredName ns [] "ES_META"

makeElasticSearchDBAdapter :: String -> String -> String -> ESQ.ElasticSearchConnInfo -> IO (NoConnectionDatabase (GenericDatabase ESTrans ElasticSearchDB))
makeElasticSearchDBAdapter ns _ _ conn = return (NoConnectionDatabase (GenericDatabase (ESTrans (constructPredTypeMap [esMetaPred ns]) (fromList [(esMetaPredName ns, (pack "ES_META", [pack "obj_id", pack "meta_id", pack "attribute", pack "value", pack "unit"]))])) conn ns [esMetaPred ns]))
