{-# LANGUAGE TypeFamilies, TypeSynonymInstances, FlexibleInstances, MultiParamTypeClasses, FlexibleContexts, RankNTypes, GADTs #-}
module QueryArrow.Cypher.Mapping where

import QueryArrow.Syntax.Term
import QueryArrow.Cypher.Neo4jConnection
import QueryArrow.Cypher.Cypher
import QueryArrow.Cypher.BuiltIn
import QueryArrow.Cypher.SQLToCypher
import QueryArrow.DB.GenericDatabase
import QueryArrow.DB.NoConnection
import QueryArrow.Mapping
import QueryArrow.BuiltIn
import QueryArrow.SQL.Mapping hiding (lookupPred, lookupPredByName)
import QueryArrow.SQL.SQL
import QueryArrow.Serialize

import Prelude hiding (lookup)
import Data.Map.Strict (empty, Map, insert, (!), member, singleton, adjust, foldlWithKey, lookup, fromList, keys, elems)

import Data.Namespace.Namespace

lookupPred :: String -> String -> PredName
lookupPred ns n =
  predName (lookupPredByName (PredName [ns] n))

lookupPredByName :: PredName -> Pred
lookupPredByName n@(PredName [ns] _) =
  let cypherStandardBuiltInPredsMap = qStandardBuiltInPredsMap ns
  in
      case lookupObject n cypherStandardBuiltInPredsMap of
            Nothing -> error ("cypherBuiltIn: cannot find predicate " ++ show n)
            Just pred1 -> pred1
cypherMapping :: String -> [Pred] -> [SQLMapping] -> CypherPredTableMap
cypherMapping ns preds mappings = (sqlToCypher (constructPredTypeMap (map (setPredNamespace ns) preds)) (sqlMapping ns preds mappings))

cypherTrans :: String -> [Pred] -> [SQLMapping] -> CypherTrans
cypherTrans ns preds mappings =
  let builtins@(CypherBuiltIn cypherbuiltinmap) = (cypherBuiltIn (lookupPred ns))
      total = map (setPredNamespace ns) preds ++ map lookupPredByName (keys cypherbuiltinmap) in
      CypherTrans builtins ((cypherMapping ns preds mappings)) (constructPredTypeMap total)

makeICATCypherDBAdapter :: String -> String -> String -> Neo4jDatabase -> IO (NoConnectionDatabase (GenericDatabase CypherTrans Neo4jDatabase  ))
makeICATCypherDBAdapter ns predsPath mappingsPath conn = do
    preds <- loadPreds predsPath
    mappings <- loadMappings mappingsPath
    let (CypherBuiltIn cypherbuiltin) = cypherBuiltIn (lookupPred ns)
    let cypherbuiltinpreds = keys cypherbuiltin
    return (NoConnectionDatabase (GenericDatabase  (cypherTrans ns preds mappings) conn ns (qStandardPreds ns preds ++ map lookupPredByName cypherbuiltinpreds)))
