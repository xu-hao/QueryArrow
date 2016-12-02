{-# LANGUAGE TypeFamilies, TypeSynonymInstances, FlexibleInstances, MultiParamTypeClasses, FlexibleContexts, RankNTypes, GADTs #-}
module QueryArrow.Cypher.ICAT where

import QueryArrow.FO.Data
import QueryArrow.Cypher.Neo4jConnection
import QueryArrow.Cypher.Cypher
import QueryArrow.Cypher.BuiltIn
import QueryArrow.Cypher.SQLToCypher
import QueryArrow.DB.GenericDatabase
import QueryArrow.DB.NoConnection
import QueryArrow.ICAT
import QueryArrow.BuiltIn
import QueryArrow.SQL.ICAT hiding (lookupPred)
import QueryArrow.SQL.SQL

import Prelude hiding (lookup)
import Data.Map.Strict (empty, Map, insert, (!), member, singleton, adjust, foldlWithKey, lookup, fromList, keys, elems)

import Data.Namespace.Namespace

lookupPred :: String -> String -> Pred
lookupPred ns n =
  let cypherStandardBuiltInPredsMap = qStandardBuiltInPredsMap ns
  in
      case lookupObject (QPredName ns [] n) cypherStandardBuiltInPredsMap of
            Nothing -> error ("cypherBuiltIn: cannot find predicate " ++ n)
            Just pred1 -> pred1

cypherMapping :: String -> [Pred] -> [(String, (Table, [SQLQualifiedCol]))] -> CypherPredTableMap
cypherMapping ns preds mappings = (sqlToCypher  (sqlMapping ns preds mappings))

cypherTrans :: String -> [Pred] -> [(String, (Table, [SQLQualifiedCol]))] -> CypherTrans
cypherTrans ns preds mappings = CypherTrans (cypherBuiltIn (lookupPred ns))  ((cypherMapping ns preds mappings))
{- fromList [
        ("DATA_OBJ", mappingPattern0 "obj_id" "DataObject"),
        ("COLL_OBJ", mappingPattern0 "obj_id" "Collection"),
        ("META_OBJ", mappingPattern0 "obj_id" "AVU"),
        ("DATA_NAME", mappingPattern1 "obj_id" "DataObject" "data_name"),
        ("DATA_SIZE", mappingPattern1 "obj_id" "DataObject" "data_size"),
        ("COLL_NAME", mappingPattern1 "obj_id" "Collection" "coll_name"),
        ("DATA_REPL_NUM", mappingPattern1 "obj_id" "DataObject" "data_repl_num"),
        ("DATA_COLL", mappingPattern2 "obj_id" "DataObject"  "obj_id" "Collection"  "coll"),
        ("DATA_PATH", mappingPattern1 "obj_id" "DataObject" "data_path"),
        ("DATA_CHECKSUM", mappingPattern1 "obj_id" "DataObject" "data_checksum"),
        ("DATA_CREATE_TIME", mappingPattern1 "obj_id" "DataObject" "create_ts"),
        ("DATA_MODIFY_TIME", mappingPattern1 "obj_id" "DataObject" "modify_ts"),
        ("COLL_CREATE_TIME", mappingPattern1 "obj_id" "Collection" "create_ts"),
        ("COLL_MODIFY_TIME", mappingPattern1 "obj_id" "Collection" "modify_ts"),
        ("OBJT_METAMAP_OBJ", mappingPattern2m "obj_id" "obj_id" "meta"),
        ("META_ATTR_NAME", mappingPattern1 "obj_id" "AVU" "meta_attr_name"),
        ("META_ATTR_VALUE", mappingPattern1 "obj_id" "AVU" "meta_attr_value"),
        ("META_ATTR_UNIT", mappingPattern1 "obj_id" "AVU" "meta_attr_unit")
        ] -}


makeICATCypherDBAdapter :: String -> [String] -> Neo4jDatabase -> IO (NoConnectionDatabase (GenericDatabase CypherTrans Neo4jDatabase  ))
makeICATCypherDBAdapter ns [predsPath, mappingsPath] conn = do
    preds <- loadPreds predsPath
    mappings <- loadMappings mappingsPath
    let (CypherBuiltIn cypherbuiltin) = cypherBuiltIn (lookupPred ns)
    let cypherbuiltinpreds = keys cypherbuiltin
    return (NoConnectionDatabase (GenericDatabase  (cypherTrans ns preds mappings) conn ns (qStandardPreds ns preds ++ cypherbuiltinpreds)))
