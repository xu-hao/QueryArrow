{-# LANGUAGE TypeFamilies, TypeSynonymInstances, FlexibleInstances, MultiParamTypeClasses, FlexibleContexts, RankNTypes, GADTs #-}
module Cypher.ICAT where

import FO.Data
import Cypher.Neo4jConnection
import Cypher.Cypher
import Cypher.SQLToCypher
import Utils
import DBQuery
import ICAT
import SQL.ICAT

import qualified Data.Text as T
import Prelude hiding (lookup)
import Control.Applicative ((<$>),(<*>))
import Data.Map.Strict (empty, Map, insert, (!), member, singleton, adjust, foldlWithKey, lookup, fromList, keys, elems)


cypherBuiltIn :: CypherBuiltIn
cypherBuiltIn = CypherBuiltIn ( fromList [
        ("le", \thesign args ->
            return (cwhere (CypherCompCond (case thesign of
                Pos -> "<="
                Neg -> ">") ( (head args)) ( (args !! 1)) Pos))),
        ("lt", \thesign args ->
            return (cwhere (CypherCompCond (case thesign of
                Pos -> "<"
                Neg -> ">=") ( (head args)) ( (args !! 1)) Pos))),
        ("eq", \thesign args ->
            return (cwhere (CypherCompCond (case thesign of
                Pos -> "="
                Neg -> "<>") ( (head args)) ( (args !! 1)) Pos))),
        ("like", \thesign args ->
            error "cypherBuiltIn: unsupported like operator, use like_regex"),
        ("like_regex", \thesign args ->
            return (cwhere (CypherCompCond "=~" ( (head args)) ( (args !! 1)) thesign)))
    ])

cypherMapping :: (PredMap, CypherPredTableMap)
cypherMapping = (sqlToCypher (fromList [
    ("r_data_main", "DataObject"),
    ("r_coll_main", "Collection")
    ]) (fromList [
    ("data_id", "object_id"),
    ("coll_id", "object_id")
    ]) sqlMapping)

cypherTrans :: CypherTrans
cypherTrans = CypherTrans cypherBuiltIn  ["DATA_OBJ", "COLL_OBJ", "META_OBJ", "OBJT_METAMAP_OBJ"] (snd cypherMapping)
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


makeICATCypherDBAdapter :: Neo4jConnection -> GenericDB   Neo4jConnection CypherTrans
makeICATCypherDBAdapter conn = GenericDB conn "ICAT" (elems (fst cypherMapping) ++ standardBuiltInPreds) cypherTrans
