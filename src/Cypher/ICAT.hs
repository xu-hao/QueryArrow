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

import Data.Namespace.Namespace

cypherBuiltIn :: String -> CypherBuiltIn
cypherBuiltIn ns =
    let cypherStandardBuiltInPredsMap = qStandardBuiltInPredsMap ns
        lookupPred n = case lookupObject (QPredName ns [] n) cypherStandardBuiltInPredsMap of
                Nothing -> error ("cypherBuiltIn: cannot find predicate " ++ n)
                Just pred1 -> pred1 in
        CypherBuiltIn ( fromList [
            (lookupPred "le", \thesign args ->
                return (cwhere (CypherCompCond (case thesign of
                    Pos -> "<="
                    Neg -> ">") ( (head args)) ( (args !! 1)) Pos))),
            (lookupPred "lt", \thesign args ->
                return (cwhere (CypherCompCond (case thesign of
                    Pos -> "<"
                    Neg -> ">=") ( (head args)) ( (args !! 1)) Pos))),
            (lookupPred "eq", \thesign args ->
                return (cwhere (CypherCompCond (case thesign of
                    Pos -> "="
                    Neg -> "<>") ( (head args)) ( (args !! 1)) Pos))),
            (lookupPred "like", \thesign args ->
                error "cypherBuiltIn: unsupported like operator, use like_regex"),
            (lookupPred "like_regex", \thesign args ->
                return (cwhere (CypherCompCond "=~" ( (head args)) ( (args !! 1)) thesign)))
        ])

cypherMapping :: String -> CypherPredTableMap
cypherMapping ns = (sqlToCypher (fromList [
    ("r_data_main", "DataObject"),
    ("r_coll_main", "Collection")
    ]) (fromList [
    ("data_id", "object_id"),
    ("coll_id", "object_id")
    ]) (sqlMapping ns))

cypherTrans :: String -> CypherTrans
cypherTrans ns = CypherTrans (cypherBuiltIn ns)  ["DATA_OBJ", "COLL_OBJ", "META_OBJ", "OBJT_METAMAP_OBJ"] ((cypherMapping ns))
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


makeICATCypherDBAdapter :: String -> Neo4jConnection -> GenericDB   Neo4jConnection CypherTrans
makeICATCypherDBAdapter ns conn = GenericDB conn ns (qStandardPreds ns ++ qStandardBuiltInPreds ns) (cypherTrans ns)
