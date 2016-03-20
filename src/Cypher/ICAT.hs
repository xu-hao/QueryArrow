{-# LANGUAGE TypeFamilies, TypeSynonymInstances, FlexibleInstances, MultiParamTypeClasses, FlexibleContexts, RankNTypes, GADTs #-}
module Cypher.ICAT where

import FO.Data
import Cypher.Neo4jConnection
import Cypher.Cypher
import DBQuery
import ICAT

import qualified Data.Text as T
import Prelude hiding (lookup)
import Control.Applicative ((<$>),(<*>))
import Data.Map.Strict (empty, Map, insert, (!), member, singleton, adjust, foldlWithKey, lookup, fromList)

cypherExprFromArg2 (IntExpr i) = CypherIntConstExpr i
cypherExprFromArg2 (StringExpr s) = CypherStringConstExpr s
cypherExprFromArg2 (VarExpr (Var a)) = CypherParamExpr a

cypherBuiltIn :: CypherBuiltIn
cypherBuiltIn = CypherBuiltIn ( fromList [
        ("le", \thesign args ->
            return (cwhere (CypherCompCond (case thesign of
                Pos -> "<="
                Neg -> ">") (cypherExprFromArg2 (head args)) (cypherExprFromArg2 (args !! 1))))),
        ("lt", \thesign args ->
            return (cwhere (CypherCompCond (case thesign of
                Pos -> "<"
                Neg -> ">=") (cypherExprFromArg2 (head args)) (cypherExprFromArg2 (args !! 1))))),
        ("eq", \thesign args ->
            return (cwhere (CypherCompCond (case thesign of
                Pos -> "="
                Neg -> "<>") (cypherExprFromArg2 (head args)) (cypherExprFromArg2 (args !! 1))))),
        ("like", \thesign args -> do
            let pos = CypherCompCond "=~" (cypherExprFromArg2 (head args)) (cypherExprFromArg2 (args !! 1))
            return (cwhere (case thesign of
                Pos -> pos
                Neg -> CypherNotCond pos))),
        ("like_regex", \thesign args -> do
            let pos = CypherCompCond "=~" (cypherExprFromArg2 (head args)) (cypherExprFromArg2 (args !! 1))
            return (cwhere (case thesign of
                Pos -> pos
                Neg -> CypherNotCond pos)))
    ])

cypherTrans :: CypherTrans
cypherTrans = CypherTrans cypherBuiltIn  ["DATA_OBJ", "COLL_OBJ", "META_OBJ", "OBJT_METAMAP_OBJ"] (fromList [
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
        ])


makeICATCypherDBAdapter :: Neo4jConnection -> GenericDB   Neo4jConnection CypherTrans
makeICATCypherDBAdapter conn = GenericDB conn "ICAT" standardPreds cypherTrans


mappingPattern0 :: String -> String -> CypherMapping
mappingPattern0 id nodetype =
        (
                [CypherVar "1"],
                GraphPattern [] [],
                GraphPattern [] [nodevlp "0" nodetype [(PropertyKey id, var "1")]],
                [(CypherVar "0", [CypherVar "1"])]
        )
mappingPattern1 :: String -> String -> String -> CypherMapping
mappingPattern1 id nodetype prop =
        (
                [CypherVar"1", CypherVar"2"],
                GraphPattern [nodevlp "0" nodetype [(PropertyKey id, var "1")]] [],
                GraphPattern [] [nodevlp "0" nodetype [(PropertyKey prop, var "2")]],
                [(CypherVar"0", [CypherVar"1"])]
        )
mappingPattern2 :: String -> String -> String -> String -> String -> CypherMapping
mappingPattern2 id1 nodetype1 id2 nodetype2 edge =
            (
                [CypherVar"1", CypherVar"2"],
                GraphPattern [nodevlp "d" nodetype1 [(PropertyKey id1, var "1")], nodevlp "c" nodetype2 [(PropertyKey id2, var "2")]] [],
                GraphPattern [] [edgevl (nodev "d") "e" edge (nodev "c")],
                [(CypherVar"d", [CypherVar"1"]), (CypherVar"e", [CypherVar"1", CypherVar"2"]), (CypherVar"c", [CypherVar"2"])]
            )

mappingPattern2m::String -> String -> String -> CypherMapping
mappingPattern2m id1 id2 edge =
            (
                [CypherVar"1", CypherVar"2"],
                GraphPattern [nodevp "d" [(PropertyKey id1, var "1")], nodevp "c" [(PropertyKey id2, var "2")]] [],
                GraphPattern [] [edgevl (nodev "d") "e" edge (nodev "c")],
                [(CypherVar"d", [CypherVar"1"]), (CypherVar"e", [CypherVar"1", CypherVar"2"]), (CypherVar"c", [CypherVar"2"])]
            )
