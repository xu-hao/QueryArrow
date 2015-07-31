{-# LANGUAGE TypeFamilies, TypeSynonymInstances, FlexibleInstances, MultiParamTypeClasses, FlexibleContexts, RankNTypes, GADTs #-}
module Cypher.ICAT where

import FO
import Cypher.Neo4jConnection
import Cypher.Cypher
import DBQuery
import ICAT

import Prelude hiding (lookup)
import Control.Applicative ((<$>),(<*>))
import Data.Map.Strict (empty, Map, insert, (!), member, singleton, adjust, foldlWithKey, lookup, fromList)

import Data.Monoid

cypherExprFromArg2 arg = do
    res <- cypherExprFromArg arg
    case res of
        Left expr -> return expr
        Right _ -> error "unbounded var"

cypherBuiltIn :: CypherBuiltIn
cypherBuiltIn = CypherBuiltIn ( fromList [
        ("le", \thesign args ->
            cwhere <$> (CypherCompCond (case thesign of
                Pos -> "<="
                Neg -> ">") <$> cypherExprFromArg2 (head args) <*> cypherExprFromArg2 (args !! 1))),
        ("lt", \thesign args ->
            cwhere <$> (CypherCompCond (case thesign of
                Pos -> "<"
                Neg -> ">=") <$> cypherExprFromArg2 (head args) <*> cypherExprFromArg2 (args !! 1))),
        ("eq", \thesign args ->
            cwhere <$> (CypherCompCond (case thesign of
                Pos -> "="
                Neg -> "<>") <$> cypherExprFromArg2 (head args) <*> cypherExprFromArg2 (args !! 1))),
        ("like", \thesign args -> do
            pos <- CypherCompCond "=~" <$> cypherExprFromArg2 (head args) <*> cypherExprFromArg2 (args !! 1)
            cwhere <$> (case thesign of
                Pos -> return pos
                Neg -> return (CypherNotCond pos))),
        ("like_regex", \thesign args -> do
            pos <- CypherCompCond "=~" <$> cypherExprFromArg2 (head args) <*> cypherExprFromArg2 (args !! 1)
            cwhere <$> (case thesign of
                Pos -> return pos
                Neg -> return (CypherNotCond pos)))
    ])

cypherTrans :: CypherTrans
cypherTrans = CypherTrans cypherBuiltIn  (fromList [
        ("DATA_OBJ", mappingPattern0 "obj_id" "DataObject"),
        ("COLL_OBJ", mappingPattern0 "obj_id" "Collection"),
        ("META_OBJ", mappingPattern0 "obj_id" "AVU"),
        ("DATA_NAME", mappingPattern1 "obj_id" "DataObject" "data_name"),
        ("DATA_SIZE", mappingPattern1 "obj_id" "DataObject" "data_size"),
        ("COLL_NAME", mappingPattern1 "obj_id" "Collection" "coll_name"),
        ("DATA_REPL_NUM", mappingPattern1 "obj_id" "DataObject" "data_repl_num"),
        ("COLL", mappingPattern2 "obj_id" "DataObject"  "obj_id" "Collection"  "coll"),
        ("DATA_PATH", mappingPattern1 "obj_id" "DataObject" "data_path"),
        ("DATA_CHECKSUM", mappingPattern1 "obj_id" "DataObject" "data_checksum"),
        ("DATA_CREATE_TIME", mappingPattern1 "obj_id" "DataObject" "create_ts"),
        ("DATA_MODIFY_TIME", mappingPattern1 "obj_id" "DataObject" "modify_ts"),
        ("COLL_CREATE_TIME", mappingPattern1 "obj_id" "Collection" "create_ts"),
        ("COLL_MODIFY_TIME", mappingPattern1 "obj_id" "Collection" "modify_ts"),
        ("META", mappingPattern2m "obj_id" "obj_id" "meta"),
        ("META_ATTR_NAME", mappingPattern1 "obj_id" "AVU" "meta_attr_name"),
        ("META_ATTR_VALUE", mappingPattern1 "obj_id" "AVU" "meta_attr_value"),
        ("META_ATTR_UNITS", mappingPattern1 "obj_id" "AVU" "meta_attr_unit")
        ])


makeICATCypherDBAdapter :: Neo4jConnection -> GenericDB   Neo4jConnection CypherTrans
makeICATCypherDBAdapter conn = GenericDB conn "ICAT" standardPreds cypherTrans


mappingPattern0 :: String -> String -> (CypherMapping, CypherMapping, CypherMapping)
mappingPattern0 id nodetype =
        (
            (
                [dot "1" id],
                match [nodevl "1" nodetype]
            ), (
                [var "1"],
                create [nodevlp "0" nodetype [(id, var "1")]]
            ), (
                [var "1"],
                match [nodevl "0" nodetype] <> cwhere (dot "0" id .=. var "1") <> delete ["0"]
            )
        )
mappingPattern1 :: String -> String -> String -> (CypherMapping, CypherMapping, CypherMapping)
mappingPattern1 id nodetype prop =
        (
            (
                [dot "1" id, dot "1" prop],
                match [nodevl "1" nodetype]
            ), (
                [var "1", var "2"],
                match [nodevl "0" nodetype] <> cwhere (dot "0" id .=. var "1") <> set [(dot "0" prop, var "2")]
            ), (
                [var "1", var "2"],
                match [nodevl "0" nodetype] <> cwhere (dot "0" id .=. var "1") <> set [(dot "0" prop, cnull)]
            )
        )
mappingPattern2 :: String -> String -> String -> String -> String -> (CypherMapping, CypherMapping, CypherMapping)
mappingPattern2 id1 nodetype1 id2 nodetype2 edge =
            (
                (
                    [dot "1" id1, dot "2" id2],
                    match [edgel (nodevl "1" nodetype1) edge (nodevl "2" nodetype2)]
                ), (
                    [var "1", var "2"],
                    match [nodevl "d" nodetype1, nodevl "c" nodetype2]
                        <> cwhere (dot "d" id1 .=. var "1")
                        <> cwhere (dot "c" id2 .=. var "2")
                        <> create [edgel (nodev "d") edge (nodev "c")]
                ), (
                    [var "1", var "2"],
                    match [edgevl (nodevl "d" nodetype1) "e" edge (nodevl "c" nodetype2)]
                        <> cwhere (dot "d" id1 .=. var "1")
                        <> cwhere (dot "c" id2 .=. var "2")
                        <> delete ["e"]
                )
            )

mappingPattern2m :: String -> String -> String ->  (CypherMapping, CypherMapping, CypherMapping)
mappingPattern2m id1 id2 edge =
            (
                (
                    [dot "1" id1, dot "2" id2],
                    match [edgel (nodev "1") edge (nodev "2")]
                ), (
                    [var "1", var "2"],
                    match [nodev "d", nodev "c"]
                        <> cwhere (dot "d" id1 .=. var "1")
                        <> cwhere (dot "c" id2 .=. var "2")
                        <> create [edgel (nodev "d") edge (nodev "c")]

                ), (
                    [var "1", var "2"],
                    match [edgevl (nodev "d") "e" edge (nodev "c")]
                        <> cwhere (dot "d" id1 .=. var "1")
                        <> cwhere (dot "c" id2 .=. var "2")
                        <> delete ["e"]
                )
            )
