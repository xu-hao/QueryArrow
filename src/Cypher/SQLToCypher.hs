module Cypher.SQLToCypher where

import Prelude hiding (lookup)
import Cypher.Cypher
import SQL.SQL
import Data.Map.Strict (foldrWithKey, empty, lookup, insert, Map, (!))
import Data.List (partition)
import Data.String.Utils
import FO.Data
import Debug.Trace

sqlToCypher :: Map String String -> Map String String -> PredMap -> PredTableMap -> CypherPredTableMap
sqlToCypher tabletype colprop predtable mappings =
    foldrWithKey (\key (OneTable tablename _, qcols) mappings' ->
        let lookup2 tablename0 table =
                case lookup tablename0 table  of
                    Nothing -> tablename0
                    Just t -> t
            cols = map snd qcols
            (Pred _ predtype) = predtable ! key
            keyCols = keyComponents predtype cols
            propCols = propComponents predtype cols
            (keyPropCols, propCols2) = partition (endswith "_id") propCols
            edges = map (\key0 -> lookup2 key0 colprop) keyCols
            propEdges = map (\pc -> lookup2 pc colprop) keyPropCols
            props = map (\pc -> lookup2 pc colprop) propCols2
            ty = lookup2 tablename tabletype
            mapping' = case propCols of
                    [] -> case edges of
                        [keyCol] -> mappingPattern0 keyCol ty
                        [keyCol1, keyCol2] -> mappingPattern2m keyCol1 keyCol2 key
                        _ -> mappingPattern3 edges edges propEdges propEdges key
                    _ -> case (edges, propEdges) of
                        ([keyCol], []) ->   mappingPattern1 keyCol ty props
                        ([keyCol1, keyCol2], []) -> mappingPattern4m keyCol1  keyCol2   key props
                        ([keyCol1], [keyCol2]) -> mappingPattern4prop keyCol1  keyCol2   key props
                        _ -> mappingPattern5 edges edges propEdges propEdges key props
        in
            insert key mapping' mappings') empty mappings

mappingPattern0 :: String -> String -> CypherMapping
mappingPattern0 id nodetype =
        (
                [CypherVar "1"],
                GraphPattern [] [],
                GraphPattern [] [nodevlp "0" nodetype [(PropertyKey id, var "1")]],
                [(CypherVar "0", [CypherVar "1"])]
        )

mappingPattern1 :: String -> String -> [String] -> CypherMapping
mappingPattern1 id nodetype props =
    let propvars = map show [2..length props + 1] in
        (
                [CypherVar"1"] ++ map CypherVar propvars,
                GraphPattern [nodevlp "0" nodetype [(PropertyKey id, var "1")]] [],
                GraphPattern [] [nodevlp "0" nodetype (zipWith (\prop v -> (PropertyKey prop, var v)) props propvars)],
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
mappingPattern2prop::String -> String -> String -> CypherMapping
mappingPattern2prop id1 id2 edge =
            (
                [CypherVar"1", CypherVar"2"],
                GraphPattern [nodevp "d" [(PropertyKey id1, var "1")], nodevp "c" [(PropertyKey id2, var "2")]] [],
                GraphPattern [] [edgevl (nodev "d") "e" edge (nodev "c")],
                [(CypherVar"d", [CypherVar"1"]), (CypherVar"e", [CypherVar"1"]), (CypherVar"c", [CypherVar"2"])]
            )

mappingPattern3 :: [String] -> [String] -> [String] -> [String] -> String -> CypherMapping
mappingPattern3 keyedges keyedgeids propedges propedgeids nodetype =
    let edges = keyedges ++ propedges
        edgeids = keyedgeids ++ propedgeids
        keyvars = map show [1..length keyedges]
        propvars = map show [length keyedges + 1 .. length propedges]
        vars = keyvars ++ propvars
        nodevars = map ("d" ++) vars
        keycyphervars = map CypherVar keyvars
        cyphervars = map CypherVar vars in
                (
                        cyphervars,
                        GraphPattern (zipWith3 (\edgeid nv v -> nodevp nv [(PropertyKey edgeid, var v)]) edgeids nodevars vars) [],
                        GraphPattern [] ([nodevl "d0" nodetype] ++ zipWith (\edge nv -> edgevl (nodev "d0") "e" edge (nodev nv)) edges nodevars),
                        [(CypherVar "d0", keycyphervars)] ++ map (\(nv , v) -> (CypherVar nv, [v])) (zip nodevars cyphervars)
                )
mappingPattern4m :: String -> String -> String -> [String] -> CypherMapping
mappingPattern4m  nodeid1 nodeid2 edge props =
    let propvars = map show [3..length props+2] in
                            (
                                [CypherVar"1", CypherVar"2"] ++ (map CypherVar propvars),
                                GraphPattern [nodevp "d"  [(PropertyKey nodeid1, var "1")], nodevp "c" [(PropertyKey nodeid2, var "2")], edgevl (nodev "d") "e" edge  (nodev "c")] [],
                                GraphPattern [] [nodevp "e" (zipWith (\prop v -> (PropertyKey prop, var v)) props propvars)],
                                [(CypherVar"d", [CypherVar"1"]), (CypherVar"e", [CypherVar"1", CypherVar"2"]), (CypherVar"c", [CypherVar"2"])]
                            )
mappingPattern4prop :: String -> String -> String -> [String] -> CypherMapping
mappingPattern4prop  nodeid1 nodeid2 edge props =
    let propvars = map show [3..length props+2] in
                            (
                                [CypherVar"1", CypherVar"2"] ++ (map CypherVar propvars),
                                GraphPattern [nodevp "d"  [(PropertyKey nodeid1, var "1")], nodevp "c" [(PropertyKey nodeid2, var "2")]] [],
                                GraphPattern [] [edgevlp (nodev "d") "e" edge (zipWith (\prop v -> (PropertyKey prop, var v)) props propvars) (nodev "c")],
                                [(CypherVar"d", [CypherVar"1"]), (CypherVar"e", [CypherVar"1"]), (CypherVar"c", [CypherVar"2"])]
                            )
mappingPattern5 :: [String] -> [String] -> [String] -> [String] -> String -> [String] -> CypherMapping
mappingPattern5 keyedges keyedgeids propedges propedgeids nodetype props =
    let edges = keyedges ++ propedges
        edgeids = keyedgeids ++ propedgeids
        keyvars = map show [1..length keyedges]
        propvars = map show [length keyedges + 1 .. length propedges]
        vars = keyvars ++ propvars
        propvars2 = map show [length edges + 1..length edges + length props]
        nodevars = map ("d" ++) vars
        keycyphervars = map CypherVar keyvars
        cyphervars = map CypherVar vars in
                (
                        cyphervars ++ (map CypherVar propvars2),
                        GraphPattern ([nodevl "d0" nodetype] ++ zipWith3 (\edgeid nv v -> nodevp nv [(PropertyKey edgeid, var v)]) edgeids nodevars vars ++ zipWith (\edge nv -> edgevl (nodev "d0") "e" edge (nodev nv)) edges nodevars) [],
                        GraphPattern [] [nodevp "d0" (zipWith (\prop v -> (PropertyKey prop, var v)) props propvars)],
                        [(CypherVar "d0", keycyphervars)] ++ zipWith (\nv v -> (CypherVar nv, [v])) nodevars cyphervars
                )
