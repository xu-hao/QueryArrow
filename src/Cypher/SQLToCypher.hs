module Cypher.SQLToCypher where

import Prelude hiding (lookup)
import Cypher.Cypher
import SQL.SQL
import Data.Map.Strict (foldrWithKey, empty, lookup, insert, Map, (!))
import Data.List (partition)
import FO.Data
import ListUtils
import Debug.Trace

sqlToCypher :: Map String String -> Map String String -> PredTableMap -> CypherPredTableMap
sqlToCypher tabletype colprop  mappings =
    foldrWithKey (\predtype (OneTable tablename _, qcols) mappings' ->
        let lookup2 tablename0 table =
                case lookup tablename0 table  of
                    Nothing -> tablename0
                    Just t -> t
            cols = map snd qcols
            key = predName predtype
            keyCols = keyComponents predtype cols
            propCols = propComponents predtype cols
            (edgeKeyCols, propKeyCols) = partition (/= "access_type_id") keyCols
            (edgePropCols, propPropCols) = partition (endswith "_id") propCols
            edges = map (\key0 -> lookup2 key0 colprop) edgeKeyCols
            propEdges = map (\pc -> lookup2 pc colprop) edgePropCols
            keyprops = map (\pc -> lookup2 pc colprop) propKeyCols
            propprops = map (\pc -> lookup2 pc colprop) propPropCols
            ty = lookup2 tablename tabletype
            mapping' = case propprops of
                    [] -> case edges of
                        [keyCol] -> mappingPattern0 keyCol ty keyprops
                        [keyCol1, keyCol2] -> mappingPattern2m keyCol1 keyCol2 (predNameToString2 key) keyprops
                        _ -> mappingPattern3 edges edges propEdges propEdges (predNameToString2 key) keyprops
                    _ -> case (edges, propEdges) of
                        ([keyCol], []) ->   mappingPattern1 keyCol ty keyprops propprops
                        ([keyCol1, keyCol2], []) -> mappingPattern4m keyCol1  keyCol2   (predNameToString2 key) keyprops propprops
                        ([keyCol1], [keyCol2]) -> mappingPattern4prop keyCol1  keyCol2   (predNameToString2 key) keyprops propprops
                        _ -> mappingPattern5 edges edges propEdges propEdges (predNameToString2 key) keyprops propprops
        in
            insert predtype mapping' mappings') empty mappings

mappingPattern0 :: String -> String -> [String] -> CypherMapping
mappingPattern0 id nodetype keyprops =
    let keypropvars = map show [2..length (keyprops) + 1] in
        (
                [CypherVar "1"] ++ map CypherVar keypropvars,
                GraphPattern [],
                GraphPattern [nodevlp "0" nodetype ([(PropertyKey id, var "1")]  ++ zipWith (\prop v -> (PropertyKey prop, var v)) keyprops keypropvars )],
                [(CypherVar "0", [CypherVar "1"] ++ map CypherVar keypropvars)]
        )

mappingPattern1 :: String -> String -> [String] -> [String] -> CypherMapping
mappingPattern1 id nodetype keyprops props =
    let keypropvars = map show [2..length (keyprops) + 1]
        proppropvars = map show [length keyprops + 2..length (keyprops ++ props) + 1]
        propvars = keypropvars ++ proppropvars in
        (
            [CypherVar "1"] ++ map CypherVar propvars,
            GraphPattern [nodevlp "0" nodetype [(PropertyKey id, var "1")]],
            GraphPattern [nodevlp "0" nodetype (zipWith (\prop v -> (PropertyKey prop, var v)) keyprops keypropvars ++ zipWith (\prop v -> (PropertyKey prop, var v)) props proppropvars)],
            [(CypherVar "0", [CypherVar "1"] ++ map CypherVar keypropvars)]
        )
mappingPattern2 :: String -> String -> String -> String -> String -> [String] -> CypherMapping
mappingPattern2 id1 nodetype1 id2 nodetype2 edge keyprops =
    let keypropvars = map show [3..length (keyprops) + 2] in
    (
        [CypherVar "1", CypherVar "2"] ++ map CypherVar keypropvars,
        GraphPattern [nodevlp "d" nodetype1 [(PropertyKey id1, var "1")], nodevlp "c" nodetype2 [(PropertyKey id2, var "2")]],
        GraphPattern [edgevlp (nodev "d") "e" edge (zipWith (\prop v -> (PropertyKey prop, var v)) keyprops keypropvars) (nodev "c")],
        [(CypherVar "d", [CypherVar "1"] ++ map CypherVar keypropvars), (CypherVar "e", [CypherVar "1", CypherVar "2"]), (CypherVar "c", [CypherVar "2"])]
    )

mappingPattern2m::String -> String -> String -> [String] -> CypherMapping
mappingPattern2m id1 id2 edge keyprops =
    let keypropvars = map show [3..length (keyprops) + 2] in
    (
        [CypherVar "1", CypherVar "2"] ++ map CypherVar keypropvars,
        GraphPattern [nodevp "d" [(PropertyKey id1, var "1")], nodevp "c" [(PropertyKey id2, var "2")]],
        GraphPattern [edgevlp (nodev "d") "e" edge (zipWith (\prop v -> (PropertyKey prop, var v)) keyprops keypropvars) (nodev "c")],
        [(CypherVar "d", [CypherVar "1"] ++ map CypherVar keypropvars), (CypherVar "e", [CypherVar "1", CypherVar "2"]), (CypherVar "c", [CypherVar "2"])]
    )

mappingPattern2prop::String -> String -> String -> [String] -> CypherMapping
mappingPattern2prop id1 id2 edge keyprops =
    let keypropvars = map show [3..length (keyprops) + 2] in
    (
        [CypherVar "1", CypherVar "2"] ++ map CypherVar keypropvars,
        GraphPattern [nodevp "d" [(PropertyKey id1, var "1")], nodevp "c" [(PropertyKey id2, var "2")]],
        GraphPattern [edgevlp (nodev "d") "e" edge (zipWith (\prop v -> (PropertyKey prop, var v)) keyprops keypropvars) (nodev "c")],
        [(CypherVar "d", [CypherVar "1"] ++ map CypherVar keypropvars), (CypherVar "e", [CypherVar "1"]), (CypherVar "c", [CypherVar "2"])]
    )

mappingPattern3 :: [String] -> [String] -> [String] -> [String] -> String  -> [String] -> CypherMapping
mappingPattern3 keyedges keyedgeids propedges propedgeids nodetype keyprops =
    let edges = keyedges ++ propedges
        edgeids = keyedgeids ++ propedgeids
        keyvars = map show [1..length keyedges]
        propedgevars = map show [length keyedges + 1.. length keyedges + length propedges]
        keypropvars = map show [length keyedges + length propedges + 1..length keyedges + length propedges + length (keyprops)]
        vars = keyvars ++ propedgevars
        nodevars = map ("d" ++) vars
        edgevars = map ("e" ++) vars
        keycyphervars = map CypherVar (keyvars ++ keypropvars)
        cyphervars = map CypherVar (vars ++ keypropvars) in
            (
                cyphervars,
                GraphPattern (zipWith3 (\edgeid nv v -> nodevp nv [(PropertyKey edgeid, var v)]) edgeids nodevars vars),
                GraphPattern ([nodevlp "d0" nodetype (zipWith (\prop v -> (PropertyKey prop, var v)) keyprops keypropvars)] ++ zipWith3 (\edge ev nv -> edgevl (nodev "d0") ev edge (nodev nv)) edges edgevars nodevars),
                [(CypherVar "d0", keycyphervars)] ++ map (\(nv , v) -> (CypherVar nv, [v])) (zip nodevars (map CypherVar vars))
            )

mappingPattern4m :: String -> String -> String -> [String] -> [String] -> CypherMapping
mappingPattern4m  nodeid1 nodeid2 edge keyprops props =
    let keypropvars = map show [3..length (keyprops) + 2]
        proppropvars = map show [length keyprops + 3..length (keyprops ++ props) + 2]
        propvars = keypropvars ++ proppropvars in
            (
                [CypherVar "1", CypherVar "2"] ++ (map CypherVar propvars),
                GraphPattern [nodevp "d"  [(PropertyKey nodeid1, var "1")], nodevp "c" [(PropertyKey nodeid2, var "2")], edgevl (nodev "d") "e" edge  (nodev "c")],
                GraphPattern [nodevp "e" (zipWith (\prop v -> (PropertyKey prop, var v)) keyprops keypropvars ++ zipWith (\prop v -> (PropertyKey prop, var v)) props proppropvars)],
                [(CypherVar "d", [CypherVar "1"] ++ map CypherVar keypropvars), (CypherVar "e", [CypherVar "1", CypherVar "2"]++ map CypherVar keypropvars), (CypherVar "c", [CypherVar "2"])]
            )

mappingPattern4prop :: String -> String -> String -> [String] -> [String] -> CypherMapping
mappingPattern4prop  nodeid1 nodeid2 edge keyprops props =
    let keypropvars = map show [3..length (keyprops) + 2]
        proppropvars = map show [length keyprops + 3..length (keyprops ++ props) + 2]
        propvars = keypropvars ++ proppropvars in
            (
                [CypherVar "1", CypherVar "2"] ++ (map CypherVar propvars),
                GraphPattern [nodevp "d"  [(PropertyKey nodeid1, var "1")], nodevp "c" [(PropertyKey nodeid2, var "2")]],
                GraphPattern [edgevlp (nodev "d") "e" edge (zipWith (\prop v -> (PropertyKey prop, var v)) keyprops keypropvars ++ zipWith (\prop v -> (PropertyKey prop, var v)) props propvars) (nodev "c")],
                [(CypherVar "d", [CypherVar"1"] ++ map CypherVar keypropvars), (CypherVar "e", [CypherVar "1"]++ map CypherVar keypropvars), (CypherVar "c", [CypherVar "2"])]
            )

mappingPattern5 :: [String] -> [String] -> [String] -> [String] -> String -> [String] -> [String] -> CypherMapping
mappingPattern5 keyedges keyedgeids propedges propedgeids nodetype keyprops props =
    let edges = keyedges ++ propedges
        edgeids = keyedgeids ++ propedgeids
        keyedgevars = map show [1..length keyedges]
        propedgevars = map show [length keyedges + 1 .. length propedges]
        keypropvars = map show [length edges + 1..length edges + length keyprops]
        proppropvars = map show [length edges + length keypropvars + 1..length edges + length keypropvars + length props]
        vars = keyedgevars ++ propedgevars
        edgevars = map ("e" ++) vars
        nodevars = map ("d" ++) vars
        keycyphervars = map CypherVar (keyedgevars ++ keypropvars)
        cyphervars = map CypherVar vars in
            (
                cyphervars ++ map CypherVar (keypropvars ++ proppropvars),
                GraphPattern ([nodevl "d0" nodetype] ++ zipWith3 (\edgeid nv v -> nodevp nv [(PropertyKey edgeid, var v)]) edgeids nodevars vars ++ zipWith3 (\edge ev nv -> edgevl (nodev "d0") ev edge (nodev nv)) edges edgevars nodevars),
                GraphPattern [nodevp "d0" (zipWith (\prop v -> (PropertyKey prop, var v)) keyprops keypropvars ++ zipWith (\prop v -> (PropertyKey prop, var v)) props proppropvars)],
                [(CypherVar "d0", keycyphervars)] ++ zipWith (\nv v -> (CypherVar nv, [v])) nodevars cyphervars
            )
