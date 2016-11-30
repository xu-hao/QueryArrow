module Cypher.SQLToCypher where

import Prelude hiding (lookup)
import Cypher.Cypher
import SQL.SQL
import Data.Map.Strict (foldrWithKey, empty, lookup, insert, Map)
import Data.List (partition)
import FO.Data
import ListUtils
import Debug.Trace

type ColI = (String, Int)
-- | there are three type of graph patterns used
-- 1. a node with an id property
-- 2. an edge connecting two nodes
-- 3. a node with edges connecting three or more nodes
-- they are used to model tables with a primary key consisting of, respectively,
-- 1. one id and zero or more primitive values
-- 2. two foreign keys and zero or more primitive values
-- 3. three or more foregin keys and zero or more primitive values
sqlToCypher :: Map String String -> Map String String -> PredTableMap -> CypherPredTableMap
sqlToCypher tabletype colprop  mappings =
    foldrWithKey (\predtype (OneTable tablename _, qcols) mappings' ->
        let lookup2 tablename0 table =
                case lookup tablename0 table  of
                    Nothing -> tablename0
                    Just t -> t
            cols = map ((\key0 -> lookup2 key0 colprop) . snd) qcols
            colsi = zip cols [1..length cols]
            keycols = keyComponents predtype colsi
            propcols = propComponents predtype colsi
            (keyedges, keyprops) = partition (\(col, _) -> col /= "access_type_id") keycols
            (propedges, propprops) = partition (\(col, _) -> endswith "_id" col) propcols
            ty = lookup2 tablename tabletype
            mapping' = case propcols of
                    [] -> case keyedges of
                        [keyedge] -> mappingPattern0 keyedge ty keyprops
                        [keyedge1, keyedge2] -> mappingPattern2m keyedge1 keyedge2 ty keyprops
                        _ -> mappingPattern3 keyedges keyedges ty keyprops
                    _ -> case (keyedges, propedges) of
                        ([keyedge], _) -> mappingPattern1 keyedge propedges propedges ty keyprops propprops
                        ([keyedge1, keyedge2], []) -> mappingPattern4m keyedge1 keyedge2 ty keyprops propprops
                        _ -> mappingPattern5 keyedges keyedges propedges propedges ty keyprops propprops
        in
            insert predtype mapping' mappings') empty mappings


nodePattern :: [ColI] -> [OneGraphPattern]
nodePattern = map (\(prop, v) -> nodevp ("d" ++ show v) [(PropertyKey prop, var (show v))])
propPatternNode :: String -> [ColI] -> OneGraphPattern
propPatternNode nodetype props = nodevlp "d0" nodetype (map (\(prop, v) -> (PropertyKey prop, var (show v))) props)
edgePattern :: [ColI] -> [OneGraphPattern]
edgePattern = map (\(prop, v) -> edgevl (nodev "d0") ("e" ++ show v) prop (nodev ("d" ++ show v)))
propPatternEdge :: String -> ColI -> ColI -> [ColI] -> OneGraphPattern
propPatternEdge nodetype (_, v1) (_, v2) props = edgevlp (nodev (show v1)) "e0" nodetype (map (\(prop, v) -> (PropertyKey prop, var (show v))) props) (nodev (show v2))
vars :: [ColI] -> [CypherVar]
vars = map (CypherVar . show . snd)

-- | a node of type `nodetype` with one key edge and zero or more key properties
-- a key edge is part of the primary key
-- a key property is part of the primary key and also a primitive value
mappingPattern0 :: ColI -> String -> [ColI] -> CypherMapping
mappingPattern0 id nodetype keyprops =
    (
        map (CypherVar . show) [1..length keyprops + 1],
        GraphPattern [],
        GraphPattern [propPatternNode nodetype (id : keyprops)],
        [(CypherVar "d0", vars (id : keyprops))]
    )

-- | an edge of type `edge` connecting two nodes with two key edges and zero or more key properties
mappingPattern2m::ColI -> ColI -> String -> [ColI] -> CypherMapping
mappingPattern2m id1 id2 nodetype keyprops =
    (
        map (CypherVar . show) [1..length keyprops + 2],
        GraphPattern (nodePattern [id1, id2]),
        GraphPattern (propPatternEdge nodetype id1 id2 keyprops : edgePattern [id1, id2]),
        [(CypherVar "e0", vars (id1 : id2 : keyprops))]
    )

-- | a node of type `nodetype` with two or more key edges and zero or more key properties
-- when there are more than one key edges, they are also foreign keys
mappingPattern3 :: [ColI] -> [ColI] -> String  -> [ColI] -> CypherMapping
mappingPattern3 keyedges keyids nodetype keyprops =
    (
        map (CypherVar . show) [1..length keyprops + length keyedges],
        GraphPattern (nodePattern keyids),
        GraphPattern (propPatternNode nodetype keyprops : edgePattern keyedges),
        [(CypherVar "e0", vars (keyids ++ keyprops))]
    )

-- | a node of type `nodetype` with one key edge, zero or more key properties, and one or more properties
mappingPattern1 :: ColI -> [ColI] -> [ColI] -> String -> [ColI] -> [ColI] -> CypherMapping
mappingPattern1 id propedges propids nodetype keyprops propprops =
    (
        map (CypherVar . show) [1..length keyprops + length propprops + length propedges + 1],
        GraphPattern (propPatternNode nodetype (id : keyprops) : nodePattern propids),
        GraphPattern (propPatternNode nodetype propprops : edgePattern propedges),
        [(CypherVar "d0", vars (id : keyprops))]
    )

-- | a edge of type `edge` connecting two nodes with two key edges, zero or more key properties, and one or more properties
mappingPattern4m :: ColI -> ColI -> String -> [ColI] -> [ColI] -> CypherMapping
mappingPattern4m id1 id2 nodetype keyprops propprops =
    (
        map (CypherVar . show) [1..length keyprops + length propprops + 2],
        GraphPattern (propPatternEdge nodetype id1 id2 keyprops : nodePattern [id1, id2] ++ edgePattern [id1, id2]),
        GraphPattern [propPatternEdge nodetype id1 id2 propprops],
        [(CypherVar "e0", vars (id1 : id2 : keyprops))]
    )

-- | a node of type `nodetype` with two or more key edges, zero or more key properties, and one or more properties
mappingPattern5 :: [ColI] -> [ColI] -> [ColI] -> [ColI] -> String -> [ColI] -> [ColI] -> CypherMapping
mappingPattern5 keyedges keyids propedges propids nodetype keyprops propprops =
    (
        map (CypherVar . show) [1..length keyprops + length keyedges + length propedges + length propprops],
        GraphPattern (propPatternNode nodetype keyprops : nodePattern (propids ++ keyids) ++ edgePattern keyedges),
        GraphPattern (propPatternNode nodetype propprops : edgePattern propedges),
        [(CypherVar "d0", vars (keyids ++ keyprops))]
    )
