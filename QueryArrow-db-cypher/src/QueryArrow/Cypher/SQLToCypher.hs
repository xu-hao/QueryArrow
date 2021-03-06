module QueryArrow.Cypher.SQLToCypher where

import Prelude hiding (lookup)
import QueryArrow.Cypher.Cypher
import QueryArrow.SQL.SQL
import Data.Map.Strict (foldrWithKey, empty, lookup, insert, Map, fromList)
import Data.List (partition)
import QueryArrow.Syntax.Type
import QueryArrow.Syntax.Term (PredTypeMap)
import QueryArrow.ListUtils
import Control.Arrow ((***))
import Data.Maybe (fromMaybe)

type ColI = (String, Int)

tabletype :: Map String String
tabletype = fromList [
    ("r_data_main", "DataObject"),
    ("r_coll_main", "Collection")
    ]

foreignKeys :: Map String String
foreignKeys =  fromList [
    ("data_id", "object_id"),
    ("coll_id", "object_id")
    ]

colprop :: Map String String
colprop =  mempty

lookup2 :: (Ord a) => a -> Map a a -> a
lookup2 tablename0 table =
        fromMaybe tablename0 (lookup tablename0 table)

endsWith :: String -> String -> Bool
endsWith a b =
      length a >= length b && drop (length a - length b) a == b

sqlToCypher :: PredTypeMap -> PredTableMap -> CypherPredTableMap
sqlToCypher ptm mappings =
        foldrWithKey (\predname (tablename, cols) mappings' ->
            let cols1 = map (\key0 -> lookup2 key0 colprop) cols
                colsi = zip cols1 [1..length cols1]
                predtype = fromMaybe (error ("sqlToCypher: cannot find predicate " ++ show predname)) (lookup predname ptm)
                keycols = keyComponentsParamType predtype colsi
                propcols = propComponentsParamType predtype colsi
                (keyedges, keyprops) = (map snd *** map snd) (
                      case length keycols of
                        1 -> ([], keycols)
                        _ -> partition (\(pt, _) -> isReference pt) keycols)
                (propedges, propprops) = (map snd *** map snd) (partition (\(pt, _) -> isReference pt) propcols)
                ty = lookup2 tablename tabletype
                foreignkeyedges = map ((\key -> lookup2 key foreignKeys) *** id) keyedges
                mapping' = case propcols of
                        [] -> mappingPattern3 keyedges foreignkeyedges ty keyprops
                        _ -> mappingPattern5 keyedges foreignkeyedges propedges propedges ty keyprops propprops
            in
                insert predname mapping' mappings') empty mappings


nodePattern :: [ColI] -> [OneGraphPattern]
nodePattern = map (\(prop, v) -> nodevp ("d" ++ show v) [(PropertyKey prop, var (show v))])
propPatternNode :: String -> [ColI] -> OneGraphPattern
propPatternNode nodetype props = nodevlp "d0" nodetype (map (\(prop, v) -> (PropertyKey prop, var (show v))) props)
edgePattern :: [ColI] -> [OneGraphPattern]
edgePattern = map (\(prop, v) -> edgevl (nodev "d0") ("e" ++ show v) prop (nodev ("d" ++ show v)))
propPatternEdge :: String -> ColI -> ColI -> [ColI] -> OneGraphPattern
propPatternEdge nodetype (_, v1) (_, v2) props = edgevlp (nodev ("d" ++ show v1)) "e0" nodetype (map (\(prop, v) -> (PropertyKey prop, var (show v))) props) (nodev ("d" ++ show v2))
vars :: [ColI] -> [CypherVar]
vars = map (CypherVar . show . snd)

-- | a node of type `nodetype` no properties
mappingPattern3 :: [ColI] -> [ColI] -> String  -> [ColI] -> CypherMapping
mappingPattern3 keyedges keyids nodetype keyprops =
    (
        map (CypherVar . show) [1..length keyprops + length keyedges],
        GraphPattern (nodePattern keyids),
        GraphPattern (propPatternNode nodetype keyprops : edgePattern keyedges),
        [(CypherVar "e0", vars (keyids ++ keyprops))]
    )

-- | a node of type `nodetype` one or more properties
mappingPattern5 :: [ColI] -> [ColI] -> [ColI] -> [ColI] -> String -> [ColI] -> [ColI] -> CypherMapping
mappingPattern5 keyedges keyids propedges propids nodetype keyprops propprops =
    (
        map (CypherVar . show) [1..length keyprops + length keyedges + length propedges + length propprops],
        GraphPattern (propPatternNode nodetype keyprops : nodePattern (propids ++ keyids) ++ edgePattern keyedges),
        GraphPattern (propPatternNode nodetype propprops : edgePattern propedges),
        [(CypherVar "d0", vars (keyids ++ keyprops))]
    )
