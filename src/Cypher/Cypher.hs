{-# LANGUAGE FlexibleInstances, MultiParamTypeClasses, FlexibleContexts, RankNTypes, GADTs, TypeFamilies #-}
module Cypher.Cypher (CypherVar(..), CypherOper, CypherExpr(..), Label,
    CypherMapping, translateQueryToCypher,
    CypherCond(..), CypherQuery(..), GraphPattern(..), NodePattern(..), PropertyKey(..), Cypher(..), cypherVarExprMap, CypherVarExprMap(..),
    CypherBuiltIn(..), CypherPredTableMap, OneGraphPattern(..),CypherTrans(..), (.=.), (.<>.), (.&&.),
    nodev, nodel, nodevl, nodevp, nodevlp, nodelp, nodep, edgel, edgevl, edgevlp, var, cnull, dot, app, match,
    create, set, delete, cwhere, creturn) where

import FO.Data hiding (Subst, subst, Unify,unify)
import DB.GenericDatabase
import DB.DB
import ListUtils

import Prelude hiding (lookup)
import Data.List (intercalate, (\\), union , partition, intersect, nub)
import Control.Monad.Trans.State.Strict (State, StateT, get, put, evalState, runState, evalStateT, runStateT)
import Control.Monad (foldM, guard)
import Control.Arrow ((***))
import Data.Map.Strict (empty, Map, insert, member, foldlWithKey, lookup, fromList, toList, elems, update, keys)
import qualified Data.Map.Strict as Map
import Data.Convertible.Base
import Data.Monoid
import Data.Maybe
import Control.Applicative ((<$>))
import Control.Monad.Trans.Class
import qualified Data.Text as T
import Data.Set (toAscList)
import qualified Data.Set as Set
import Algebra.Lattice

-- basic definitions

newtype CypherVar = CypherVar {unCypherVar::String} deriving (Eq, Ord, Show, Read)

data CypherExpr = CypherVarExpr CypherVar
                | CypherIntConstExpr Int
                | CypherStringConstExpr T.Text
                | CypherParamExpr String
                | CypherDotExpr CypherExpr PropertyKey
                | CypherAppExpr String [CypherExpr]
                | CypherNullExpr deriving (Eq, Ord, Show, Read)

extractVarFromExpr :: CypherExpr -> CypherVar
extractVarFromExpr (CypherVarExpr var) = var
extractVarFromExpr _ = error "this is not a var expr"

isCypherConstExpr :: CypherExpr -> Bool
isCypherConstExpr e = case e of CypherIntConstExpr _ -> True; CypherStringConstExpr _ -> True ; _ -> False

type CypherOper = String

data CypherValue = CypherIntValue Int
                 | CypherStringValue String
                 | CypherNullValue deriving (Show, Read)

data CypherCond = CypherCompCond CypherOper CypherExpr CypherExpr Sign
                | CypherAndCond CypherCond CypherCond
                -- | CypherOrCond CypherCond CypherCond
                | CypherTrueCond
                -- | CypherFalseCond
                -- | CypherNotCond CypherCond
                -- | CypherExistsCond CypherCond
                -- | CypherNotExistsCond CypherCond
                | CypherPatternCond GraphPattern deriving (Eq, Show, Read)

getConjuncts :: CypherCond -> [CypherCond]
getConjuncts (CypherAndCond c1 c2) = getConjuncts c1 ++ getConjuncts c2
getConjuncts c = [c]

-- getDisjuncts :: CypherCond -> [CypherCond]
-- getDisjuncts (CypherOrCond c1 c2) = getDisjuncts c1 ++ getDisjuncts c2
-- getDisjuncts c = [c]

conj :: [CypherCond] -> CypherCond
conj = foldl (.&&.) CypherTrueCond

-- disj :: [CypherCond] -> CypherCond
-- disj = foldl (.||.) CypherFalseCond

(.&&.) :: CypherCond -> CypherCond -> CypherCond
CypherTrueCond .&&. b = b
-- CypherFalseCond .&&. b = CypherFalseCond
a .&&. CypherTrueCond = a
-- a .&&. CypherFalseCond = CypherFalseCond
a .&&. b = CypherAndCond a b

-- (.||.) :: CypherCond -> CypherCond -> CypherCond
-- CypherFalseCond .||. b = b
-- CypherTrueCond .||. b = CypherTrueCond
-- a .||. CypherFalseCond = a
-- a .||. CypherTrueCond = CypherTrueCond
-- a .||. b = CypherOrCond a b

(.=.) :: CypherExpr -> CypherExpr -> CypherCond
a .=. b = CypherCompCond "=" a b Pos

(.<>.) :: CypherExpr -> CypherExpr -> CypherCond
a .<>. b = CypherCompCond "<>" a b Pos

data Cypher = Cypher {cypherReturn :: [ (CypherExpr, CypherVar) ], cypherMatch :: GraphPattern,
                        cypherWhere :: CypherCond, cypherSet :: [(CypherExpr, CypherExpr)],
                        cypherCreate :: GraphPattern, cypherDelete :: [CypherVar] } deriving (Eq, Show, Read)
-- graph patterns

type Label = String
newtype PropertyKey = PropertyKey String deriving (Eq, Ord, Show, Read)
type Properties = [(PropertyKey, CypherExpr)]
data NodePattern = NodePattern (Maybe CypherVar) (Maybe Label) Properties deriving (Eq, Show, Read)
data GraphPattern = GraphPattern [OneGraphPattern] deriving (Show, Read, Eq)
data OneGraphPattern = GraphEdgePattern NodePattern NodePattern NodePattern
                     | GraphNodePattern NodePattern deriving (Eq, Show, Read)

instance Monoid GraphPattern where
    mempty = GraphPattern []
    GraphPattern as `mappend` GraphPattern bs = GraphPattern (mergeOneGraphPatterns (as ++ bs))

data CypherVarType = CypherNodeVar | CypherPropertyVar deriving (Eq, Show)

data Context = Root | Node (Maybe Label) | Edge CypherVar CypherVar (Maybe Label) | Dot CypherVar PropertyKey deriving (Eq, Show)

class FVType a where
    fvType :: Context -> a -> [(CypherVar, Context, Properties, CypherVarType)]

instance FVType OneGraphPattern where
    fvType _ (GraphEdgePattern nodepatternsrc label nodepatterntgt) =
        fvType (Node Nothing) nodepatternsrc `union` fvType (Edge (nodeVar nodepatternsrc) (nodeVar nodepatterntgt) Nothing) label `union` fvType (Node Nothing) nodepatterntgt
    fvType _ (GraphNodePattern nodepattern) = fvType (Node Nothing) nodepattern

instance FVType GraphPattern where
    fvType _ (GraphPattern list)= unions (map (fvType Root) (list))

instance FVType NodePattern where
    fvType path nodepattern@(NodePattern var label props) =
        let v = fromMaybe (error "fvtype without var") var
            nonvarprops = filter (\(_, expr) -> case expr of
                CypherVarExpr _ -> False
                _ -> True) props in
            [(v, case label of
                    Just l -> addLabel l path
                    Nothing -> path, nonvarprops, CypherNodeVar)] `union` unions (map (\(prop, expr)-> fvType (Dot v prop ) expr) props)

instance FVType CypherExpr where
    fvType path (CypherVarExpr var) = [(var, path, [], CypherPropertyVar)]
    fvType _ _ = [] -- cannot be deleted

addLabel :: Label -> Context -> Context
addLabel l (Edge v v2 _) = Edge v v2 (Just l)
addLabel l (Node _) = Node (Just l)

nodeVar :: NodePattern -> CypherVar
nodeVar (NodePattern var label props) =
    fromMaybe (error "fvtype without var") var

compatible :: Eq a => Maybe a -> Maybe a -> Bool
compatible Nothing _ = True
compatible _ Nothing = True
compatible (Just a) (Just a') = a == a'

sameNode :: NodePattern -> NodePattern -> Bool
sameNode (NodePattern v l1 p1) (NodePattern v2 l2 p2) = v == v2 && l1 `compatible` l2 &&
    and [k1 /= k2 || e1 == e2 | (k1, e1) <- p1, (k2, e2) <- p2]

mergeNodes :: NodePattern -> NodePattern -> NodePattern
mergeNodes (NodePattern v l props) (NodePattern _ _ props2) = NodePattern v l (props `union` props2)

mergeTwoOneGraphPatterns :: OneGraphPattern -> OneGraphPattern -> Maybe OneGraphPattern
mergeTwoOneGraphPatterns (GraphNodePattern n) (GraphNodePattern n2)
    | sameNode n n2 = return (GraphNodePattern (mergeNodes n n2))
mergeTwoOneGraphPatterns (GraphEdgePattern ( ns) ne ( nt)) (GraphNodePattern n2)
    | sameNode ns n2 = return (GraphEdgePattern ( (mergeNodes ns n2)) ne ( nt))
    | sameNode nt n2 = return (GraphEdgePattern ( ns) ne ( (mergeNodes nt n2)))
mergeTwoOneGraphPatterns _ _ = Nothing

findMerge :: OneGraphPattern -> [OneGraphPattern] -> Maybe (OneGraphPattern, [OneGraphPattern])
findMerge _ [] = Nothing
findMerge p (r:rs) =
    case mergeTwoOneGraphPatterns p r of
        Nothing -> do
            (p', rs') <- findMerge p rs
            return (p', r : rs')
        Just p' ->
            return (p', rs)

mergeOneGraphPatterns :: [OneGraphPattern] -> [OneGraphPattern]
mergeOneGraphPatterns [] = []
mergeOneGraphPatterns (p:ps) =
    case findMerge p ps of
        Nothing -> p : mergeOneGraphPatterns ps
        Just (p', rest) -> mergeOneGraphPatterns (p' : rest)

splitGraphPattern::GraphPattern -> [GraphPattern]
splitGraphPattern (GraphPattern ps) = map (\p -> GraphPattern [p]) ps


-- show
instance Serialize OneGraphPattern where
    serialize (GraphNodePattern nodepattern) = serialize nodepattern
    serialize (GraphEdgePattern nodepatternsrc label nodepatterntgt) =
        serialize nodepatternsrc ++ "-[" ++ trimBrackest (serialize label) ++ "]->" ++ serialize nodepatterntgt
        where
            trimBrackest ('(' : t) = take (length t - 1) t
            trimBrackest t = t

instance Serialize NodePattern where
    serialize (NodePattern var label props) =
        "(" ++ (case var of
            Just v -> serialize v
            Nothing -> "") ++ (case label of
            Just l -> ":" ++ l
            Nothing -> "") ++ (if not (null props)
            then "{" ++ intercalate "," (f props) ++ "}"
            else "") ++ ")" where
        f = map (\(PropertyKey key, value) -> key ++ ":" ++ serialize value)

instance Serialize CypherExpr where
    serialize (CypherVarExpr var) = serialize var
    serialize (CypherIntConstExpr i) = show i
    serialize (CypherStringConstExpr s) = "'" ++ cypherStringEscape (T.unpack s) ++ "'"
    serialize (CypherParamExpr m) = "{" ++ m ++ "}"
    serialize (CypherDotExpr expr (PropertyKey prop)) = serialize expr ++ "." ++ prop
    serialize (CypherAppExpr f args) = f ++ "(" ++ intercalate "," (map serialize args) ++ ")"
    serialize (CypherNullExpr) = "NULL"

instance Serialize CypherVar where
    serialize (CypherVar var) = var

cypherStringEscape :: String -> String
cypherStringEscape = concatMap f where
    f '\t' = "\\t"
    f '\b' = "\\b"
    f '\n' = "\\n"
    f '\r' = "\\r"
    f '\f' = "\\f"
    f '\'' = "\\'"
    f '\"' = "\\\""
    f '\\' = "\\\\"
    f a = [a]

{- showN _ 0 = "..."
showN (CypherCompCond op lhs rhs s) n = case s of
    Pos -> show lhs ++ " " ++ op ++ " " ++ show rhs
    Neg -> "NOT (" ++ show lhs ++ " " ++ op ++ " " ++ show rhs ++ ")"
showN (CypherTrueCond) _ = "@True"
showN (CypherPatternCond (GraphPattern [])) _ = "@Graph"
showN (CypherPatternCond (GraphPattern as)) n = "(" ++ intercalate " AND " (map show (as)) ++ ")"
showN (CypherAndCond a b) n = "(" ++ showN a (n-1) ++ " AND " ++ showN b (n-1) ++ ")" -}
instance Serialize CypherCond where
    serialize (CypherCompCond op lhs rhs s) = case s of
        Pos -> serialize lhs ++ " " ++ op ++ " " ++ serialize rhs
        Neg -> "NOT (" ++ serialize lhs ++ " " ++ op ++ " " ++ serialize rhs ++ ")"
    serialize (CypherTrueCond) = "@True"
    serialize (CypherAndCond a b) = "(" ++ serialize a ++ " AND " ++ serialize b ++ ")"
    serialize (CypherPatternCond (GraphPattern [])) = "@Graph"
    serialize (CypherPatternCond (GraphPattern as)) = "(" ++ intercalate " AND " (map serialize (as)) ++ ")"

instance Serialize CypherValue where
    serialize (CypherIntValue i) = show i
    serialize (CypherStringValue s)=s
    serialize (CypherNullValue) = "NULL"

instance Serialize Cypher where
    serialize (Cypher vars (GraphPattern patterns) conds sets (GraphPattern creates) deletes) =
            unwords (filter (not . null) [
                case patterns of
                    []-> ""
                    _ -> "MATCH " ++ intercalate "," (map serialize patterns),
                case conds of
                    CypherTrueCond -> ""
                    _ -> "WHERE " ++ serialize conds,
                case sets of
                    [] -> ""
                    _ -> "SET " ++ intercalate "," (map (\(a, b)-> serialize a ++ " = " ++ serialize b) sets),
                case creates of
                    [] -> ""
                    _ -> "CREATE " ++ intercalate "," (map serialize creates),
                case deletes of
                    [] -> ""
                    _ -> "DELETE " ++ intercalate "," (map serialize deletes),
                case vars of
                    [] -> ""
                    _ -> "WITH " ++ intercalate "," (map (\(expr, var) -> serialize expr ++ " AS " ++ serialize var) vars)])

instance Serialize CypherQuery where
    serialize (CypherQuery rvars cyphers params) =
        (case params of
            [] -> ""
            _ -> "WITH " ++ intercalate "," (map (\x -> "{"++serialize x++"} AS " ++ serialize x) params) ++ " "
        ) ++ unwords (map serialize cyphers) ++ " " ++ case rvars of
            [] -> "RETURN 1"
            _ -> "RETURN " ++ intercalate "," (map serialize rvars)


-- auxiliary functions

-- cor :: Cypher -> Cypher -> Cypher
-- cor (Cypher r1 m1 w1 s1 c1 d1) (Cypher r2 m2 w2 s2 c2 d2) = Cypher (r1 ++ r2) (m1 <> m2) (w1 .||. w2) (s1 ++ s2) (c1 <> c2) (d1 ++ d2)

-- false :: Cypher
-- false = Cypher [] mempty CypherFalseCond [] mempty []

instance Monoid Cypher where
    mappend (Cypher r1 m1 w1 s1 c1 d1) (Cypher r2 m2 w2 s2 c2 d2) = Cypher (r1 ++ r2) (m1 <> m2) (w1 .&&. w2) (s1 ++ s2) (c1 <> c2) (d1 ++ d2)
    mempty = Cypher [] mempty CypherTrueCond [] mempty []

cmatch :: GraphPattern -> Cypher
cmatch c = Cypher [] c CypherTrueCond [] mempty []

cwhere :: CypherCond -> Cypher
cwhere c = Cypher [] mempty c [] mempty []

creturn :: [(CypherExpr, CypherVar)] -> Cypher
creturn rs = Cypher rs mempty CypherTrueCond [] mempty []

set :: [(CypherExpr, CypherExpr)] -> Cypher
set ss = Cypher [] mempty CypherTrueCond ss mempty []

create :: [OneGraphPattern] -> Cypher
create cs = Cypher [] mempty CypherTrueCond [] (GraphPattern cs) mempty

delete :: [CypherVar] -> Cypher
delete ds = Cypher [] mempty CypherTrueCond [] mempty ds

-- fv

class FV a where
    fv :: a -> [CypherVar]

instance FV CypherVar where
    fv var = [var]

instance FV OneGraphPattern where
    fv (GraphEdgePattern nodepatternsrc label nodepatterntgt) =
        fv nodepatternsrc `union` fv label `union` fv nodepatterntgt
    fv (GraphNodePattern nodepattern) = fv nodepattern

instance FV GraphPattern where
    fv (GraphPattern list)= foldl f [] (list) where
        f vars pattern = vars `union` fv pattern

instance FV NodePattern where
    fv (NodePattern var label props) = (case var of
        Just var -> [var]
        Nothing -> []) `union` fv props

instance FV CypherCond where
    fv (CypherCompCond op a b _) = fv a `union` fv b
    fv (CypherAndCond a b) = (fv a) `union` (fv b)
    fv (CypherTrueCond) = []
    fv (CypherPatternCond a) = fv a

instance FV CypherExpr where
    fv (CypherVarExpr var) = [var]
    fv (CypherDotExpr expr _) = fv expr
    fv (CypherAppExpr _ args) = foldl union [] (map fv args)
    fv _ = []

instance FV a => FV [a] where
   fv vars = unions (map fv vars)

instance FV Cypher where
   fv (Cypher r m w s c d) = fv m `union` fv w `union` fv s `union` fv c `union` fv d

instance FV PropertyKey where
   fv _ = []

instance (FV a, FV b) => FV (a, b) where
  fv (a, b) = fv a `union` fv b



-- subst
newtype CypherVarExprMap = CypherVarExprMap (Map CypherVar CypherExpr) deriving (Eq, Show)



instance Monoid CypherVarExprMap where
   mempty = CypherVarExprMap empty
   m1 `mappend` m2 = -- assume that
                     -- fv of m2's codomain is disjoint from m1's domain
                     -- m2's domain is disjoint from m1's domain
       let (CypherVarExprMap m1') = subst m2 m1
           (CypherVarExprMap m2') = m2 in
           CypherVarExprMap (m1' `Map.union` m2')

cypherVarExprMap :: CypherVar -> CypherExpr -> CypherVarExprMap
cypherVarExprMap k v = CypherVarExprMap (insert k v empty)


class Subst a where
   subst :: CypherVarExprMap -> a -> a

instance Subst OneGraphPattern where
   subst varmap (GraphEdgePattern nodepatternsrc label nodepatterntgt) =
       GraphEdgePattern (subst varmap nodepatternsrc) (subst varmap label) (subst varmap nodepatterntgt)
   subst varmap (GraphNodePattern nodepattern) =
       GraphNodePattern (subst varmap nodepattern)

instance Subst NodePattern where
    subst varmap (NodePattern var label props) =
        NodePattern (fmap (extractVarFromExpr . subst varmap . CypherVarExpr) var) label (subst varmap props)

instance Subst GraphPattern where
    subst varmap (GraphPattern list) = GraphPattern (map (subst varmap) list)

instance Subst CypherCond where
    subst varmap (CypherCompCond op a b sign) = CypherCompCond op (subst varmap a) (subst varmap b) sign
    subst varmap (CypherAndCond a b) = CypherAndCond (subst varmap a) (subst varmap b)
    subst _ CypherTrueCond = CypherTrueCond
    subst varmap (CypherPatternCond a) = CypherPatternCond (subst varmap a)

instance Subst CypherExpr where
    subst (CypherVarExprMap varmap) ve@(CypherVarExpr var) = fromMaybe ve (lookup var varmap)
    subst varmap (CypherDotExpr expr prop) = CypherDotExpr (subst varmap expr) prop
    subst varmap (CypherAppExpr f args) = CypherAppExpr f (map (subst varmap) args)
    subst _ a = a

instance Subst CypherVarExprMap where
    subst varmap (CypherVarExprMap m)= CypherVarExprMap (Map.map (subst varmap) m)

instance Subst Cypher where
    subst varmap (Cypher r m w s c d) = Cypher (subst varmap r) (subst varmap m) (subst varmap w) (subst varmap s) (subst varmap c) (map (substVar varmap) d)
      where substVar varmap = extractVarFromExpr . subst varmap . CypherVarExpr

instance Subst PropertyKey where
    subst _ a = a

instance Subst a => Subst (Maybe a) where
    subst varmap = fmap (subst varmap)

instance (Subst a , Subst b) => Subst (a, b) where
    subst varmap = subst varmap *** subst varmap

instance Subst CypherVar where
    subst _ = id

instance Subst a => Subst [a] where
    subst varmap = map (subst varmap)

instance Subst Char where
    subst _ = id


-- translation

data CypherQuery = CypherQuery [Var] [Cypher] [Var] deriving (Show, Read)

-- mapping from predicate to a cypher query
type CypherMapping = ([CypherVar], GraphPattern, GraphPattern, Dependencies)

type Dependencies = [(CypherVar, [CypherVar])]

-- translate relational calculus to cypher
-- If P maps to graph pattern p
-- {xs : P(e_1,e_2,...,e_n)}
-- translates to MATCH p WHERE ...
-- if x_i = e_j then "col_j", if there are multiple j's, choose any one
-- if e_i is a const, then "P.col_i = e_i"
-- if e_i is a variable, and e_j = e_i then "P.col_j = P.col_i"
-- otherwise True
-- Var -> CypherExpr
type CypherVarMap = Map Var CypherExpr

-- predicate -> pattern (query, insert, update, delete)
type CypherPredTableMap = Map Pred CypherMapping
-- builtin predicate -> op, neg op
newtype CypherBuiltIn = CypherBuiltIn (Map Pred (Sign -> [CypherExpr] -> TransMonad Cypher))
type TransMonad a = StateT (CypherBuiltIn, CypherPredTableMap) NewEnv a

extractPropertyVarInMatch :: [CypherVar] -> Cypher -> Cypher
extractPropertyVarInMatch env (Cypher r as w s c d) =
    let (w', as', vmap) = extractPropertyVarInMatch2 env as mempty in
        subst vmap (Cypher r as' (w .&&. w') s c d)

extractPropertyVarInMatch2 :: [CypherVar] -> GraphPattern -> CypherVarExprMap -> (CypherCond, GraphPattern, CypherVarExprMap)
extractPropertyVarInMatch2 env as vmap =
    let onegraphs = splitGraphPattern as in
        foldr (\(GraphPattern [p]) (cond', pat', vmap') -> case p of
                    GraphNodePattern n ->
                        let (cond'', n', vmap'') = extractPropertyVarInNodePattern env n vmap' in
                            (cond' .&&. cond'', pat' <> GraphPattern [ (GraphNodePattern n')], vmap'')
                    GraphEdgePattern n1 n2 n3 ->
                        let (cond1'', n1', vmap1'') = extractPropertyVarInNodePattern env n1 vmap' in
                        let (cond2'', n2', vmap2'') = extractPropertyVarInNodePattern env n2 vmap1'' in
                        let (cond3'', n3', vmap3'') = extractPropertyVarInNodePattern env n3 vmap2'' in
                            (cond' .&&. cond1'' .&&. cond2'' .&&. cond3'', pat' <> GraphPattern [ (GraphEdgePattern n1' n2' n3')], vmap3'')
        ) (CypherTrueCond, mempty, vmap) onegraphs

extractPropertyVarInNodePattern :: [CypherVar] -> NodePattern -> CypherVarExprMap -> (CypherCond, NodePattern, CypherVarExprMap)
extractPropertyVarInNodePattern env n@(NodePattern v l p) (CypherVarExprMap vmap) =
    case v of
        Nothing -> error ("extractPropertyVarInNodePattern: node with no variable " ++ serialize n)
        Just var ->
            let (cond', props', vmap') = foldl (\(cond', props, vmap') (prop, expr) ->
                    case expr of
                        CypherIntConstExpr _ -> (cond', props ++ [(prop, expr)], vmap')
                        CypherStringConstExpr _ -> (cond', props ++ [(prop, expr)], vmap')
                        CypherVarExpr v -> case lookup v vmap' of
                            Nothing -> if v `elem` env
                                then (cond' .&&. (CypherDotExpr (CypherVarExpr var) prop .=. expr), props, vmap')
                                else (cond', props, insert v (CypherDotExpr (CypherVarExpr var) prop) vmap')
                            Just expr2 -> (cond' .&&. (CypherDotExpr (CypherVarExpr var) prop .=. expr2), props, vmap')
                        _ -> (cond' .&&. (CypherDotExpr (CypherVarExpr var) prop .=. expr), props, vmap')
                        ) (CypherTrueCond, [], vmap) p in
                (cond', NodePattern v l props', CypherVarExprMap vmap')

translateQueryToCypher :: [Var] -> Query -> [Var] -> TransMonad CypherQuery
translateQueryToCypher vars (Query  conj) env = do
    lift $ registerVars env
    cypher <- translateFormulaToCypher vars conj env
    return (CypherQuery vars cypher env)

cypherDeterminedVars :: CypherBuiltIn -> Formula -> [Var]
cypherDeterminedVars (CypherBuiltIn builtin) (FAtomic a@(Atom name _)) =
    if name `member` builtin
        then []
        else toAscList (freeVars a)
cypherDeterminedVars builtin (FSequencing form1 form2) =
    cypherDeterminedVars builtin form1 `union` cypherDeterminedVars builtin form2
cypherDeterminedVars builtin (FChoice form1 form2) =
    cypherDeterminedVars builtin form1 `intersect` cypherDeterminedVars builtin form2
cypherDeterminedVars builtin FTransaction = []
cypherDeterminedVars builtin FOne = []
cypherDeterminedVars builtin FZero = []
cypherDeterminedVars builtin (FInsert _) = []
cypherDeterminedVars builtin _ = error "unsupported"

translateFormulaToCypher :: [Var] -> Formula -> [Var] -> TransMonad [Cypher]
translateFormulaToCypher rvars (FSequencing form1 form2) env = do
    (builtin , _) <- get
    let determinedvars = cypherDeterminedVars builtin form1
    let rvars1 = (determinedvars `union` env) `intersect` (toAscList (freeVars form2) `union` rvars)
    cypher1 <- translateFormulaToCypher rvars1 form1 env
    cypher2 <- translateFormulaToCypher rvars form2 rvars1
    return (cypher1 ++ cypher2)
translateFormulaToCypher rvars (FChoice disj1 disj2) env =
    error "translateFormulaToCypher: unsupported"
translateFormulaToCypher rvars FOne env = return []
translateFormulaToCypher rvars FZero env =
    error "translateFormulaToCypher: unsupported"
translateFormulaToCypher rvars (FAtomic a) env = do
    cypher <- translateQueryAtomToCypher rvars Pos a env
    return [cypher]
translateFormulaToCypher rvars (FInsert lits@(Lit sign0 a)) env = do
    cypher <- case sign0 of
        Pos -> translateInsertAtomToCypher rvars a env
        Neg -> translateDeleteAtomToCypher rvars a env
    return [cypher]
translateFormulaToCypher rvars (Aggregate Not (FAtomic a)) env = do
    cypher <- translateQueryAtomToCypher rvars Neg a env
    return [cypher]

translateFormulaToCypher _ _ _ = do
    error "translateFormulaToCypher: unsupported"

-- instantiate a cypher mapping
instantiate :: [CypherVar] -> GraphPattern -> GraphPattern -> [CypherExpr]-> TransMonad (GraphPattern, GraphPattern)
instantiate vars matchpattern pattern args = do
    let allvars = fv matchpattern `union` fv pattern
    let internalvars = allvars \\ vars
    newvars <- lift $ new (map CypherVarExpr internalvars)
    let varmap = mconcat (zipWith cypherVarExprMap internalvars (map CypherVarExpr newvars)) <> mconcat (zipWith cypherVarExprMap vars args)
    return (subst varmap matchpattern, subst varmap pattern)
-- translate atom to cypher in a query
translateQueryAtomToCypher :: [Var] -> Sign -> Atom -> [Var] -> TransMonad Cypher
translateQueryAtomToCypher rvars thesign atom env = do
    (CypherBuiltIn builtin, predtablemap) <- get
    let (Atom pred1 args) = atom
    let exprs = convert args
    --try builtin first
    case lookup pred1 builtin of
        Just builtinpred -> do
            builtinpred thesign exprs
        Nothing -> case thesign of
            Pos -> case lookup pred1 predtablemap of
                Just (vars, matchpattern, pattern, _) -> do
                    (matchpattern, pattern) <- instantiate vars matchpattern pattern exprs
                    return (extractPropertyVarInMatch (map (\(Var var)->CypherVar var) env) (cmatch (matchpattern <> pattern) <> creturn (map (\(Var var) -> (CypherVarExpr (CypherVar var), CypherVar var)) rvars)))
                Nothing -> error (show pred1 ++ " is not defined")
            Neg -> error ("negative literal is not supported")

translateDeleteAtomToCypher :: [Var] -> Atom -> [Var] -> TransMonad Cypher
translateDeleteAtomToCypher rvars atom env = do
    (CypherBuiltIn builtin, predtablemap) <- get
    let (Atom pred1 args) = atom
    case lookup pred1 predtablemap of
        Just (vars, matchpattern, pattern, _) -> do
            (matchpattern, pattern) <- instantiate vars matchpattern pattern (convert args)
            -- generate a list of vars of match pattern and pattern, the vars that are in pattern but not match pattern needs to be deleted
            -- if the var is prop var then it is set to NULL, if it is a node var then it is deleted
            let mfvt = fvType Root matchpattern
            let fvt = fvType Root pattern
            let mfvt1 = map (\(x,_,_,_) -> x) mfvt
            let (del, setnull) = partition (\(v, c, p, t) -> t == CypherNodeVar) (filter (\(x,_,_,_) -> not (elem x mfvt1)) fvt)
            return (mergeDeleteAndSetNull (extractPropertyVarInMatch (map (\(Var var)->CypherVar var) env) (cmatch (matchpattern <> pattern) <> delete (nub (map (\(a,_,_,_)->a) del))
                 <> set (map (\(_, Dot v prop, _, var) -> (CypherDotExpr (CypherVarExpr v) prop, CypherNullExpr)) setnull)
                 <> creturn (map (\(Var var )-> (CypherVarExpr (CypherVar var), CypherVar var)) rvars))))
        Nothing -> error (show pred1 ++ " is not defined")

translateInsertAtomToCypher :: [Var] -> Atom -> [Var] -> TransMonad Cypher
translateInsertAtomToCypher rvars x@(Atom pred1 args) env = do
    (CypherBuiltIn builtin, predtablemap) <- get
    case lookup pred1 predtablemap of
        Just (vars, matchpattern, pattern, _) -> do
            (matchpattern, pattern) <- instantiate vars matchpattern pattern (convert args)
            -- generate a list of vars of match pattern and pattern, the vars that are in pattern but not match pattern needs to be inserted
            -- if the var is prop var then it is set to a new value, if it is a node var then it is created
            let mfvt = fvType Root matchpattern
            let fvt = fvType Root pattern
            let (cre, setexpr) = partition (\(v, c, p, t) ->  t == CypherNodeVar) (fvt \\ mfvt)
            return (mergeCreateAndSet (extractPropertyVarInMatch (map (\(Var var)->CypherVar var) env) (cmatch matchpattern <> create (map (\(var, p, nonvarprops, t) -> case p of
                      Edge srcv tgtv l -> edgevlp' (nodev' srcv) var l nonvarprops (nodev' tgtv)
                      Node l -> nodevlp' var l nonvarprops
                      ) cre)
                 <> set (map (\(var, Dot v prop, _, _) -> (CypherDotExpr (CypherVarExpr v) prop, CypherVarExpr var)) setexpr)
                 <> creturn (map (\(Var var) -> (CypherVarExpr (CypherVar var), CypherVar var)) rvars))))
        Nothing -> error (show pred1 ++ " is not defined")

data CypherTrans = CypherTrans CypherBuiltIn [String] CypherPredTableMap

nodevl v l = GraphNodePattern (NodePattern (Just (CypherVar v)) (Just l) [])
nodelp l ps = GraphNodePattern (NodePattern Nothing (Just l) ps)
nodep ps = GraphNodePattern (NodePattern Nothing Nothing ps)
nodel l = GraphNodePattern (NodePattern Nothing (Just l) [])
nodevp v ps = GraphNodePattern (NodePattern (Just (CypherVar v)) Nothing ps)

nodev' v =  (NodePattern (Just v) Nothing [])
nodev v =  (NodePattern (Just (CypherVar v)) Nothing [])

nodevlp' v l ps = GraphNodePattern (NodePattern (Just v) l ps)
nodevlp v l ps = GraphNodePattern (NodePattern (Just (CypherVar v)) (Just l) ps)


edgel n1 l n2 = GraphEdgePattern n1 (NodePattern Nothing (Just l) []) n2
edgevl n1 v l n2 = GraphEdgePattern n1 (NodePattern (Just (CypherVar v)) (Just l) []) n2
edgevlp' n1 v l ps n2 = GraphEdgePattern n1 (NodePattern (Just v) l ps) n2
edgevlp n1 v l ps n2 = GraphEdgePattern n1 (NodePattern (Just (CypherVar v)) (Just l) ps) n2
dot v l = CypherDotExpr (CypherVarExpr (CypherVar v)) (PropertyKey l)
dot' v l = CypherDotExpr (CypherVarExpr v) l
app l e = CypherAppExpr l [CypherVarExpr e]
var v = CypherVarExpr (CypherVar v)
cnull = CypherNullExpr
match m = Cypher [] (GraphPattern m) CypherTrueCond [] mempty []

type CypherEnv = Map CypherVar CypherExpr

instance Convertible Var CypherVar where
    safeConvert = Right . CypherVar . unVar
instance Convertible MapResultRow CypherEnv where
    safeConvert = Right . foldlWithKey (\map k v  -> insert (convert k) (convert v) map) empty

instance Convertible ResultValue CypherExpr where
    safeConvert (IntValue i) = Right (CypherIntConstExpr i)
    safeConvert (StringValue i) = Right (CypherStringConstExpr i)

instance Convertible Expr CypherExpr where
    safeConvert (IntExpr i) = Right (CypherIntConstExpr i)
    safeConvert (StringExpr i) = Right (CypherStringConstExpr i)
    safeConvert (VarExpr (Var i)) = Right (CypherVarExpr (CypherVar i))

instance Convertible [Expr] [CypherExpr] where
        safeConvert  = mapM safeConvert

data CypherState = CypherState {
    csUpdate  :: Bool,
    csQuery :: Bool,
    csReturn :: [Var]
}

translateableCypher :: CypherTrans -> Formula -> StateT CypherState Maybe ()
translateableCypher trans (FSequencing form1 form2) = do
    translateableCypher trans form1
    translateableCypher trans form2
translateableCypher trans (FChoice _ _) = lift $ Nothing
translateableCypher _ (FAtomic _) = do
    cs <- get
    if csUpdate cs
        then lift $ Nothing
        else do
            put cs{csQuery = True}
            return ()
translateableCypher _ (FInsert lit) = do
    cs <- get
    if csQuery cs && not (null (csReturn cs `intersect` toAscList (freeVars lit)))
        then lift $ Nothing
        else do
            put cs{csUpdate = True}
            return ()
translateableCypher trans (FTransaction) = lift $ Nothing
translateableCypher trans (FOne) = return ()
translateableCypher trans (FZero) = lift $ Nothing
translateableCypher trans (Aggregate _ _)  = lift $ Nothing

instance IGenericDatabase01 CypherTrans where
    type GDBQueryType CypherTrans = CypherQuery
    gDeterminateVars trans varDomainSize (Atom name args) =
        (if isBuiltIn
            then bottom
            else if name `member` predtablemap
                then Set.unions (map (\arg -> case arg of
                    (VarExpr v) -> Set.singleton v
                    _ -> bottom) args)
                else bottom) where
                    isBuiltIn = name `member` builtin
                    (CypherTrans (CypherBuiltIn builtin) _ predtablemap) = trans

    gTranslateQuery trans vars query env =
        let (CypherTrans builtin _ predtablemap) = trans in
            runNew (evalStateT (translateQueryToCypher (toAscList vars) query (toAscList env)) (builtin, predtablemap))
    gSupported trans form vars = layeredF form && isJust (evalStateT (translateableCypher trans form ) (CypherState False False (toAscList vars)))

instance New CypherVar CypherExpr where
    new _ = CypherVar <$> new (StringWrapper "var")

insertMaybe :: Ord a => Maybe a -> b -> Map a b -> Map a b
insertMaybe l0 v m =
    case l0 of
        Just l -> insert l v m
        Nothing -> m

createsToMap :: GraphPattern -> (Map CypherVar OneGraphPattern, Map CypherVar CypherVar, Map CypherVar CypherVar)
createsToMap (GraphPattern cres ) = foldl oneCreateToMap (empty, empty, empty) (cres) where
    oneCreateToMap (map1, map2, map3) p@(GraphNodePattern (NodePattern (Just v) _ _)) = (insert v p map1, map2, map3)
    oneCreateToMap (map1, map2, map3) p@(GraphEdgePattern ( (NodePattern l1 _ _)) (NodePattern (Just v) _ _ ) ( (NodePattern l2 _ _))) =
        (insert v p map1, insertMaybe l1 v map2, insertMaybe l2 v map3)
    oneCreateToMap map1 _ = map1

mergeCreateAndSet :: Cypher -> Cypher
mergeCreateAndSet (Cypher r m w s c d) =
    let (c', s') = mergeCreateAndSet2 c s in
        Cypher r m w s' c' d

mergeCreateAndSet2 :: GraphPattern -> [(CypherExpr, CypherExpr)] -> (GraphPattern, [(CypherExpr, CypherExpr)])
mergeCreateAndSet2 cres sets  =
  let (map1, map2, map3) = createsToMap cres
      (sets', (map1', _, _)) = runState (doMergeCreateAndSet2 sets) (map1, map2, map3) in
      (GraphPattern (elems map1'), sets')

doMergeCreateAndSet2 :: [(CypherExpr, CypherExpr)] -> State (Map CypherVar OneGraphPattern, Map CypherVar CypherVar, Map CypherVar CypherVar) [(CypherExpr, CypherExpr)]
doMergeCreateAndSet2 = foldM doMergeCreateAndOneSet [] where
    doMergeCreateAndOneSet sets set@(CypherDotExpr (CypherVarExpr v) p, r) = do
        (map1, map2, map3) <- get
        case lookup v map1 of
            Nothing ->
                case lookup v map2 of
                    Just v1 -> do
                        case lookup v1 map1 of
                            Just n -> do
                                let n' = addPropToPattern2 n p r
                                put (insert v n' map1, map2, map3)
                                return sets
                            Nothing -> case lookup v map2 of
                                Just v1 -> do
                                    case lookup v1 map1 of
                                        Just n -> do
                                            let n' = addPropToPattern3 n p r
                                            put (insert v n' map1, map2, map3)
                                            return sets
                                        Nothing -> error ("mergeCreateAndSet: node to edge map is incorrectly constructed: " ++ serialize v ++ " maps to " ++ serialize v1 ++ " but edge is not in map")
                    Nothing -> return (sets ++ [set])
            Just n -> do
                let n' = addPropToPattern n p r
                put (insert v n' map1, map2, map3)
                return sets
    addPropToPattern (GraphNodePattern (NodePattern v l ps)) p r = GraphNodePattern (NodePattern v l (ps `union` [(p, r)]))
    addPropToPattern (GraphEdgePattern n1 (NodePattern v l ps) n2) p r = GraphEdgePattern n1 (NodePattern v l (ps `union` [(p,r)])) n2
    addPropToPattern2 (GraphEdgePattern ( (NodePattern v l ps)) n1  n2) p r = GraphEdgePattern ( (NodePattern v l (ps `union` [(p,r)]))) n1  n2
    addPropToPattern3 (GraphEdgePattern n1  n2 ( (NodePattern v l ps))) p r = GraphEdgePattern n1  n2 ( (NodePattern v l (ps `union` [(p,r)])))

mergeDeleteAndSetNull :: Cypher->Cypher
mergeDeleteAndSetNull (Cypher r m w s c d) = Cypher r m w (mergeDeleteAndSetNull2 d s) c d

mergeDeleteAndSetNull2 :: [CypherVar] ->[(CypherExpr, CypherExpr)] ->[(CypherExpr, CypherExpr)]
mergeDeleteAndSetNull2 vars = filter (\set-> case set of
    (CypherDotExpr (CypherVarExpr v) _,CypherNullExpr) -> v `notElem` vars
    _ -> True)
