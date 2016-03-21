{-# LANGUAGE FlexibleInstances, MultiParamTypeClasses, FlexibleContexts, RankNTypes, GADTs, UndecidableInstances #-}
module Cypher.Cypher (CypherVar(..), CypherOper, CypherExpr(..), Label,
    CypherMapping, translateQueryToCypher, simplifyDependencies', unifyAll, unifysCond, unifyOne, normalizeGraphPatternCond,
    CypherCond(..), CypherQuery, GraphPattern(..), NodePattern(..), PropertyKey(..), Cypher(..), cypherVarExprMap, CypherVarExprMap(..),
    CypherBuiltIn(..), CypherPredTableMap, OneGraphPattern(..),CypherTrans(..), (.=.), (.<>.), (.&&.), (.||.),
    nodev, nodel, nodevl, nodevp, nodevlp, nodelp, nodep, edgel, edgevl, var, cnull, dot, app, match,
    create, set, delete, cnot, cwhere, creturn, cor, cfalse) where

import FO.Data hiding (getConjuncts, getDisjuncts, Subst, subst, instantiate, conj, disj, Unify,unify)
import qualified FO.Data as FO
import FO.Domain
import DBQuery
import QueryPlan

import Prelude hiding (lookup)
import Data.List (intercalate, (\\), union , partition)
import Control.Monad.Trans.State.Strict (State, StateT, get, put, evalState, runState, evalStateT, runStateT)
import Control.Monad (foldM, guard)
import Control.Arrow ((***))
import Data.Map.Strict (empty, Map, insert, member, foldlWithKey, lookup, fromList, toList, elems, update, keys)
import qualified Data.Map.Strict as Map
import Data.Convertible.Base
import Data.Monoid
import Data.Maybe
import Data.Set ()
import qualified Data.Set as Set
import Control.Applicative ((<$>), liftA2)
import Control.Monad.Trans.Class
import Debug.Trace

-- basic definitions

newtype CypherVar = CypherVar {unCypherVar::String} deriving (Eq, Ord)
data CypherExpr = CypherVarExpr CypherVar
                | CypherRigidVarExpr CypherVar -- used only in unification
                | CypherIntConstExpr Int
                | CypherStringConstExpr String
                | CypherParamExpr String
                | CypherDotExpr CypherExpr PropertyKey
                | CypherAppExpr String [CypherExpr]
                | CypherNullExpr deriving (Eq, Ord)

extractVarFromExpr :: CypherExpr -> CypherVar
extractVarFromExpr (CypherVarExpr var) = var
extractVarFromExpr _ = error "this is not a var expr"

isCypherConstExpr :: CypherExpr -> Bool
isCypherConstExpr e = case e of CypherIntConstExpr _ -> True; CypherStringConstExpr _ -> True ; _ -> False

type CypherOper = String

data CypherValue = CypherIntValue Int
                 | CypherStringValue String
                 | CypherNullValue

data CypherCond = CypherCompCond CypherOper CypherExpr CypherExpr
                | CypherAndCond CypherCond CypherCond
                | CypherOrCond CypherCond CypherCond
                | CypherTrueCond
                | CypherFalseCond
                | CypherNotCond CypherCond
                | CypherExistsCond CypherCond
                | CypherNotExistsCond CypherCond
                | CypherPatternCond GraphPattern deriving Eq



getConjuncts :: CypherCond -> [CypherCond]
getConjuncts (CypherAndCond c1 c2) = getConjuncts c1 ++ getConjuncts c2
getConjuncts c = [c]

getDisjuncts :: CypherCond -> [CypherCond]
getDisjuncts (CypherOrCond c1 c2) = getDisjuncts c1 ++ getDisjuncts c2
getDisjuncts c = [c]

conj :: [CypherCond] -> CypherCond
conj = foldl (.&&.) CypherTrueCond

disj :: [CypherCond] -> CypherCond
disj = foldl (.||.) CypherFalseCond

(.&&.) :: CypherCond -> CypherCond -> CypherCond
CypherTrueCond .&&. b = b
CypherFalseCond .&&. b = CypherFalseCond
a .&&. CypherTrueCond = a
a .&&. CypherFalseCond = CypherFalseCond
a .&&. b = CypherAndCond a b

(.||.) :: CypherCond -> CypherCond -> CypherCond
CypherFalseCond .||. b = b
CypherTrueCond .||. b = CypherTrueCond
a .||. CypherFalseCond = a
a .||. CypherTrueCond = CypherTrueCond
a .||. b = CypherOrCond a b

(.=.) :: CypherExpr -> CypherExpr -> CypherCond
a .=. b = CypherCompCond "=" a b

(.<>.) :: CypherExpr -> CypherExpr -> CypherCond
a .<>. b = CypherCompCond "<>" a b

data Cypher = Cypher {cypherReturn :: [ CypherExpr ], cypherMatch :: GraphPattern, cypherWhere :: CypherCond, cypherSet :: [(CypherExpr, CypherExpr)], cypherCreate :: GraphPattern, cypherDelete :: [CypherVar] } deriving Eq

instance Monoid Cypher where
    mappend (Cypher r1 m1 w1 s1 c1 d1) (Cypher r2 m2 w2 s2 c2 d2) = Cypher (r1 ++ r2) (m1 <> m2) (w1 .&&. w2) (s1 ++ s2) (c1 <> c2) (d1 ++ d2)
    mempty = Cypher [] mempty CypherTrueCond [] mempty []

-- graph patterns

type Label = String
newtype PropertyKey = PropertyKey String deriving (Eq, Ord)
type Properties = [(PropertyKey, CypherExpr)]
data NodePattern = NodePattern (Maybe CypherVar) (Maybe Label) Properties deriving Eq
-- mergable with create, not mergable with create
data GraphPattern = GraphPattern [OneGraphPattern] [OneGraphPattern] deriving (Show, Eq)
data OneGraphPattern = GraphEdgePattern OneGraphPattern NodePattern OneGraphPattern
                     | GraphNodePattern NodePattern deriving Eq


instance Monoid GraphPattern where
    mempty = GraphPattern [] []
    GraphPattern as as2 `mappend` GraphPattern bs bs2 = GraphPattern (mergeOneGraphPatterns (as ++ bs)) (mergeOneGraphPatterns (as2 ++ bs2))

data CypherVarType = CypherNodeVar | CypherPropertyVar deriving Eq

data Context = Root | Node Label | Edge CypherVar CypherVar Label | Dot CypherVar PropertyKey deriving Eq

class FVType a where
    fvType :: Context -> a -> [(CypherVar, Context, Properties, CypherVarType)]

instance FVType OneGraphPattern where
    fvType _ (GraphEdgePattern nodepatternsrc label nodepatterntgt) =
        fvType Root nodepatternsrc `union` fvType (Edge (nodeVar2 nodepatternsrc) (nodeVar2 nodepatterntgt) "") label `union` fvType Root nodepatterntgt
    fvType _ (GraphNodePattern nodepattern) = fvType (Node "") nodepattern

instance FVType GraphPattern where
    fvType _ (GraphPattern list list2)= unions (map (fvType Root) (list ++ list2))

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
addLabel l (Edge v v2 _) = Edge v v2 l
addLabel l (Node _) = Node l

nodeVar :: NodePattern -> CypherVar
nodeVar (NodePattern var label props) =
    fromMaybe (error "fvtype without var") var

nodeVar2 :: OneGraphPattern -> CypherVar
nodeVar2 (GraphNodePattern n) =
    nodeVar n

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
mergeTwoOneGraphPatterns (GraphNodePattern n) (GraphNodePattern n2) = do
    guard (sameNode n n2)
    return (GraphNodePattern (mergeNodes n n2))
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
splitGraphPattern (GraphPattern ps ps2) = map (\p -> GraphPattern [p] []) ps ++ map (\p -> GraphPattern [] [p]) ps2

createsToMap :: GraphPattern -> Map CypherVar OneGraphPattern
createsToMap (GraphPattern cres cres2) = foldl oneCreateToMap empty (cres ++ cres2) where
    oneCreateToMap map1 p@(GraphNodePattern (NodePattern (Just v) _ _)) = insert v p map1
    oneCreateToMap map1 p@(GraphEdgePattern _ (NodePattern (Just v) _ _ ) _) = insert v p map1
    oneCreateToMap map1 _ = map1
-- show
instance Show OneGraphPattern where
    show (GraphNodePattern nodepattern) = show nodepattern
    show (GraphEdgePattern nodepatternsrc label nodepatterntgt) =
        show nodepatternsrc ++ "-[" ++ trimBrackest (show label) ++ "]->" ++ show nodepatterntgt
        where
            trimBrackest ('(' : t) = take (length t - 1) t
            trimBrackest t = t

instance Show NodePattern where
    show (NodePattern var label props) =
        "(" ++ (case var of
            Just v -> show v
            Nothing -> "") ++ (case label of
            Just l -> ":" ++ l
            Nothing -> "") ++ (if not (null props)
            then "{" ++ intercalate "," (f props) ++ "}"
            else "") ++ ")" where
        f = map (\(PropertyKey key, value) -> key ++ ":" ++ show value)

instance Show CypherExpr where
    show (CypherVarExpr var) = show var
    show (CypherIntConstExpr i) = show i
    show (CypherStringConstExpr s) = "'" ++ cypherStringEscape s ++ "'"
    show (CypherParamExpr m) = "{" ++ m ++ "}"
    show (CypherDotExpr expr (PropertyKey prop)) = show expr ++ "." ++ prop
    show (CypherAppExpr f args) = f ++ "(" ++ intercalate "," (map show args) ++ ")"
    show (CypherNullExpr) = "NULL"

instance Show CypherVar where
    show (CypherVar var) = var

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

showN _ 0 = "..."
showN (CypherCompCond op lhs rhs) n = show lhs ++ " " ++ op ++ " " ++ show rhs
showN (CypherTrueCond) _ = "@True"
showN (CypherFalseCond) _ = "@False"
showN (CypherPatternCond (GraphPattern [] [])) _ = "@Graph"
showN (CypherAndCond a b) n = "(" ++ showN a (n-1) ++ " AND " ++ showN b (n-1) ++ ")"
showN (CypherOrCond a b) n = "(" ++ showN a (n-1) ++ " OR " ++ showN b (n-1) ++ ")"
showN (CypherPatternCond (GraphPattern as as2)) n = "(" ++ intercalate " AND " (map show (as ++ as2)) ++ ")"
showN (CypherNotCond a) n = "NOT (" ++ showN a (n-1) ++ ")"
showN (CypherExistsCond cypher) n = "(EXISTS (" ++ showN cypher (n-1) ++ "))"
instance Show CypherCond where
    show (CypherCompCond op lhs rhs) = show lhs ++ " " ++ op ++ " " ++ show rhs
    show (CypherTrueCond) = "@True"
    show (CypherFalseCond) = "@False"
    show (CypherPatternCond (GraphPattern [] [])) = "@Graph"
    show (CypherAndCond a b) = "(" ++ show a ++ " AND " ++ show b ++ ")"
    show (CypherOrCond a b) = "(" ++ show a ++ " OR " ++ show b ++ ")"
    show (CypherPatternCond (GraphPattern as as2)) = "(" ++ intercalate " AND " (map show (as ++ as2)) ++ ")"
    show (CypherNotCond a) = "NOT (" ++ show a ++ ")"
    show (CypherExistsCond cypher) = "(EXISTS (" ++ show cypher ++ "))"

instance Show CypherValue where
    show (CypherIntValue i) = show i
    show (CypherStringValue s)=s
    show (CypherNullValue) = "NULL"

instance Show Cypher where
    show (Cypher vars (GraphPattern patterns1 patterns2) conds sets (GraphPattern creates1 creates2) deletes) =
        let patterns = patterns1 ++ patterns2
            creates = creates1 ++ creates2 in
            unwords (filter (not . null) [
                case patterns of
                    []-> ""
                    _ -> "MATCH " ++ intercalate "," (map show patterns),
                case conds of
                    CypherTrueCond -> ""
                    _ -> "WHERE " ++ show conds,
                case sets of
                    [] -> ""
                    _ -> "SET " ++ intercalate "," (map (\(a, b)-> show a ++ " = " ++ show b) sets),
                case creates of
                    [] -> ""
                    _ -> "CREATE " ++ intercalate "," (map show creates),
                case deletes of
                    [] -> ""
                    _ -> "DELETE " ++ intercalate "," (map show deletes),
                case vars of
                    [] -> case (sets, creates, deletes) of
                            ([], [], []) -> "RETURN 1"
                            _ -> ""
                    _ -> "RETURN " ++ intercalate "," (map show vars)])
-- dependencies

data DependencyGraphNode = CypherExprNode {unCypherExprNode :: CypherExpr}
                        | ExprNode {unExprNode :: Expr} deriving (Eq, Ord)

data Relation = Equal DependencyGraphNode | Functional (Set.Set DependencyGraphNode)

type DependencyGraph = Map DependencyGraphNode Relation

getEqualRelation :: DependencyGraph -> Maybe (DependencyGraphNode, DependencyGraphNode)
getEqualRelation g = getEqualRelation' (toList g) where
   getEqualRelation' [] = Nothing
   getEqualRelation' ((n, Equal n2) : _) = Just (n, n2)
   getEqualRelation' (_ : s) = getEqualRelation' s

getFunctionalRelation :: DependencyGraph -> Maybe ([DependencyGraphNode], Set.Set DependencyGraphNode)
getFunctionalRelation g = getFunctionalRelation' (toList g) where
   getFunctionalRelation' [] = Nothing
   getFunctionalRelation' ((n, Functional s) : ns) = Just (getOtherFunctionalRelation [n] s ns)
   getFunctionalRelation' (_ : ns) = getFunctionalRelation' ns
   getOtherFunctionalRelation ns s [] = (ns, s)
   getOtherFunctionalRelation ns s ((n, Functional s2) : ns2) | s == s2 = getOtherFunctionalRelation (ns ++ [n]) s ns2
   getOtherFunctionalRelation ns s (_ : ns2) = getOtherFunctionalRelation ns s ns2


-- auxiliary functions

cor :: Cypher -> Cypher -> Cypher
cor (Cypher r1 m1 w1 s1 c1 d1) (Cypher r2 m2 w2 s2 c2 d2) = Cypher (r1 ++ r2) (m1 <> m2) (w1 .||. w2) (s1 ++ s2) (c1 <> c2) (d1 ++ d2)

false :: Cypher
false = Cypher [] mempty CypherFalseCond [] mempty []

matchToWhere :: GraphPattern -> CypherCond
matchToWhere = CypherPatternCond

-- cnot . cnot != id

cnot :: Cypher -> Cypher
cnot (Cypher r1 m1 w1 s1 c1 d1) = Cypher r1 mempty (CypherNotCond (w1 .&&. matchToWhere m1)) s1 c1 d1

cwhere :: CypherCond -> Cypher
cwhere c = Cypher [] mempty c [] mempty []

creturn :: [CypherExpr] -> Cypher
creturn rs = Cypher rs mempty CypherTrueCond [] mempty []

set :: [(CypherExpr, CypherExpr)] -> Cypher
set ss = Cypher [] mempty CypherTrueCond ss mempty []

create :: [OneGraphPattern] -> Cypher
create cs = Cypher [] mempty CypherTrueCond [] (GraphPattern cs []) mempty

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
    fv (GraphPattern list list2)= foldl f [] (list ++ list2) where
        f vars pattern = vars `union` fv pattern

instance FV NodePattern where
    fv (NodePattern var label props) = (case var of
        Just var -> [var]
        Nothing -> []) `union` fv props

instance FV CypherCond where
    fv (CypherCompCond op a b) = fv a `union` fv b
    fv (CypherAndCond a b) = (fv a) `union` (fv b)
    fv (CypherOrCond a b) = (fv a) `union` (fv b)
    fv (CypherTrueCond) = []
    fv (CypherFalseCond) = []
    fv (CypherNotCond a) = fv a
    fv (CypherPatternCond a) = fv a
    fv (CypherExistsCond a) = fv a
    fv cond = error ("unsupported CypherCond" ++ show cond)

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
    subst varmap (GraphPattern list list2) = GraphPattern (map (subst varmap) list) (map (subst varmap)  list2)

instance Subst CypherCond where
    subst varmap (CypherCompCond op a b) = CypherCompCond op (subst varmap a) (subst varmap b)
    subst varmap (CypherAndCond a b) = CypherAndCond (subst varmap a) (subst varmap b)
    subst varmap (CypherOrCond a b) = CypherOrCond (subst varmap a) (subst varmap b)
    subst _ CypherTrueCond = CypherTrueCond
    subst _ CypherFalseCond = CypherFalseCond
    subst varmap (CypherNotCond a) = CypherNotCond (subst varmap a)
    subst varmap (CypherPatternCond a) = CypherPatternCond (subst varmap a)
    subst varmap (CypherExistsCond a) = CypherExistsCond (subst varmap a)
    subst _ cond = error ("unsupported CypherCond" ++ show cond)

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

instance Subst a => Subst [a] where
    subst varmap = map (subst varmap)

instance Subst DependencyGraphNode where
    subst varmap (CypherExprNode expr) = CypherExprNode (subst varmap expr)
    subst varmap e = e

instance Subst Relation where
    subst varmap r@(Equal _) = r
    subst varmap (Functional s) = Functional (Set.map (subst varmap) s)

instance Subst Char where
    subst _ = id

--unify

class Unify a where
    unify :: a -> a -> Maybe CypherVarExprMap

instance Unify CypherExpr where
    unify (CypherVarExpr vara) b =
        Just (cypherVarExprMap vara b)
    unify a (CypherVarExpr varb) =
        Just (cypherVarExprMap varb a)
    unify (CypherIntConstExpr i) (CypherIntConstExpr j) = do
        guard (i == j)
        return mempty
    unify (CypherStringConstExpr s) (CypherStringConstExpr t) = do
        guard (s == t)
        return mempty
    unify (CypherDotExpr va expra) (CypherDotExpr vb exprb) = do
        cv <- unify va vb
        guard (expra == exprb)
        return cv
    unify (CypherAppExpr fna expra) (CypherAppExpr fnb exprb) = do
        guard (fna == fnb)
        unify expra exprb
    unify (CypherParamExpr pa) (CypherParamExpr pb) = do
        guard (pa == pb)
        return mempty
    unify CypherNullExpr CypherNullExpr = Just mempty
    unify _ _ = Nothing

instance Unify CypherCond where
    unify (CypherAndCond a b) (CypherAndCond a2 b2) = do
        unify [a, b] [a2, b2]
    unify (CypherOrCond a b) (CypherOrCond a2 b2) = do
        unify [a, b] [a2, b2]
    unify (CypherTrueCond ) (CypherTrueCond) = do
        return mempty
    unify (CypherFalseCond) (CypherFalseCond) = do
        return mempty
    unify (CypherNotCond a) (CypherNotCond a2) =
        unify a a2
    unify (CypherExistsCond a) (CypherExistsCond a2) = do
        -- let varmap = fromList [(v, CypherRigidVarExpr v2), (v2, CypherRigidVarExpr v2)]
        -- let a' = subst varmap a
        -- let a2' = subst varmap a2
        -- unify a' a2'
        unify a a2
    unify (CypherNotExistsCond a) (CypherNotExistsCond a2) = do
        -- let varmap = fromList [(v, CypherRigidVarExpr v2), (v2, CypherRigidVarExpr v2)]
        -- let a' = subst varmap a
        -- let a2' = subst varmap a2
        -- unify a' a2'
        unify a a2
    unify (CypherPatternCond p) (CypherPatternCond p2) = do
        unify p p2
    unify (CypherCompCond op a b) (CypherCompCond op2 a2 b2) = do
        guard (op == op2)
        ma <- unify a a2
        mb <- unify (subst ma b) (subst ma b2)
        return (ma <> mb)
    unify _ _ = Nothing

instance Unify a => Unify (Maybe a) where
    unify (Just a) (Just a2) = unify a a2
    unify Nothing Nothing = Just mempty
    unify _ _ = Nothing

instance (Subst a, Unify a) => Unify [a] where
    unify as as2 = do
        guard (length as == length as2)
        foldM (\ m (a, a2) -> do
            m1 <- unify (subst m a) (subst m a2)
            return (m <> m1)) mempty (zip as as2)

instance (Unify a, Unify b, Subst b) => Unify (a, b) where
    unify (a, b) (a2, b2) = do
        ma <- unify a a2
        mb <- unify (subst ma b) (subst ma b2)
        return (ma <> mb)

instance Unify OneGraphPattern where
    unify (GraphNodePattern n) (GraphNodePattern n2) =
        unify n n2

    unify (GraphEdgePattern m l n) (GraphEdgePattern m2 l2 n2)= do
        mm <- unify m m2
        ml <- unify (subst mm l) (subst mm l2)
        mn <- unify (subst (mm <> ml) n) (subst (mm <> ml) n2)
        return (mm <> ml <> mn)
    unify _ _ = Nothing

instance Unify NodePattern where
    unify (NodePattern varmaybe labelmaybe props) (NodePattern varmaybe2 labelmaybe2 props2) = do
        mvar <- unify (fmap CypherVarExpr varmaybe) (fmap CypherVarExpr varmaybe2)
        guard (labelmaybe == labelmaybe2)
        -- assume that there is no duplication in propkeys
        let props' = subst mvar props
        let props2' = subst mvar props2
        let map1 = fromList props'
        mprops <- foldl (\mm (key, expr2) -> case lookup key map1 of
                Nothing -> Nothing
                Just expr -> do
                    m2 <- unify expr expr2
                    m <- mm
                    return (m <> m2)) mempty props2'
        return (mvar <> mprops)

instance Unify GraphPattern where
    unify (GraphPattern l1 l12) (GraphPattern l2 l22) = do
        s <- unify l1 l2
        unify (subst s l12) (subst s l22)

instance Unify Cypher where
    unify (Cypher r m w s c d) (Cypher r2 m2 w2 s2 c2 d2) = do
        mr <- unify r r2
        mm <- unify (subst mr m) (subst mr m2)
        let mm' = mr <> mm
        mw <- unify (subst mm' w) (subst mm' w2)
        let mw' = mm' <> mw
        ms <- unify (subst mw' s) (subst mw' s2)
        let ms' = mw' <> ms
        mc <- unify (subst ms' c) (subst ms' c2)
        let mc' = ms' <> mc
        md <- unify (subst mc' (map CypherVarExpr d)) (subst mc' (map CypherVarExpr d2))
        return (mc' <> md)

unifys :: (Unify a, Subst a) => [a] -> Maybe CypherVarExprMap
unifys as = unify (take ((length as) - 1) as) (tail as)

-- translation

type CypherQuery = ([Var], Cypher)

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
type CypherPredTableMap = Map String CypherMapping
-- builtin predicate -> op, neg op
newtype CypherBuiltIn = CypherBuiltIn (Map String (Sign -> [Expr] -> TransMonad Cypher))
type TransMonad a = StateT (CypherBuiltIn, CypherPredTableMap, DependencyGraph, CypherVarMap) NewEnv a

addDependencies :: DependencyGraph -> TransMonad ()
addDependencies m = do
    (a, b, dg, params) <- get
    put (a, b, dg `Map.union` m, params)

mergeCreateAndSet :: Cypher -> Cypher
mergeCreateAndSet (Cypher r m w s c d) =
    let (c', s') = mergeCreateAndSet2 c s in
        trace ("mergeCreateAndSet: " ++ show (Cypher r m w s c d)) $ Cypher r m w s' c' d

mergeCreateAndSet2 :: GraphPattern -> [(CypherExpr, CypherExpr)] -> (GraphPattern, [(CypherExpr, CypherExpr)])
mergeCreateAndSet2 cres sets  =
  let map1 = createsToMap cres
      (sets', map1') = runState (doMergeCreateAndSet2 sets) map1 in
      (GraphPattern (elems map1') [], sets')

doMergeCreateAndSet2 :: [(CypherExpr, CypherExpr)] -> State (Map CypherVar OneGraphPattern) [(CypherExpr, CypherExpr)]
doMergeCreateAndSet2 = foldM doMergeCreateAndOneSet [] where
    doMergeCreateAndOneSet sets set@(CypherDotExpr (CypherVarExpr v) p, r) = do
        map1 <- get
        case lookup v map1 of
            Nothing -> return (sets ++ [set])
            Just n -> do
                let n' = addPropToPattern n p r
                put (insert v n' map1)
                return sets
    addPropToPattern (GraphNodePattern (NodePattern v l ps)) p r = GraphNodePattern (NodePattern v l (ps `union` [(p, r)]))
    addPropToPattern (GraphEdgePattern n1 (NodePattern v l ps) n2) p r = GraphEdgePattern n1 (NodePattern v l (ps `union` [(p,r)])) n2

mergeCreateAndWhere :: Cypher -> Cypher
mergeCreateAndWhere cyp@(Cypher r m w s c d) =
    let (c', w') = mergeCreateAndWhere2 c w in
        trace ("mergeCreateAndWhere: " ++ show cyp) $ Cypher r m w' s c' d

mergeCreateAndWhere2 :: GraphPattern -> CypherCond -> (GraphPattern, CypherCond)
mergeCreateAndWhere2 cres sets  =
  let map1 = createsToMap cres
      (sets', map1') = runState (doMergeCreateAndWhere2 sets) map1 in
      (GraphPattern (elems map1') [], sets')

doMergeCreateAndWhere2 :: CypherCond -> State (Map CypherVar OneGraphPattern) CypherCond
doMergeCreateAndWhere2 (CypherAndCond a b) = (.&&.) <$> doMergeCreateAndWhere2 a <*>  doMergeCreateAndWhere2 b
doMergeCreateAndWhere2 (CypherOrCond a b) = (.||.) <$> doMergeCreateAndWhere2 a <*>  doMergeCreateAndWhere2 b
doMergeCreateAndWhere2 (CypherTrueCond) = return CypherTrueCond
doMergeCreateAndWhere2 (CypherFalseCond) = return CypherFalseCond
doMergeCreateAndWhere2 cond@(CypherNotCond a) = return cond -- CypherNotCond <$> doMergeCreateAndWhere2 a
doMergeCreateAndWhere2 cond@(CypherExistsCond _) = return cond
-- only merge mergable patterns
doMergeCreateAndWhere2 c@(CypherPatternCond (GraphPattern [GraphNodePattern (NodePattern (Just v) l ps)] [])) =  do
    map1 <- get
    return (case lookup v map1 of
        Nothing -> c
        Just (GraphNodePattern (NodePattern _ _ ps2)) ->
            let ps' = ps \\ ps2 in
                case ps' of
                    [] -> CypherTrueCond
                    _ -> CypherFalseCond
        _ -> c)
doMergeCreateAndWhere2 c@(CypherPatternCond (GraphPattern [GraphEdgePattern ns (NodePattern (Just v) l ps) nt] [])) =  do
    map1 <- get
    return (case lookup v map1 of
        Nothing -> c
        Just (GraphEdgePattern _ (NodePattern _ _ ps2) _) ->
            let ps' = ps \\ ps2 in
                case ps' of
                    [] -> CypherTrueCond
                    _ -> CypherFalseCond
        _ -> c)
doMergeCreateAndWhere2 c = return c -- for now ignore other patterns

mergeSetExprAndSetNull :: Cypher->Cypher
mergeSetExprAndSetNull (Cypher r m w s c d) = Cypher r m w (mergeSetExprAndSetNullSet s s) c d

mergeSetExprAndSetNullSet :: [(CypherExpr, CypherExpr)] -> [(CypherExpr, CypherExpr)] ->[(CypherExpr, CypherExpr)]
mergeSetExprAndSetNullSet [] _ = []
mergeSetExprAndSetNullSet (set@(a, CypherNullExpr) : sets) sets2 =
      if any (\(b,_)-> b==a) (sets2 \\ [set])
          then mergeSetExprAndSetNullSet sets sets2
          else set : mergeSetExprAndSetNullSet sets sets2
mergeSetExprAndSetNullSet (set : sets)  set2 = set : mergeSetExprAndSetNullSet sets set2

mergeDeleteAndSetNull :: Cypher->Cypher
mergeDeleteAndSetNull (Cypher r m w s c d) = Cypher r m w (mergeDeleteAndSetNull2 d s) c d

mergeDeleteAndSetNull2 :: [CypherVar] -> [(CypherExpr, CypherExpr)] ->[(CypherExpr, CypherExpr)]
mergeDeleteAndSetNull2 vars = filter (\set-> case set of
    (CypherDotExpr (CypherVarExpr v) _,CypherNullExpr) -> v `notElem` vars
    _ -> True)

separateMatchFromWhere :: Cypher  -> Cypher
separateMatchFromWhere (Cypher r m w s c d) =
    let (m', w') = separateMatchFromWhere2 w in
        trace ("separateMatchFromWhere: " ++ show (Cypher r m w s c d)) $ Cypher r (m <> m') w' s c d

separateMatchFromWhere2 :: CypherCond -> (GraphPattern, CypherCond)
separateMatchFromWhere2 (CypherAndCond a b) =
    let (gp1, c1) = separateMatchFromWhere2 a
        (gp2, c2) = separateMatchFromWhere2 b in
        (mappend gp1 gp2, c1 .&&. c2)
separateMatchFromWhere2 (CypherTrueCond) =
        (mempty, CypherTrueCond)
separateMatchFromWhere2 (CypherPatternCond (GraphPattern [GraphNodePattern (NodePattern (Just v) l ps)] [])) =
    (GraphPattern [GraphNodePattern (NodePattern (Just v) l psconst)] [], cond) where
        (psconst, psnonconst) = partition (\(p,e)->isCypherConstExpr e) ps
        cond = conj (map (\(p,e)-> (v `dot'` p) .=. e) psnonconst)
separateMatchFromWhere2 (CypherPatternCond (GraphPattern [GraphEdgePattern ns (NodePattern (Just v) l ps) nt] [])) =
    (GraphPattern [GraphEdgePattern ns (NodePattern (Just v) l psconst) nt] [], cond) where
        (psconst, psnonconst) = partition (\(p,e)->isCypherConstExpr e) ps
        cond = conj (map (\(p,e)-> (v `dot'` p) .=. e) psnonconst)
separateMatchFromWhere2 (CypherPatternCond (GraphPattern [] [GraphNodePattern (NodePattern (Just v) l ps)] )) =
    (GraphPattern [] [GraphNodePattern (NodePattern (Just v) l psconst)], cond) where
        (psconst, psnonconst) = partition (\(p,e)->isCypherConstExpr e) ps
        cond = conj (map (\(p,e)-> (v `dot'` p) .=. e) psnonconst)
separateMatchFromWhere2 (CypherPatternCond (GraphPattern [] [GraphEdgePattern ns (NodePattern (Just v) l ps) nt])) =
    (GraphPattern [] [GraphEdgePattern ns (NodePattern (Just v) l psconst) nt], cond) where
        (psconst, psnonconst) = partition (\(p,e)->isCypherConstExpr e) ps
        cond = conj (map (\(p,e)-> (v `dot'` p) .=. e) psnonconst)
separateMatchFromWhere2 a = (GraphPattern [] [], a)

eliminateUnboundedVars :: Cypher -> Cypher
eliminateUnboundedVars (Cypher r m w s c d) =
    let fvm = (fv m)
        (w', (varmap, cond)) = runState (eliminateUnboundedVars2 fvm w) (mempty, CypherTrueCond) in
        trace ("eliminateUnboundedVars: " ++ show (Cypher r m w s c d)) $ subst varmap (Cypher r m (w' .&&. cond) s c d)

eliminateUnboundedVars2 :: [CypherVar] -> CypherCond -> State (CypherVarExprMap, CypherCond) CypherCond
eliminateUnboundedVars2 vars (CypherAndCond a b) = (.&&.) <$> eliminateUnboundedVars2 vars a <*> eliminateUnboundedVars2 vars b
eliminateUnboundedVars2 vars (CypherOrCond a b) = (.||.) <$> eliminateUnboundedVars2 vars a <*> eliminateUnboundedVars2 vars b
eliminateUnboundedVars2 vars (CypherTrueCond) = return CypherTrueCond
eliminateUnboundedVars2 vars (CypherFalseCond) = return CypherFalseCond
eliminateUnboundedVars2 vars (CypherNotCond a) = CypherNotCond <$> eliminateUnboundedVars2 vars a
eliminateUnboundedVars2 vars cond@(CypherExistsCond c) = CypherExistsCond <$> eliminateUnboundedVars2 vars c
eliminateUnboundedVars2 vars (CypherPatternCond p) = do
    p' <- eliminateUnboundedVars3 vars p
    return (case p' of
        Nothing -> CypherTrueCond
        Just p' -> CypherPatternCond p')
eliminateUnboundedVars2 vars c@(CypherCompCond op e1 e2) =
    (if null (fv e1 \\ vars)
        then if null (fv e2 \\ vars)
            then return c
            else do
                addToVarMap (extractVarFromExpr e2) e1
                trace ("add " ++ show e2 ++ " :=: " ++ show e1) $ return ()
                return CypherTrueCond
        else do
            addToVarMap (extractVarFromExpr e1) e2
            trace ("add " ++ show e1 ++ " :=: " ++ show e2) $ return ()
            return CypherTrueCond)

addToVarMap :: CypherVar -> CypherExpr -> State (CypherVarExprMap, CypherCond) ()
addToVarMap var expr = do
    (varmap@(CypherVarExprMap m), cond ) <- get
    case lookup var m of
        Nothing ->
            put (varmap <> cypherVarExprMap var expr, cond)
        Just expr2 ->
            put (varmap, cond .&&. (expr .=. expr2))

eliminateUnboundedVars3 :: [CypherVar] -> GraphPattern -> State (CypherVarExprMap,CypherCond) (Maybe GraphPattern)
eliminateUnboundedVars3 vars (GraphPattern [GraphNodePattern (NodePattern (Just v) l ps)] []) =
    if v `elem` vars
        then do
          let (psvar, psnonvar) = partition (\(p,e)-> null (fv e \\ vars)) ps
          let pat = case psnonvar of
                  [] -> Nothing
                  _ -> Just (GraphPattern [GraphNodePattern (NodePattern (Just v) l psnonvar)] [])
          mapM_ (\ (p,e)-> do
              addToVarMap (extractVarFromExpr e) (v `dot'` p))  psvar
          return pat
        else return Nothing
eliminateUnboundedVars3 vars (GraphPattern [GraphEdgePattern ns (NodePattern (Just v) l ps) nt] []) =
    if v `elem` vars && nodeVar2 ns `elem` vars && nodeVar2 nt `elem` vars
        then do
          let (psvar, psnonvar) = partition (\(p,e)-> null (fv e \\ vars)) ps
          let pat = case psnonvar of
                  [] -> Nothing
                  _ -> Just (GraphPattern [GraphEdgePattern ns (NodePattern (Just v) l psnonvar) nt] [])
          mapM_ (\ (p,e)-> do
              addToVarMap (extractVarFromExpr e) (v `dot'` p))  psvar
          return pat
            -- assume ns nt have no properites
        else return Nothing
eliminateUnboundedVars3 vars (GraphPattern [] [GraphNodePattern (NodePattern (Just v) l ps)]) =
    if v `elem` vars
        then do
          let (psvar, psnonvar) = partition (\(p,e)-> null (fv e \\ vars)) ps
          let pat = case psnonvar of
                  [] -> Nothing
                  _ -> Just (GraphPattern [] [GraphNodePattern (NodePattern (Just v) l psnonvar)])
          mapM_ (\ (p,e)-> do
              addToVarMap (extractVarFromExpr e) (v `dot'` p))  psvar
          return pat
        else return Nothing
eliminateUnboundedVars3 vars (GraphPattern [] [GraphEdgePattern ns (NodePattern (Just v) l ps) nt]) =
    if v `elem` vars && nodeVar2 ns `elem` vars && nodeVar2 nt `elem` vars
        then do
          let (psvar, psnonvar) = partition (\(p,e)-> null (fv e \\ vars)) ps
          let pat = case psnonvar of
                  [] -> Nothing
                  _ -> Just (GraphPattern [] [GraphEdgePattern ns (NodePattern (Just v) l psnonvar) nt])
          mapM_ (\ (p,e)-> do
              addToVarMap (extractVarFromExpr e) (v `dot'` p))  psvar
          return pat
            -- assume ns nt have no properites
        else return Nothing
eliminateUnboundedVars3 _ p = error ("pattern " ++ show p)

eliminateUnboundedVars4 :: [CypherVar] -> Cypher -> State (CypherVarExprMap, CypherCond) Cypher
eliminateUnboundedVars4 vars (Cypher r m w s c d) = do
    w' <- eliminateUnboundedVars2 (vars `union` fv m) w
    return (Cypher r m w' s c d)

normalizeGraphPattern :: Cypher -> Cypher
normalizeGraphPattern (Cypher r m w s c d) = trace ("normalizeGraphPattern: " ++ show (Cypher r m w s c d)) $ Cypher r m (normalizeGraphPatternCond w) s c d

normalizeGraphPatternCond :: CypherCond -> CypherCond
normalizeGraphPatternCond a@(CypherAndCond _ _) =
    let as = getConjuncts a
        (graphpatterns, nongraphpatterns) = partition (\c -> case c of CypherPatternCond _ -> True ; _ -> False) as in
        normalizeGraphPatternCond (CypherPatternCond (mconcat (map (\(CypherPatternCond p)->p) graphpatterns))) .&&. conj nongraphpatterns
normalizeGraphPatternCond (CypherNotCond a) = CypherNotCond (normalizeGraphPatternCond a)
normalizeGraphPatternCond (CypherOrCond a b) =  ( normalizeGraphPatternCond a) .||. ( normalizeGraphPatternCond b)
normalizeGraphPatternCond CypherTrueCond = CypherTrueCond
normalizeGraphPatternCond CypherFalseCond = CypherFalseCond
normalizeGraphPatternCond (CypherPatternCond p) =
        let onegraphs = splitGraphPattern (mconcat (splitGraphPattern p)) in
            conj (map (CypherPatternCond) onegraphs)
normalizeGraphPatternCond (CypherExistsCond c) = CypherExistsCond (normalizeGraphPatternCond c)
normalizeGraphPatternCond c = c

normalizeCond :: Cypher -> Cypher
normalizeCond (Cypher r m w s c d) =
    trace ("normalizeCond: " ++ show (Cypher r m w s c d)) $ Cypher r m (normalizeCond2 w) s c d

normalizeCond2 :: CypherCond -> CypherCond
normalizeCond2 CypherTrueCond = CypherTrueCond
normalizeCond2 CypherFalseCond = CypherFalseCond
normalizeCond2 (CypherAndCond a b) =
    normalizeCond2 a .&&. normalizeCond2 b
normalizeCond2 (CypherOrCond a b) =
    normalizeCond2 a .||. normalizeCond2 b
normalizeCond2 (CypherNotCond a) =
    CypherNotCond (normalizeCond2 a)
normalizeCond2 (CypherExistsCond c) = CypherExistsCond (normalizeCond2 c)
normalizeCond2 c = c

mergeAllPatternInWhere :: Cypher -> Cypher
mergeAllPatternInWhere (Cypher r m w s c d) = trace ("mergeAllPatternInWhere: " ++ show (Cypher r m w s c d)) $ Cypher r m (mergeAllPatternInWhere2 w) s c d

mergeAllPatternInWhere2 :: CypherCond -> CypherCond
mergeAllPatternInWhere2 CypherTrueCond = CypherTrueCond
mergeAllPatternInWhere2 CypherFalseCond = CypherFalseCond
mergeAllPatternInWhere2 a@(CypherAndCond _ _) =
    let as = getDisjuncts a
        (graphpatterns, nongraphpatterns) = partition (\c -> case c of CypherPatternCond _ -> True ; _ -> False) as in
        CypherPatternCond (mconcat (concatMap (\(CypherPatternCond (GraphPattern ps ps2))->[GraphPattern ps [], GraphPattern ps2 []]) graphpatterns)) .&&. conj nongraphpatterns
mergeAllPatternInWhere2 (CypherNotCond a) = CypherNotCond (mergeAllPatternInWhere2 a)
mergeAllPatternInWhere2 (CypherOrCond a b) = mergeAllPatternInWhere2 a .||. mergeAllPatternInWhere2 b
mergeAllPatternInWhere2 c@(CypherPatternCond p) = c
mergeAllPatternInWhere2 (CypherExistsCond c) = CypherExistsCond (mergeAllPatternInWhere2 c)
mergeAllPatternInWhere2 c = c

mergeAllPatternInMatch :: Cypher -> Cypher
mergeAllPatternInMatch (Cypher r m w s c d) = trace ("mergeAllPatternInMatch: " ++ show (Cypher r m w s c d)) $ Cypher r (mergeAllPatternInMatch2 m) w s c d

mergeAllPatternInMatch2 :: GraphPattern -> GraphPattern
mergeAllPatternInMatch2 (GraphPattern as as2) = mappend (GraphPattern as []) (GraphPattern as2 [])

translateQueryToCypher :: Query -> TransMonad CypherQuery
translateQueryToCypher (Query vars conj) = do
    cypher <- translateFormulaToCypher conj
    cypher'0 <- simplifyDependencies (cypher)
    (_,_,_,fovarcypherexprmap) <- get
    let exprsMaybe = map (`lookup` fovarcypherexprmap) vars
    let exprs = map (\(v, em) -> fromMaybe (error ("unbounded var " ++ show v ++ " in " ++ show conj)) em) (zip vars exprsMaybe)
    let cypher' = postprocess (cypher'0 <> creturn exprs)
    return (vars,  cypher')

cfalse = Cypher [] mempty CypherFalseCond [] mempty []

translateFormulaToCypher :: Formula -> TransMonad Cypher
translateFormulaToCypher (FSequencing form1 form2) = do
    mappend <$> translateFormulaToCypher form1 <*>  translateFormulaToCypher form2

translateFormulaToCypher (FChoice disj1 disj2) =
    cor <$> translateFormulaToCypher disj1 <*> translateFormulaToCypher disj2
translateFormulaToCypher FOne = return mempty
translateFormulaToCypher FZero = return cfalse

translateFormulaToCypher (FAtomic a) = do
    cypher <- translateQueryAtomToCypher Pos a
    return (cypher)

translateFormulaToCypher (FInsert lits@(Lit sign0 a)) = do
    case sign0 of
        Pos -> translateInsertAtomToCypher a
        Neg -> translateDeleteAtomToCypher a

translateFormulaToCypher' :: PureFormula -> TransMonad Cypher
translateFormulaToCypher' (Conjunction form1 form2) =
    mappend <$> translateFormulaToCypher' form1 <*>  translateFormulaToCypher' form2

translateFormulaToCypher' (Disjunction disj1 disj2) =
    cor <$> translateFormulaToCypher' disj1 <*> translateFormulaToCypher' disj2

translateFormulaToCypher' CTrue = return mempty
translateFormulaToCypher' CFalse = return cfalse
translateFormulaToCypher' (Atomic a) =
    translateQueryAtomToCypher Pos a

translateFormulaToCypher' (Not (Atomic a)) =
    translateQueryAtomToCypher Neg a

translateFormulaToCypher' (Exists var formula) = do
-- assume that all atoms in conj involves var
-- should use subquery in from clause
    (_ , Cypher r m w s c d) <- translateQueryToCypher (Query [var] (convert formula))
    return (cwhere (CypherExistsCond (matchToWhere m .&&. w)))


translateFormulaToCypher' (Not (Exists var formula)) = do
-- assume that all atoms in conj involves var
    (_ , Cypher r m w s c d) <- translateQueryToCypher (Query [var] (convert formula))
    return (cwhere (CypherNotExistsCond (matchToWhere m .&&. w)))

translateFormulaToCypher' (Not _ ) = error "not not pushed"


-- instantiate a cypher mapping
instantiate :: [CypherVar] -> GraphPattern -> GraphPattern -> Dependencies -> [Expr] -> TransMonad (GraphPattern, GraphPattern)
instantiate vars matchpattern pattern dependencies args = do
    let allvars = fv matchpattern `union` fv pattern
    newvars <- lift $ new (map CypherVarExpr allvars)
    let varmap = mconcat (zipWith cypherVarExprMap allvars (map CypherVarExpr newvars))
    let vars' = map (subst varmap . CypherVarExpr) vars
    let vardeps = [(CypherExprNode var, Equal (ExprNode arg)) | (var, arg) <- zip vars' args]
    let dependencies' = map (subst varmap . (CypherVarExpr *** map CypherVarExpr)) dependencies
    let depdeps = [(CypherExprNode v1, Functional (Set.fromList (map CypherExprNode vs2))) | (v1, vs2) <- dependencies' ]
    addDependencies (fromList vardeps)
    addDependencies (fromList depdeps)
    return (subst varmap matchpattern, subst varmap pattern)

unifysCond :: [CypherExpr] -> (CypherCond, CypherVarExprMap)
unifysCond srcs =
    let (vars, nonvars) = partition (\n -> case n of
            CypherVarExpr _ -> True
            _ -> False) srcs
        rep = head vars
        varmap = CypherVarExprMap (fromList (liftA2 (,) (map extractVarFromExpr (tail vars)) [rep])) in
            case nonvars of
                [] -> (CypherTrueCond, varmap)
                es ->
                    let (cond, varmap') = unifyAll (extractVarFromExpr rep) (subst varmap es) [] in
                        (cond, varmap <> varmap')

-- find a locally maximal way of unifying all expression, building up
unifyAll :: CypherVar -> [CypherExpr] -> [CypherExpr] -> (CypherCond, CypherVarExprMap)
unifyAll rep [] cannotunify =
    let e = head cannotunify in
        (equalize cannotunify, cypherVarExprMap rep e)

unifyAll rep (e:es) cannotunify =
    case unifyOne e es of
        Just (es', varmap) ->
            let (cond, varmap') = unifyAll rep es' cannotunify in
                (cond, varmap <> varmap')
        Nothing ->
            unifyAll rep es (cannotunify ++ [e])

-- find e' that unifies with e and generate a substitution
unifyOne :: CypherExpr -> [CypherExpr] -> Maybe ([CypherExpr], CypherVarExprMap)
unifyOne _ [] = Nothing
unifyOne e (e2:es) =
    case unify e e2 of
        Just varmap ->
            Just (subst varmap (e:es), varmap)
        Nothing ->
            unifyOne e es

equalize :: [CypherExpr] -> CypherCond
equalize (e:es) = conj (liftA2 (.=.) [e] es )

-- if two variables a, b depends on the same set of nodes, then they represents the same value
simplifyDependencies :: Cypher -> TransMonad (Cypher)
simplifyDependencies cypher =
    (simplifyDependencies' cypher)

simplifyDependencies' :: Cypher -> TransMonad (Cypher )
simplifyDependencies' cypher = do
    (a, b, dependencyGraph, fovarcypherexprmap) <- get
    case getEqualRelation dependencyGraph of
            Just (src@(CypherExprNode ve@(CypherVarExpr v1)), n2) -> (case n2 of -- n1 is always a cypher var node
                CypherExprNode expr -> -- subst src to expr
                    recurseWith expr
                ExprNode (IntExpr i) ->
                    recurseWith (CypherIntConstExpr i)
                ExprNode (StringExpr s) ->
                    recurseWith (CypherStringConstExpr s)
                ExprNode (VarExpr v) ->
                    case lookup v fovarcypherexprmap of
                        Just expr ->
                            recurseWith expr
                        Nothing ->
                            do
                                let fovarcypherexprmap' = Map.insert v ve fovarcypherexprmap
                                put (a, b, dependencyGraph', fovarcypherexprmap')
                                simplifyDependencies' cypher) where
                dependencyGraph' = Map.delete src dependencyGraph
                recurseWith expr = do
                    let varmap = cypherVarExprMap v1 expr
                    let dependencyGraph'' = Map.map (subst varmap) dependencyGraph'
                    let cypher' = subst varmap cypher
                    let fovarcypherexprmap' = Map.map (subst varmap) fovarcypherexprmap
                    put (a, b, dependencyGraph'', fovarcypherexprmap')
                    trace (show varmap ++ " = " ++ show fovarcypherexprmap) $ return ()
                    simplifyDependencies' cypher'
            Nothing -> case getFunctionalRelation dependencyGraph of
                Nothing -> return cypher
                Just (srcs, _) -> do -- unify all srcs
                    let (cond, varmap) = unifysCond (map unCypherExprNode srcs)
                    let dependencyGraph' = foldr Map.delete dependencyGraph srcs
                    let dependencyGraph'' = Map.map (subst varmap) dependencyGraph'
                    let cypher' = subst varmap cypher
                    let fovarcypherexprmap' = Map.map (subst varmap) fovarcypherexprmap
                    put (a, b, dependencyGraph'', fovarcypherexprmap')
                    trace (show varmap ++ " = " ++ show fovarcypherexprmap) $ return ()
                    simplifyDependencies' cypher'

-- translate atom to cypher in a query
translateQueryAtomToCypher :: Sign -> Atom -> TransMonad Cypher
translateQueryAtomToCypher thesign atom = do
    (CypherBuiltIn builtin, predtablemap, _, params) <- get
    let (Atom (Pred name _) args) = atom
    --try builtin first
    trace (show atom) $ case lookup name builtin of
        Just builtinpred ->
            builtinpred thesign args
        Nothing -> case lookup name predtablemap of
            Just (vars, matchpattern, pattern, dependencies) -> do
                (matchpattern, pattern) <- instantiate vars matchpattern pattern dependencies args
                trace (show matchpattern ++ " <-> " ++ show pattern)$ case thesign of
                    Pos -> return (cwhere (CypherPatternCond matchpattern .&&. CypherPatternCond pattern))
                    Neg -> return (cwhere (CypherPatternCond matchpattern .&&. CypherNotCond (CypherPatternCond pattern)))
            Nothing -> error (name ++ " is not defined")

translateDeleteAtomToCypher :: Atom -> TransMonad Cypher
translateDeleteAtomToCypher atom = do
    (CypherBuiltIn builtin, predtablemap, _, params) <- get
    let (Atom (Pred pred predtype) args) = atom
    case lookup pred predtablemap of
        Just (vars, matchpattern, pattern, dependencies) -> do
            (matchpattern, pattern) <- instantiate vars matchpattern pattern dependencies args
            -- generate a list of vars of match pattern and pattern, the vars that are in pattern but not match pattern needs to be deleted
            -- if the var is prop var then it is set to NULL, if it is a node var then it is deleted
            let mfvt = fvType Root matchpattern
            let fvt = fvType Root pattern
            let (del, setnull) = partition (\(v, c, p, t) -> t == CypherNodeVar) (fvt \\ mfvt)
            return (cwhere (CypherPatternCond matchpattern .&&. CypherPatternCond pattern) <> delete (map (\(a,_,_,_)->a) del)
                 <> set (map (\(_, Dot v prop, _, var) -> (CypherDotExpr (CypherVarExpr v) prop, CypherNullExpr)) setnull))
        Nothing -> error (pred ++ " is not defined")

translateInsertAtomToCypher :: Atom -> TransMonad Cypher
translateInsertAtomToCypher (Atom (Pred pred predtype) args) = do
    (CypherBuiltIn builtin, predtablemap, _, _) <- get
    case lookup pred predtablemap of
        Just (vars, matchpattern, pattern, dependencies) -> do
            (matchpattern, pattern) <- instantiate vars matchpattern pattern dependencies args
            -- generate a list of vars of match pattern and pattern, the vars that are in pattern but not match pattern needs to be inserted
            -- if the var is prop var then it is set to a new value, if it is a node var then it is created
            let mfvt = fvType Root matchpattern
            let fvt = fvType Root pattern
            let (cre, setexpr) = partition (\(v, c, p, t) ->  t == CypherNodeVar) (fvt \\ mfvt)
            return (cwhere (CypherPatternCond matchpattern) <> create (map (\(var, p, nonvarprops, t) -> case p of
                      Edge srcv tgtv l -> edgevlp' (nodev' srcv) var l nonvarprops (nodev' tgtv)
                      Node l -> nodevlp' var l nonvarprops) cre)
                 <> set (map (\(var, Dot v prop, _, _) -> (CypherDotExpr (CypherVarExpr v) prop, CypherVarExpr var)) setexpr))
        Nothing -> error (pred ++ " is not defined")

postprocess =
  normalizeCond
  . eliminateUnboundedVars
  . normalizeGraphPattern -- eliminate empty graph patterns
  . mergeAllPatternInWhere
  . mergeAllPatternInMatch
  . separateMatchFromWhere
  . mergeDeleteAndSetNull
  . mergeCreateAndWhere
  . mergeCreateAndSet
  . mergeSetExprAndSetNull
  . normalizeGraphPattern


data CypherTrans = CypherTrans CypherBuiltIn [String] CypherPredTableMap

nodevl v l = GraphNodePattern (NodePattern (Just (CypherVar v)) (Just l) [])
nodelp l ps = GraphNodePattern (NodePattern Nothing (Just l) ps)
nodep ps = GraphNodePattern (NodePattern Nothing Nothing ps)
nodel l = GraphNodePattern (NodePattern Nothing (Just l) [])
nodevp v ps = GraphNodePattern (NodePattern (Just (CypherVar v)) Nothing ps)

nodev' v = GraphNodePattern (NodePattern (Just v) Nothing [])
nodev v = GraphNodePattern (NodePattern (Just (CypherVar v)) Nothing [])

nodevlp' v l ps = GraphNodePattern (NodePattern (Just v) (Just l) ps)
nodevlp v l ps = GraphNodePattern (NodePattern (Just (CypherVar v)) (Just l) ps)


edgel n1 l n2 = GraphEdgePattern n1 (NodePattern Nothing (Just l) []) n2
edgevl n1 v l n2 = GraphEdgePattern n1 (NodePattern (Just (CypherVar v)) (Just l) []) n2
edgevlp' n1 v l ps n2 = GraphEdgePattern n1 (NodePattern (Just v) (Just l) ps) n2
edgevlp n1 v l ps n2 = GraphEdgePattern n1 (NodePattern (Just (CypherVar v)) (Just l) ps) n2
dot v l = CypherDotExpr (CypherVarExpr (CypherVar v)) (PropertyKey l)
dot' v l = CypherDotExpr (CypherVarExpr v) l
app l e = CypherAppExpr l [CypherVarExpr e]
var v = CypherVarExpr (CypherVar v)
cnull = CypherNullExpr
match m = Cypher [] (GraphPattern m []) CypherTrueCond [] mempty []

type CypherEnv = Map CypherVar CypherExpr

instance Convertible Var CypherVar where
    safeConvert = Right . CypherVar . unVar
instance Convertible MapResultRow CypherEnv where
    safeConvert = Right . foldlWithKey (\map k v  -> insert (convert k) (convert v) map) empty

instance Convertible ResultValue CypherExpr where
    safeConvert (IntValue i) = Right (CypherIntConstExpr i)
    safeConvert (StringValue i) = Right (CypherStringConstExpr i)

data CypherState = CypherState {
    csUpdate  :: Bool
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
        else return ()
translateableCypher _ (FInsert lit) = do
    cs <- get
    put cs{csUpdate = True}
    return ()
translateableCypher trans (FClassical formula) = do
    cs <- get
    if csUpdate cs
        then lift $ Nothing
        else translateableCypher' trans formula
translateableCypher trans (FTransaction form) = translateableCypher trans form

translateableCypher' :: CypherTrans -> PureFormula -> StateT CypherState Maybe ()
translateableCypher' (CypherTrans builtin positiverequired predtablemap) (Not formula)  = lift $ Nothing
        {- if lookForPositiveRequiredSubformula positiverequired formula
            then lift $ Nothing
            else return () where
                lookForPositiveRequiredSubformula pr (Atomic (Atom (Pred p _) _)) = p `elem` pr
                lookForPositiveRequiredSubformula pr (Conjunction s1 s2) = lookForPositiveRequiredSubformula pr s1 || lookForPositiveRequiredSubformula pr s2
                lookForPositiveRequiredSubformula pr (Disjunction s1 s2) = lookForPositiveRequiredSubformula pr s1 || lookForPositiveRequiredSubformula pr s2
                lookForPositiveRequiredSubformula pr p@(Not _) = True
                lookForPositiveRequiredSubformula pr p@(Exists _ _) = True -}
translateableCypher' _ (Exists _ _)  = lift $ Nothing
translateableCypher' _ (Forall _ _)  = lift $ Nothing
translateableCypher' trans (Conjunction form1 form2)  = do
    translateableCypher' trans form1
    translateableCypher' trans form2
translateableCypher' trans (Disjunction form1 form2)  = lift $ Nothing
translateableCypher' _ (Atomic _)  = return ()

instance DBConnection conn CypherQuery  => ExtractDomainSize DBAdapterMonad conn CypherTrans where
    extractDomainSize conn trans varDomainSize (Atom (Pred name _) args) =
        return (if isBuiltIn
            then maxArgDomainSize
            else if name `member` predtablemap then fromList [(fv, Bounded 1) | fv <- freeVars args] else empty) where
            argsDomainSizeMaps = map (exprDomainSizeMap varDomainSize Unbounded) args
            maxArgDomainSize = mmaxs argsDomainSizeMaps
            isBuiltIn = name `member` builtin
            (CypherTrans (CypherBuiltIn builtin) _ predtablemap) = trans

instance Translate CypherTrans MapResultRow CypherQuery where
    translateQueryWithParams trans query@(Query vars _) env =
        let (CypherTrans builtin _ predtablemap) = trans
            fovarcypherexprmap = foldMap (\v@(Var a) -> Map.singleton v (CypherParamExpr a)) env
            sql = runNew (evalStateT (translateQueryToCypher query) (builtin, predtablemap, empty, fovarcypherexprmap)) in
            (sql,  env)
    translateable trans form vars = isJust (evalStateT (translateableCypher trans form) (CypherState False))

    translateable' trans form _ = isJust (evalStateT (translateableCypher' trans form) (CypherState False))

instance New CypherVar CypherExpr where
    new _ = CypherVar <$> new (StringWrapper "var")
-- normalize a graph pattern to canonical representations

data GraphPatternCanonical = CLabel CypherVar Label
                           | CProp CypherVar PropertyKey CypherExpr
                           | CEdge CypherVar CypherVar CypherVar

class Normalize a where
    normalize :: a -> TransMonad [GraphPatternCanonical]

instance Normalize GraphPattern where
    normalize (GraphPattern ps ps2) = do
        ps' <- mapM normalize ps
        ps2' <- mapM normalize ps2
        return (concat (ps' ++ ps2'))

instance Normalize OneGraphPattern where
    normalize (GraphNodePattern n) = do
        n' <- normalize n
        return n'
    normalize (GraphEdgePattern n l m) = do
        n' <- normalize n
        l' <- normalize l
        m' <- normalize m
        return (n'++l'++m'++[CEdge (nodeVar2 n)  (nodeVar l) (nodeVar2 m)])

instance Normalize NodePattern where
    normalize (NodePattern v l ps) = do
        v <- normalizeVar v
        l' <- normalize (v, l)
        ps' <- normalize (v, ps)
        return (l' ++ ps')
normalizeVar :: Maybe CypherVar -> TransMonad CypherVar
normalizeVar (Just v) = return v
normalizeVar Nothing = lift $ new CypherNullExpr

instance Normalize (CypherVar, Maybe Label) where
    normalize (v, Just l) = return [CLabel v l]
    normalize _ = return []

instance Normalize (CypherVar, [(PropertyKey, CypherExpr)]) where
    normalize (v, ps) = return (map (uncurry (CProp v)) ps)
