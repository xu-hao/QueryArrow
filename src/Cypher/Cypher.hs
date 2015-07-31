{-# LANGUAGE TypeFamilies, TypeSynonymInstances, FlexibleInstances, MultiParamTypeClasses, FlexibleContexts, RankNTypes, GADTs, FunctionalDependencies, UndecidableInstances #-}
module Cypher.Cypher (CypherVar, CypherOper, CypherExpr(..), Label, cypherExprFromArg,
    CypherMapping, translateQueryToCypher, translateInsertToCypher,
    CypherCond(..), CypherQuery, GraphPattern(..), NodePattern(..), PropertyKey, Cypher(..),
    CypherBuiltIn(..), CypherPredTableMap, GraphPatternList,CypherTrans(..), (.=.), (.<>.), (.&&.), (.||.),
    nodev, nodel, nodevl, nodevp, nodevlp, nodelp,nodep, edgel, edgevl, var, cnull, true, dot, app, match,
    create, set, delete, cnot, cwhere, creturn, cor, false) where

import FO
import DBQuery

import Prelude hiding (lookup)
import Data.List (intercalate, (\\), union)
import Control.Monad.Trans.State.Strict (State, get, put, evalState, runState,evalStateT)
import Control.Monad (foldM)
import Control.Arrow ((***))
import Data.Map.Strict (empty, Map, insert, (!), member, singleton, adjust, foldlWithKey, lookup, fromList)
import qualified Data.Map.Strict as Map
import Data.Convertible.Base
import Data.Monoid
import Control.Applicative ((<$>))

type Label = String
type PropertyKey = String
data NodePattern = NodePattern (Maybe CypherVar) (Maybe Label) [(PropertyKey, CypherExpr)]
data GraphPattern = GraphEdgePattern GraphPattern NodePattern GraphPattern
                  | GraphNodePattern NodePattern

type CypherVar = String

type CypherOper = String

data CypherValue = CypherIntValue Int
                 | CypherStringValue String
                 | CypherNullValue

data CypherExpr = CypherVarExpr CypherVar
                | CypherIntConstExpr Int
                | CypherStringConstExpr String
                | CypherParamExpr String
                | CypherDotExpr CypherExpr PropertyKey
                | CypherAppExpr String [CypherExpr]
                | CypherNullExpr

data CypherCond = CypherCompCond CypherOper CypherExpr CypherExpr
                | CypherAndCond CypherCond CypherCond
                | CypherOrCond CypherCond CypherCond
                | CypherNotCond CypherCond
                | CypherPatternCond GraphPattern
                | CypherTrueCond
                | CypherFalseCond
                | CypherExistsCond Cypher
                | CypherNotExistsCond Cypher

(.&&.) :: CypherCond -> CypherCond -> CypherCond
CypherTrueCond .&&. b = b
CypherFalseCond .&&. _ = CypherFalseCond
a .&&. CypherTrueCond = a
_ .&&. CypherFalseCond = CypherFalseCond
a .&&. b = CypherAndCond a b

(.||.) :: CypherCond -> CypherCond -> CypherCond
CypherTrueCond .||. _ = CypherTrueCond
CypherFalseCond .||. b = b
_ .||. CypherTrueCond = CypherTrueCond
a .||. CypherFalseCond = a
a .||. b = CypherOrCond a b

(.=.) :: CypherExpr -> CypherExpr -> CypherCond
a .=. b = CypherCompCond "=" a b

(.<>.) :: CypherExpr -> CypherExpr -> CypherCond
a .<>. b = CypherCompCond "<>" a b

type GraphPatternList = [GraphPattern]
data Cypher = Cypher {cypherReturn :: [ CypherExpr ], cypherMatch :: GraphPatternList, cypherWhere :: CypherCond, cypherSet :: [(CypherExpr, CypherExpr)], cypherCreate :: GraphPatternList, cypherDelete :: [CypherVar] }

instance Show Cypher where
    show (Cypher vars patterns conds sets creates deletes) = unwords (filter (not . null) [
        case patterns of
            [] -> ""
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
            _ -> "DELETE " ++ intercalate "," deletes,
        case vars of
            [] -> ""
            _ -> "RETURN " ++ intercalate "," (map show vars)])

instance Monoid Cypher where
    mappend (Cypher r1 m1 w1 s1 c1 d1) (Cypher r2 m2 w2 s2 c2 d2) = Cypher (r1 ++ r2) (m1 ++ m2) (w1 .&&. w2) (s1 ++ s2) (c1 ++ c2) (d1 ++ d2)
    mempty = Cypher [] [] CypherTrueCond [] [] []

cor :: Cypher -> Cypher -> Cypher
cor (Cypher r1 m1 w1 s1 c1 d1) (Cypher r2 m2 w2 s2 c2 d2) = Cypher (r1 ++ r2) (m1 ++ m2) (w1 .||. w2) (s1 ++ s2) (c1 ++ c2) (d1 ++ d2)

false :: Cypher
false = Cypher [] [] CypherFalseCond [] [] []

matchToWhere :: GraphPatternList -> CypherCond
matchToWhere = foldl (\cond pattern -> cond .&&. CypherPatternCond pattern) CypherTrueCond

-- cnot . cnot != id

cnot :: Cypher -> Cypher
cnot (Cypher r1 m1 w1 s1 c1 d1) = Cypher r1 [] (CypherNotCond (w1 .&&. matchToWhere m1)) s1 c1 d1

cwhere :: CypherCond -> Cypher
cwhere c = Cypher [] [] c [] [] []

creturn :: [CypherExpr] -> Cypher
creturn rs = Cypher rs [] CypherTrueCond [] [] []

set :: [(CypherExpr, CypherExpr)] -> Cypher
set ss = Cypher [] [] CypherTrueCond ss [] []

create :: GraphPatternList -> Cypher
create cs = Cypher [] [] CypherTrueCond [] cs []

delete :: [CypherVar] -> Cypher
delete ds = Cypher [] [] CypherTrueCond [] [] ds

instance Show GraphPattern where
    show (GraphNodePattern nodepattern) = show nodepattern
    show (GraphEdgePattern nodepatternsrc label nodepatterntgt) =
        show nodepatternsrc ++ "-[" ++ show label ++ "]->" ++ show nodepatterntgt

instance Show NodePattern where
    show (NodePattern var label props) =
        "(" ++ (case var of
            Just v -> v
            Nothing -> "") ++ (case label of
            Just l -> ":" ++ l
            Nothing -> "") ++ (if not (null props)
            then "{" ++ intercalate "," (f props) ++ "}"
            else "") ++ ")" where
        f = map (\(key, value) -> key ++ ":" ++ show value)

instance Show CypherCond where
    show (CypherCompCond op lhs rhs) = show lhs ++ " " ++ op ++ " " ++ show rhs
    show (CypherAndCond a b) = "(" ++ show a ++ " AND " ++ show b ++ ")"
    show (CypherOrCond a b) = "(" ++ show a ++ " OR " ++ show b ++ ")"
    show (CypherPatternCond a) = show a
    show (CypherNotCond a) = "NOT (" ++ show a ++ ")"
    show CypherTrueCond = "@TRUE"
    show CypherFalseCond = "@FALSE"
    show (CypherExistsCond cypher) = "(EXISTS (" ++ show cypher ++ "))"
    show (CypherNotExistsCond cypher) = "(NOT EXISTS (" ++ show cypher ++ "))"

instance Show CypherValue where
    show (CypherIntValue i) = show i
    show (CypherStringValue s)=s
    show (CypherNullValue) = "NULL"

instance Show CypherExpr where
    show (CypherVarExpr var) = var
    show (CypherIntConstExpr i) = show i
    show (CypherStringConstExpr s) = "'" ++ cypherStringEscape s ++ "'"
    show (CypherParamExpr m) = "{" ++ m ++ "}"
    show (CypherDotExpr expr prop) = show expr ++ "." ++ prop
    show (CypherAppExpr f args) = f ++ "(" ++ intercalate "," (map show args) ++ ")"
    show (CypherNullExpr) = "NULL"

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

extractVarFromExpr :: CypherExpr -> CypherVar
extractVarFromExpr (CypherVarExpr var) = var
extractVarFromExpr _ = error "this is not a var expr"

class FV a where
    fv :: a -> [CypherVar]

instance FV CypherVar where
    fv var = [var]

instance FV GraphPattern where
    fv (GraphEdgePattern nodepatternsrc label nodepatterntgt) =
        fv nodepatternsrc `union` fv nodepatterntgt
    fv (GraphNodePattern nodepattern) = fv nodepattern

instance FV GraphPatternList where
    fv = foldl f [] where
        f vars pattern = vars `union` fv pattern

instance FV NodePattern where
    fv (NodePattern var label props) = (case var of
        Just var -> [var]
        Nothing -> []) `union` fv props

instance FV CypherCond where
    fv (CypherCompCond op a b) = fv a `union` fv b
    fv (CypherAndCond a b) = fv a `union` fv b
    fv (CypherOrCond a b) = fv a `union` fv b
    fv (CypherNotCond a) = fv a
    fv (CypherPatternCond a) = fv a
    fv CypherTrueCond = []
    fv CypherFalseCond = []
    fv cond = error ("unsupported CypherCond" ++ show cond)

instance FV CypherExpr where
    fv (CypherVarExpr var) = [var]
    fv (CypherDotExpr expr _) = fv expr
    fv (CypherAppExpr _ args) = foldl union [] (map fv args)
    fv _ = []

instance FV [Var] where
   fv vars = unions (map fv vars)

instance FV [(CypherExpr, CypherExpr)] where
   fv sets = unions (map (\(l, r) -> fv l `union` fv r) sets)

instance FV Cypher where
   fv (Cypher r m w s c d) = fv m `union` fv w `union` fv s `union` fv c `union` fv d

instance FV [(PropertyKey, CypherExpr)] where
   fv = unions . map (\(a, b) -> fv b)

class Subst a b|a->b where
   subst :: Map CypherVar CypherExpr -> a -> b

instance Subst CypherVar CypherExpr where
   subst varmap var = case lookup var varmap of
       Nothing -> CypherVarExpr var
       Just var2 -> var2
instance Subst [CypherVar] [CypherExpr] where
    subst varmap vars = map (subst varmap) vars
instance Subst GraphPattern GraphPattern where
   subst varmap (GraphEdgePattern nodepatternsrc label nodepatterntgt) =
       GraphEdgePattern (subst varmap nodepatternsrc) (subst varmap label) (subst varmap nodepatterntgt)
   subst varmap (GraphNodePattern nodepattern) =
       GraphNodePattern (subst varmap nodepattern)

instance Subst NodePattern NodePattern where
    subst varmap (NodePattern var label props) =
        NodePattern (fmap extractVarFromExpr (fmap (subst varmap) var)) label (subst varmap props)

instance Subst GraphPatternList GraphPatternList where
    subst varmap = map (subst varmap)

instance Subst CypherCond CypherCond where
    subst varmap (CypherCompCond op a b) = CypherCompCond op (subst varmap a) (subst varmap b)
    subst varmap (CypherAndCond a b) = CypherAndCond (subst varmap a) (subst varmap b)
    subst varmap (CypherOrCond a b) = CypherOrCond (subst varmap a) (subst varmap b)
    subst varmap (CypherNotCond a) = CypherNotCond (subst varmap a)
    subst varmap (CypherPatternCond a) = CypherPatternCond (subst varmap a)
    subst varmap CypherTrueCond = CypherTrueCond
    subst varmap CypherFalseCond = CypherFalseCond
    subst _ cond = error ("unsupported CypherCond" ++ show cond)

instance Subst CypherExpr CypherExpr where
    subst varmap (CypherVarExpr var) = subst varmap var
    subst varmap (CypherDotExpr expr prop) = CypherDotExpr (subst varmap expr) prop
    subst varmap (CypherAppExpr f args) = CypherAppExpr f (map (subst varmap) args)
    subst _ a = a

instance Subst CypherVarExprMap CypherVarExprMap where
    subst varmap = Map.map (subst varmap)

instance Subst CypherQuery CypherQuery where
    subst varmap = \(a,b)-> (a, subst varmap b)

instance Subst Cypher Cypher where
    subst varmap (Cypher r m w s c d) = Cypher (subst varmap r) (subst varmap m) (subst varmap w) (subst varmap s) (subst varmap c) (map extractVarFromExpr (subst varmap d))

instance Subst [(CypherExpr, CypherExpr)] [(CypherExpr,CypherExpr)] where
    subst varmap = map (subst varmap *** subst varmap)

instance Subst [(PropertyKey, CypherExpr)] [(PropertyKey, CypherExpr)] where
    subst varmap = map (id *** subst varmap)

instance Subst [CypherExpr] [CypherExpr] where
    subst varmap = map (subst varmap)

type CypherQuery = ([Var], Cypher)

-- mapping from predicate to a cypher query
type CypherMapping = ([CypherExpr], Cypher)

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
type CypherVarExprMap = Map CypherVar CypherExpr
data CypherAction = Set CypherCond [(CypherExpr, CypherExpr)]
                  | Create GraphPatternList
-- predicate -> pattern (query, insert, delete)
type CypherPredTableMap = Map String (CypherMapping, CypherMapping, CypherMapping)
-- builtin predicate -> op, neg op
newtype CypherBuiltIn = CypherBuiltIn (Map String (Sign -> [Expr] -> TransMonad Cypher))
type TransMonad a = State (CypherBuiltIn, CypherPredTableMap, CypherVarMap, CypherVarExprMap, Int) a


addSubst :: CypherVar -> CypherExpr -> TransMonad ()
addSubst var expr = do
    (a, b, c, varexprmap, e) <- get
    let varexprmap1 = insert var (subst varexprmap expr) varexprmap
    put (a, b, c, subst varexprmap1 varexprmap1,e)

addVarExpr :: Var -> CypherExpr -> TransMonad ()
addVarExpr var expr = do
    (a, b, varexprmap,c,  e) <- get
    let varexprmap1 = insert var expr varexprmap
    put (a, b, varexprmap1, c, e)

lookupVarExpr :: Var -> TransMonad (Maybe CypherExpr)
lookupVarExpr var = do
    (_, _, varexprmap, _,  _) <- get
    return (lookup var varexprmap)

substCypher :: Subst a b => a -> TransMonad b
substCypher a = do
    (_, _, _,varexprmap,   _) <- get
    return (subst varexprmap a)

unify :: CypherExpr -> CypherExpr -> TransMonad CypherCond
unify a b = (do
    a' <- substCypher a
    b' <- substCypher b
    case (a', b') of
        (CypherVarExpr vara, _) -> do
            addSubst vara b'
            return CypherTrueCond
        (_, CypherVarExpr varb) -> do
            addSubst varb a'
            return CypherTrueCond
        (_, _) -> do
            return (CypherCompCond "=" a' b'))


newVar :: TransMonad CypherVar
newVar = do
    (bi, ptm, env, subst, vid) <- get
    put (bi, ptm, env, subst, vid+1)
    return ("var" ++ show vid)

translateQueryToCypher :: Query -> TransMonad CypherQuery
translateQueryToCypher (Query vars conj) = do
    cypher <- translateFormulaToCypher conj
    exprsMaybe <- mapM lookupVarExpr vars
    let exprs = map (\em-> case em of Just e -> e ; Nothing -> error "unbounded var") exprsMaybe
    substCypher (vars, cypher  <>  creturn exprs)

translateFormulaToCypher :: Formula -> TransMonad Cypher
translateFormulaToCypher (Conjunction formulas) =
    mconcat <$> mapM translateFormulaToCypher formulas

translateFormulaToCypher (Disjunction disjs) =
    foldM (\c f -> do
      f' <- translateFormulaToCypher f
      return (c `cor` f')) (cwhere CypherFalseCond) disjs

translateFormulaToCypher (Atomic a) =
    translateAtomToCypher Pos a

translateFormulaToCypher (Not (Atomic a)) =
    translateAtomToCypher Neg a

translateFormulaToCypher (Exists var formula) = do
-- assume that all atoms in conj involves var
-- should use subquery in from clause
    (_ , sql) <- translateQueryToCypher (Query [var] formula)
    return (cwhere (CypherExistsCond sql))


translateFormulaToCypher (Not (Exists var formula)) = do
-- assume that all atoms in conj involves var
    (_ , sql) <- translateQueryToCypher (Query [var] formula)
    return (cwhere (CypherNotExistsCond sql))

translateFormulaToCypher (Not _ ) = error "not not pushed"

translateAtomToCypher :: Sign -> Atom -> TransMonad Cypher
translateAtomToCypher thesign (Atom (Pred name _) args) = do
    (CypherBuiltIn builtin, predtablemap, _, _, _) <- get
    --try builtin first
    case lookup name builtin of
        Just builtinpred -> do
            builtinpred thesign args
        Nothing -> case lookup name predtablemap of
            Just (( exprs, cypher), _, _) -> do
                newcypher <- instantiate exprs cypher args
                case thesign of
                    Pos -> return newcypher
                    Neg -> return (cnot newcypher)
            Nothing -> error (name ++ " is not defined")



cypherExprFromArg :: Expr -> TransMonad (Either CypherExpr Var)
cypherExprFromArg arg = do
    case arg of
        VarExpr var -> do
            exprMaybe <- lookupVarExpr var
            case exprMaybe of
                Nothing -> return (Right var)
                Just expr -> return (Left expr)
        IntExpr i ->
            return (Left (CypherIntConstExpr i))
        StringExpr s ->
            return (Left (CypherStringConstExpr s))


instantiate :: [CypherExpr] -> Cypher -> [Expr] -> TransMonad Cypher
instantiate exprs cypher args = do
    -- find all vars
    let allvars = fv cypher `union` unions (map fv exprs)
    newvars <- mapM (const newVar) allvars
    let varmap = fromList (zip allvars (map CypherVarExpr newvars))
    let newexprs = subst varmap exprs
    let newcypher = subst varmap cypher
    -- for each arg
    -- if the arg is a var, then look it up from var map; if exists generate equation; if not put (var, param) in var map
    -- otherwise generate equation
    let exprToCond expr arg = case arg of
            VarExpr var -> do
                maybe <- lookupVarExpr var
                case maybe of
                    Just expr2 ->
                        unify expr expr2
                    Nothing -> do
                        addVarExpr var expr
                        return CypherTrueCond
            _ -> do
                either <- cypherExprFromArg arg
                case either of
                    Left expr2 ->
                        unify expr expr2
                    Right _ -> error "wrong return type"


    cond1 <- foldM (\ cond (expr, arg)  -> do
        newcond <- exprToCond expr arg
        return (cond .&&. newcond)) CypherTrueCond (zip newexprs args)
    return (newcypher <> cwhere cond1)

translateInsertToCypher :: Insert -> TransMonad Cypher
translateInsertToCypher (Insert lits conj) = do
    c <- translateFormulaToCypher conj
    c2 <- mapM translateInsertLitToCypher lits
    substCypher (c <> mconcat c2)

translateInsertLitToCypher :: Lit -> TransMonad Cypher
translateInsertLitToCypher (Lit thesign (Atom (Pred name _) args)) = do
    (CypherBuiltIn builtin, predtablemap, _, _, _) <- get
    --try builtin first
    case lookup name builtin of
        Just builtinpred -> do
            builtinpred thesign args
        Nothing -> case lookup name predtablemap of
            Just (_, ( iexprs, icypher), (dexprs, dcypher)) -> do
                case thesign of
                    Pos -> do
                        instantiate iexprs icypher args
                    Neg -> do
                        instantiate dexprs dcypher args
            Nothing -> error (name ++ " is not defined")

data CypherTrans = CypherTrans CypherBuiltIn CypherPredTableMap

nodev v = GraphNodePattern (NodePattern (Just v) Nothing [])
nodevl v l = GraphNodePattern (NodePattern (Just v) (Just l) [])
nodelp l ps = GraphNodePattern (NodePattern Nothing (Just l) ps)
nodep ps = GraphNodePattern (NodePattern Nothing Nothing ps)
nodel l = GraphNodePattern (NodePattern Nothing (Just l) [])
nodevlp v l ps = GraphNodePattern (NodePattern (Just v) (Just l) ps)
nodevp v ps = GraphNodePattern (NodePattern (Just v) Nothing ps)
edgel n1 l n2 = GraphEdgePattern n1 (NodePattern Nothing (Just l) []) n2
edgevl n1 v l n2 = GraphEdgePattern n1 (NodePattern (Just v) (Just l) []) n2
dot v l = CypherDotExpr (CypherVarExpr v) l
app l e = CypherAppExpr l [CypherVarExpr e]
true = CypherTrueCond
var v = CypherVarExpr v
cnull = CypherNullExpr
match m = Cypher [] m true [] [] []

type CypherEnv = Map CypherVar CypherExpr

instance Convertible MapResultRow CypherEnv where
    safeConvert = Right . foldlWithKey (\map k v  -> insert k (convert v) map) empty

instance Convertible ResultValue CypherExpr where
    safeConvert (IntValue i) = Right (CypherIntConstExpr i)
    safeConvert (StringValue i) = Right (CypherStringConstExpr i)

instance DBConnection conn CypherQuery Cypher => ConnectionDB DBAdapterMonad conn CypherTrans where
    extractDomainSize conn trans varDomainSize thesign (Atom (Pred name _) args) =
        return (if isBuiltIn
            then maxArgDomainSize
            else (case thesign of
                Neg -> maxArgDomainSize
                Pos -> if name `member` predtablemap then fromList [(fv, Bounded 1) | fv <- freeVars args] else empty)) where
            argsDomainSizeMaps = map (exprDomainSizeMap varDomainSize Unbounded) args
            maxArgDomainSize = mmaxs argsDomainSizeMaps
            isBuiltIn = name `member` builtin
            (CypherTrans (CypherBuiltIn builtin) predtablemap) = trans

instance Translate CypherTrans MapResultRow CypherQuery Cypher where
    translateQuery trans query =
        let (CypherTrans builtin predtablemap) = trans in
            evalState (translateQueryToCypher query) (builtin, predtablemap, empty, empty, 0)
    translateQueryWithParams trans query@(Query vars _) env =
        let (CypherTrans builtin predtablemap) = trans
            sql = evalState (translateQueryToCypher query) (builtin, predtablemap, convert env, empty, 0) in
            (sql, vars)
    translateInsert trans query =
        let (CypherTrans builtin predtablemap) = trans in
            evalState (translateInsertToCypher query) (builtin, predtablemap, empty, empty, 0)
    translateInsertWithParams trans query env =
        let (CypherTrans builtin predtablemap) = trans in
            evalState (translateInsertToCypher query) (builtin, predtablemap, convert env, empty, 0)
