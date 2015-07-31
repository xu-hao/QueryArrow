{-# LANGUAGE TypeFamilies, MultiParamTypeClasses, FunctionalDependencies, ExistentialQuantification, FlexibleInstances, OverloadedStrings,
   RankNTypes, FlexibleContexts, GADTs #-}
module FO where

import ResultStream

import Prelude  hiding (lookup)
import Data.Map.Strict (Map, (!), empty, member, insert, foldrWithKey, alter, lookup, fromList, toList, unionWith, unionsWith, intersectionWith, delete)
import Data.Maybe (catMaybes)
import Data.List ((\\), intercalate, union)
import Control.Monad (liftM2,foldM ,filterM, liftM, join)
import Control.Monad.Trans.State.Strict (StateT, evalStateT, get, put   )
import Control.Monad.Trans.Class (lift,)
import Data.Convertible.Base

-- variable types
type Type = String

data PredKind = ObjectPred | PropertyPred deriving Eq

-- predicate types
data PredType = PredType PredKind [ParamType] deriving Eq

data ParamType = Key Type
               | Property Type deriving Eq

-- predicate
data Pred = Pred { predName :: String, predType :: PredType} deriving Eq

-- variables
type Var = String

-- expression
data Expr = VarExpr Var | IntExpr Int | StringExpr String | PatternExpr String deriving (Eq, Ord)

-- result value
data ResultValue = StringValue String | IntValue Int deriving (Eq , Show)

instance Convertible ResultValue Expr where
    safeConvert (StringValue s) = Right (StringExpr s)
    safeConvert (IntValue i) = Right (IntExpr i)

instance Convertible Expr ResultValue where
    safeConvert (StringExpr s) = Right (StringValue s)
    safeConvert (IntExpr i) = Right (IntValue i)
    safeConvert _ = Left (ConvertError "" "" "" "")
-- atoms
data Atom = Atom { atomPred :: Pred, atomArgs :: [Expr] } deriving Eq

-- sign of literal
data Sign = Pos | Neg deriving Eq

-- literals
data Lit = Lit { sign :: Sign,  atom :: Atom } deriving Eq

data Formula = Atomic Atom
             | Disjunction [Formula]
             | Conjunction [Formula]
             | Not Formula
             | Exists { boundvar :: Var, formula :: Formula } deriving Eq
-- query
data Query = Query { select :: [Var], cond :: Formula }
data Insert = Insert { ilits :: [Lit], icond :: Formula }

intResultStream :: (Functor m, Monad m) => Int -> ResultStream m MapResultRow
intResultStream i = return (insert "i" (IntValue i) empty)

-- database
class Database_ db m row | db -> m row where
    dbBegin :: db -> m ()
    dbCommit :: db -> m ()
    dbRollback :: db -> m ()
    getName :: db -> String
    getPreds :: db -> [Pred]
    -- domainSize function is a function from arguments to domain size
    -- it is used to compute the optimal query plan
    domainSize :: db -> DomainSizeMap -> DomainSizeFunction m Atom
    doQuery :: db -> Query -> ResultStream m row
    -- filter takes a result stream, a list of input vars, a query, and generate a result stream
    doFilter :: db -> Query -> ResultStream m row -> ResultStream m row
    doInsert :: db -> Insert -> m [Int]

-- https://wiki.haskell.org/Existential_type#Dynamic_dispatch_mechanism_of_OOP
data Database m  row = forall db. Database_ db m  row => Database { unDatabase :: db }

-- predicate map
type PredMap = Map String Pred

-- map from predicate name to database names
type PredDBMap = Map String [String]

unique :: [a] -> a
unique as = if length as /= 1
        then error "More than one primary parameters and nonzero secondary parameters"
        else head as

allTrue :: forall a. (a -> Bool) -> [a] -> Bool
allTrue f = foldr ((&&) . f) True

someTrue :: forall a. (a -> Bool) -> [a] -> Bool
someTrue f = foldr ((||) . f) False

isConst :: Expr -> Bool
isConst (VarExpr _) = False
isConst _ = True

-- free variables

class FreeVars a where
    freeVars :: a -> [Var]

class SubstituteResultValue a where
    substResultValue :: MapResultRow -> a -> a

instance FreeVars Expr where
    freeVars (VarExpr var) = [var]
    freeVars _ = []

instance SubstituteResultValue Expr where
    substResultValue varmap expr@(VarExpr var) = case lookup var varmap of
        Nothing -> expr
        Just res -> convert res

instance FreeVars Atom where
    freeVars (Atom _ theargs) = foldl (\fvs expr ->
        fvs `union` freeVars expr) [] theargs

instance SubstituteResultValue Atom where
    substResultValue varmap (Atom thesign theargs) = Atom thesign newargs where
        newargs = map (substResultValue varmap) theargs


instance FreeVars Lit where
    freeVars (Lit _ theatom) = freeVars theatom

instance SubstituteResultValue Lit where
    substResultValue varmap (Lit thesign theatom) = Lit thesign newatom where
        newatom = substResultValue varmap theatom
instance FreeVars [Expr] where
    freeVars = unions. map freeVars

instance FreeVars [Lit] where
    freeVars = unions . map freeVars

instance FreeVars [Formula] where
    freeVars = unions . map freeVars

instance FreeVars Formula where
    freeVars (Atomic atom1) = freeVars atom1
    freeVars (Conjunction formulas) =
        unions (map freeVars formulas)
    freeVars (Disjunction formulas) =
        unions (map freeVars formulas)
    freeVars (Not formula1) =
        freeVars formula1
    freeVars (Exists var formula1) =
        freeVars formula1 \\ [var]


-- Int must be nonnegative
data DomainSize = Unbounded
                | Bounded Int deriving Show

dmin :: DomainSize -> DomainSize -> DomainSize
dmin Unbounded b = b
dmin a Unbounded = a
dmin (Bounded a) (Bounded b) = Bounded (min a b)

dmax :: DomainSize -> DomainSize -> DomainSize
dmax Unbounded _ = Unbounded
dmax _ Unbounded = Unbounded
dmax (Bounded a) (Bounded b) = Bounded (max a b)

-- dmul :: DomainSize -> DomainSize -> DomainSize
-- dmul (Infinite a) (Infinite b) = liftM2 (*)

dmaxs :: [DomainSize] -> DomainSize
dmaxs = foldl dmax (Bounded 0)

exprDomainSizeMap :: DomainSizeMap -> DomainSize -> Expr -> DomainSizeMap
exprDomainSizeMap varDomainSize maxval expr = case expr of
    VarExpr var -> insert var (dmin (lookupDomainSize var varDomainSize) maxval) empty
    _ -> empty

-- estimate domain size (upper bound)
type DomainSizeMap = Map Var DomainSize

lookupDomainSize :: Var -> DomainSizeMap -> DomainSize
lookupDomainSize var map1 = case lookup var map1 of
    Nothing -> Unbounded
    Just a -> a

-- anything DomainSize value is less than or equal to Unbounded so we can use union and intersection operations on DomainSizeMaps
mmins :: [DomainSizeMap] -> DomainSizeMap
mmins = unionsWith dmin

mmin :: DomainSizeMap -> DomainSizeMap -> DomainSizeMap
mmin = unionWith dmin
mmaxs :: [DomainSizeMap] -> DomainSizeMap
mmaxs = foldl1 (intersectionWith dmax) -- must have at least one

type DomainSizeFunction m a = Sign -> a -> m DomainSizeMap

class DeterminedVars a where
    determinedVars :: Monad m => DomainSizeFunction m Atom -> Sign -> a -> m DomainSizeMap

instance DeterminedVars Atom where
    determinedVars dsp = dsp

instance DeterminedVars Formula where
    determinedVars dsp sign (Atomic atom) = determinedVars dsp sign atom
    determinedVars dsp Pos (Conjunction formulas) = do
        maps <- mapM (determinedVars dsp Pos) formulas
        return (mmins maps)
    determinedVars dsp Neg (Conjunction formulas) = do
        maps <- mapM (determinedVars dsp Neg) formulas
        return (mmaxs maps)
    determinedVars dsp Pos (Disjunction formulas) = do
        maps <- mapM (determinedVars dsp Pos) formulas
        return (mmins maps)
    determinedVars dsp Neg (Disjunction formulas) = do
        maps <- mapM (determinedVars dsp Neg) formulas
        return (mmaxs maps)
    determinedVars dsp Pos (Not formula) =
        determinedVars dsp Neg formula
    determinedVars dsp Neg (Not formula) =
        determinedVars dsp Pos formula
    determinedVars dsp Pos (Exists var formula) = do
        map1 <- determinedVars dsp Pos formula
        return (delete var map1)
    determinedVars dsp Neg (Exists var formula) = do
        return empty -- this may be an underestimation, need look into this more

intersects :: Eq a => [[a]] -> [a]
intersects [] = error "can't intersect zero lists"
intersects (hd : tl) = foldl (\ as bs -> [ x | x <- as, x `elem` bs ]) hd tl

unions :: Eq a => [[a]] -> [a]
unions = foldl union []

type Report = [[Formula]]

type DBSession m row a = StateT [Database m row] (StateT Report m) a

dbWithSession :: (Monad m) => [Database m row] -> DBSession m row a -> m a
dbWithSession dbs action = do
    mapM_ (\(Database db) -> dbBegin db) dbs
    r <- evalStateT (evalStateT action dbs) []
    mapM_ (\(Database db) -> dbCommit db) dbs
    return r


getDBsFromDBSession :: (Monad m) => DBSession m row [Database m row]
getDBsFromDBSession = get

getReportFromDBSession :: (Monad m) => DBSession m row Report
getReportFromDBSession = lift get

writeReportToDBSession :: (Monad m) => Report -> DBSession m row ()
writeReportToDBSession report = lift $ do
    reports <- get
    put (reports ++ report)

-- exec query from dbname
getAllResults :: (Monad m, Functor m) => Query -> DBSession m row [row]
getAllResults query = do
    stream <- execQuery query
    lift $ lift $ getAllResultsInStream stream

execQuery :: (Monad m) => Query -> DBSession m row (ResultStream m row)
execQuery (Query vars formula) = (do
    let conjuncts = getConjuncts formula
    dbs <- getDBsFromDBSession
    (rest, mapnew, results) <- doTheQuery dbs conjuncts
    doTheFilters dbs mapnew rest results)  where
        doTheQuery dbs1 conjucts1 = do
            let db_maps1 = [(db1, empty)  | db1 <- dbs1]
            (db_maps1', effective1, rest1) <- lift . lift $ findEffectiveFormulas (db_maps1, [], conjucts1)
            if null effective1
                    then error ("can't find effective literals, try reordering the literals: " ++ show conjucts1)
                    else case head db_maps1' of
                        (Database db, map1) -> do
                            writeReportToDBSession [effective1]
                            let freevars1 = freeVars effective1
                            return (rest1, map1, doQuery db (Query (vars `union` freevars1) (Conjunction effective1)))

        -- doTheFilters :: [Database m row] -> [Disjunction] -> [Var] -> ResultStream m seed row -> DBSession m row (ResultStream m seed row)
        doTheFilters _ _ [] results2 = return results2
        doTheFilters dbs2 map2 formulas2 results2 = do
            let db_maps2 = [(db2, map2 ) | db2 <- dbs2]
            (rest2, map2new, results2new) <- doTheFilter db_maps2 formulas2 results2
            doTheFilters dbs2 map2new rest2 results2new

        doTheFilter db_maps3 formulas3 results3 =  do
            (db_maps3', effective3, rest3) <- lift . lift $ findEffectiveFormulas (db_maps3, [], formulas3)
            if null effective3
                    then error ("can't find effective literals, try reordering the literals 2: " ++ show (map snd db_maps3) ++ show formulas3)
                    else case head db_maps3' of
                        (Database db, map3) -> do
                            writeReportToDBSession [effective3]
                            let freevars3 = freeVars effective3
                            return (rest3, map3, doFilter db (Query (vars `union` freevars3) (Conjunction effective3)) results3)


constructPredMap :: [Database m  row] -> PredMap
constructPredMap = foldr addPredFromDBToMap empty where
    addPredFromDBToMap (Database db) predmap = foldr addPredToMap predmap preds where
        addPredToMap thepred = insert (predName thepred) thepred
        preds = getPreds db

-- construct map from predicates to db names
constructPredDBMap :: [Database m  row] -> PredDBMap
constructPredDBMap = foldr addPredFromDBToMap empty where
    addPredFromDBToMap (Database db) predmap = foldr addPredToMap predmap preds where
        dbname = getName db
        addPredToMap thepred = alter alterValue (predName thepred) where
            alterValue Nothing = Just [dbname]
            alterValue (Just dbnames) = Just (dbname : dbnames)
        preds = getPreds db

-- get the top level conjucts
getConjuncts :: Formula -> [Formula]
getConjuncts (Conjunction conjuncts) = conjuncts
getConjuncts a = [a]

-- a formula is called "effective" only when it has a finite domain size.
-- a variable is called "determined" only when it has a finite domain size.
-- the first stage is a query and subsequent stages are filters
-- each stage T is associated with a database D
-- a set S of literals is assigned a stage T. Denote by A -> T -> D. The assignment must satisfy the following
-- for each L in S
-- (1) the predicate in L is provided by D and
-- (2) all variable in L are determined by S and D or previous stages
-- a variable is determined by S and D, iff
-- there is a literal L in S such that
-- (1) x in fv(L) and
-- (2) L has a finite domain size in D given the variables determined in previous stages

-- given
-- a list of variables that has already been determined
-- a list of databases
-- a list of effective literals
-- a list of literal
-- find the longest prefix of the literal list the literals in which become effective in at least one databases
findEffectiveFormulas :: (Monad m ) => ([(Database m  row, DomainSizeMap)], [Formula], [Formula]) -> m ([(Database m  row, DomainSizeMap)], [Formula], [Formula])
findEffectiveFormulas (db_maps, effective, []) = return (db_maps, effective, [])
findEffectiveFormulas (db_maps, effective, candidates@(formula : rest)) = do
    let isFormulaEffective formula' (db@(Database db_), map1) = do
        map1' <- determinedVars (domainSize db_ map1) Pos formula'
        let map2 = mmin map1 map1'
        let freevars = freeVars formula'
        return (if allTrue (\freevar -> case lookupDomainSize freevar map2 of
            Bounded _ -> True
            Unbounded -> False) freevars
                then (Just (db, map2))
                else Nothing)
    db_mapmaybes <- mapM (isFormulaEffective formula) db_maps
    let db_mapsnew = catMaybes db_mapmaybes
    if null db_mapsnew
        then return (db_maps, effective, candidates)
        else findEffectiveFormulas (db_mapsnew, effective ++ [formula], rest)

splitPosNegLits :: [Lit] -> ([Atom], [Atom])
splitPosNegLits = foldr (\ (Lit thesign theatom) (pos, neg) -> case thesign of
    Pos -> (theatom : pos, neg)
    Neg -> (pos, theatom : neg)) ([],[])

pushNegations :: Sign -> Formula -> Formula
pushNegations Pos (Conjunction formulas) = Conjunction (map (pushNegations Pos) formulas)
pushNegations Pos (Disjunction formulas) = Disjunction (map (pushNegations Pos) formulas)
pushNegations Pos (Not formula) = pushNegations Neg formula
pushNegations Pos (Exists var formula) = Exists var (pushNegations Pos formula)
pushNegations Pos a@(Atomic _ ) = a
pushNegations Neg (Conjunction formulas) = Disjunction (map (pushNegations Neg) formulas)
pushNegations Neg (Disjunction formulas) = Conjunction (map (pushNegations Neg) formulas)
pushNegations Neg (Not formula) = pushNegations Pos formula
pushNegations Neg (Exists var formula) = Not (Exists var (pushNegations Pos formula))
pushNegations Neg a@(Atomic _) = Not a

pushNegations' = pushNegations Pos

-- example MapDB



-- result row
type MapResultRow = Map Var ResultValue

filterResults :: (Functor m, Monad m) => [Var] -> (Pred-> ResultValue -> ResultValue -> MapResultRow -> ResultStream m MapResultRow)
    -> (Pred-> Var -> ResultValue -> MapResultRow -> ResultStream m MapResultRow)
    -> (Pred-> ResultValue -> Var -> MapResultRow -> ResultStream m MapResultRow)
    -> (Pred-> Var -> Var -> MapResultRow -> ResultStream m MapResultRow)
    -> ([Var]-> Var -> Formula -> MapResultRow -> ResultStream m MapResultRow)
    -> (Pred-> ResultValue -> ResultValue -> MapResultRow -> ResultStream m MapResultRow)
    -> ([Var]-> Var -> Formula -> MapResultRow -> ResultStream m MapResultRow)
    -> ResultStream m MapResultRow
    -> Formula
    -> ResultStream m MapResultRow
filterResults freevars filterBy expand1 expand2 expand12 exists excludeBy notExists results (Conjunction formulas) =
    foldl (filterResults freevars filterBy expand1 expand2 expand12 exists excludeBy notExists)  results formulas

filterResults freevars filterBy expand1 expand2 expand12 exists excludeBy notExists results (Disjunction formulas) =
    join ( listResultStream ( map (filterResults freevars filterBy expand1 expand2 expand12 exists excludeBy notExists results) formulas))

filterResults _ filterBy expand1 expand2 expand12 _ _ _ results (Atomic (Atom thepred args)) = do
    resrow <- results
    case args of
        VarExpr var1 : (VarExpr var2 : _)
            | var1 `member` resrow ->
                if var2 `member` resrow then
                    filterBy thepred (resrow ! var1) (resrow ! var2) resrow else
                    expand2 thepred (resrow ! var1) var2 resrow
            | var2 `member` resrow -> expand1 thepred var1 (resrow ! var2) resrow
            | otherwise -> expand12 thepred var1 var2 resrow
        VarExpr var1 : StringExpr str2 : _ ->
            if var1 `member` resrow
                then filterBy thepred (resrow ! var1) (StringValue str2) resrow
                else expand1 thepred var1 (StringValue str2) resrow
        StringExpr str1 : VarExpr var2 : _ ->
            if var2 `member` resrow
                then filterBy thepred (StringValue str1) (resrow ! var2) resrow
                else expand2 thepred (StringValue str1) var2 resrow
        StringExpr str1 : StringExpr str2 : _ ->
            filterBy thepred (StringValue str1) (StringValue str2) resrow
        _ -> listResultStream []

filterResults _ _ _ _ _ _ excludeBy _ results (Not (Atomic (Atom thepred args))) = do
    resrow <- results
    case args of
        VarExpr var1 : VarExpr var2 : _ ->
            if var1 `member` resrow && var2 `member` resrow
                then excludeBy thepred (resrow ! var1) (resrow ! var2) resrow
                else error ("unconstrained variable " ++ var1 ++ " or " ++ var2)
        VarExpr var1 : StringExpr str2 : _ ->
            if var1 `member` resrow
                then excludeBy thepred (resrow ! var1) (StringValue str2) resrow
                else error ("unconstrained variable " ++ var1 ++ " in " ++ show (Lit Neg (Atom thepred args)))
        StringExpr str1 : VarExpr var2 : _ ->
            if var2 `member` resrow
                then excludeBy thepred (StringValue str1) (resrow ! var2) resrow
                else error ("unconstrained variable " ++ var2 ++ " in " ++ show (Lit Neg (Atom thepred args)))
        StringExpr str1 : StringExpr str2 : _ ->
            excludeBy thepred (StringValue str1) (StringValue str2) resrow
        _ -> listResultStream [resrow]

filterResults freevars2 _ _ _ _ exists _ _ results (Exists var conj) =  do
    row <- results
    exists (freevars2 `union` (freeVars conj \\ [var])) var conj row

filterResults freevars2 _ _ _ _ _ _ notExists results (Not (Exists var conj)) = do
    row <- results
    notExists freevars2 var conj row

filterResults _ _ _ _ _ _ _ _ _ (Not _) = error "not not pushed to atoms or existential quantifications"

limitvarsInRow :: [Var] -> MapResultRow -> MapResultRow
limitvarsInRow vars = foldrWithKey (\ var val newresrow -> if var `elem` vars then insert var val newresrow else newresrow) empty

data MapDB (m :: * -> * )= MapDB String String [(ResultValue, ResultValue)] deriving Show

instance (Functor m, Monad m) => Database_ (MapDB m) m MapResultRow where
    dbBegin _ = return ()
    dbCommit _ = return ()
    dbRollback _ = return ()
    getName (MapDB name _ _) = name
    getPreds (MapDB _ predname _) = [ Pred predname (PredType ObjectPred [Key "String", Key "String"]) ]
    domainSize db varDomainSize thesign (Atom thepred args)
        | thepred `elem` getPreds db = return (case thesign of
            Pos -> case db of
                MapDB _ _ rows ->
                    mmins (map (exprDomainSizeMap varDomainSize (Bounded (length rows))) args)
            Neg ->
                mmins (map (exprDomainSizeMap varDomainSize Unbounded) args)) -- this just look up each var from the varDomainSize
    domainSize _ _ _ _ = return empty

    doQuery db query = doFilter db query (return empty)
    doFilter db (Query vars disjs) stream  = do
        row2 <- mapDBFilterResults db vars stream disjs
        return (limitvarsInRow vars row2)
    doInsert db (Insert lits disjs) = do
        let (MapDB _ _ rows) = db
        let freevars = freeVars lits
        let add rows1 row = rows1 `union` [row]
        let remove rows1 row = rows1 \\ [row]
        let arg12 (Lit _ (Atom _ [a,b])) = (convert a, convert b)
            arg12 _ = error "wrong number of args"
        rows1 <- getAllResultsInStream (doQuery db (Query freevars disjs))
        let rows2 = foldl (\rows' lit@(Lit thesign (Atom _ _)) ->
                foldl (\rows'' row1 -> (case thesign of
                        Pos -> add
                        Neg -> remove) rows'' (arg12 (substResultValue row1 lit))) rows' rows1) rows lits
        return [0]



mapDBFilterResults :: (Functor m, Monad m) => MapDB m -> [Var] -> ResultStream m MapResultRow -> Formula -> ResultStream m MapResultRow
mapDBFilterResults db vars results formula =
        filterResults vars filterBy expand1 expand2 expand12 exists excludeBy notExists results (pushNegations' formula) where
            (MapDB _ _ rows) = db
            filterBy _ str1 str2 resrow = listResultStream [resrow | (str1, str2) `elem` rows]
            excludeBy _ str1 str2 resrow = listResultStream [resrow | (str1, str2) `notElem` rows ]
            exists freevars _ conj resrow = doFilter db (Query freevars conj) (return resrow)
            notExists freevars _ conj resrow = do
                e <- eos (doFilter db (Query freevars conj) (return (resrow)))
                listResultStream (if e then [resrow] else [])
            expand1 _ var1 str2 resrow = listResultStream [insert var1 (fst row) resrow | row <- rows, snd row == str2 ]
            expand2 _ str1 var2 resrow = listResultStream [insert var2 (snd row) resrow | row <- rows, fst row == str1 ]
            expand12 _ var1 var2 resrow = listResultStream [ insert var1 (fst row) (insert var2 (snd row) resrow) | row <- rows ]


-- example EqDB

data EqDB (m :: * -> *) = EqDB String
instance (Functor m, Monad m) => Database_ (EqDB m) m MapResultRow where
    dbBegin _ = return ()
    dbCommit _ = return ()
    dbRollback _ = return ()
    getName (EqDB name) = name
    getPreds _ = [ Pred "eq" (PredType ObjectPred [Key "String", Key "String"]) ]
    domainSize db varDomainSize thesign (Atom thepred args)
        | thepred `elem` getPreds db =
            let [d1, d2] = map (exprDomainSizeMap varDomainSize Unbounded) args in
                return (case thesign of
                    Pos -> case toList d1 of
                        [] -> d2
                        [(var1, ds1)] -> case toList d2 of
                            [] -> d1
                            [(var2, ds2)] ->
                                let ds = dmin ds1 ds2 in
                                    fromList [(var, ds) | var <- [var1, var2]]
                    Neg -> mmin d1 d2)
    domainSize _ _ _ _ = return empty

    doQuery db query = doFilter db query (return empty)
    doFilter db (Query vars disjs) stream = (do
        row2 <- filterResults vars filterBy expand1 expand2 expand12 exists excludeBy notExists stream (pushNegations' disjs)
        return (limitvarsInRow vars row2)) where
            filterBy _ str1 str2 resrow = listResultStream [resrow | str1 == str2]
            excludeBy _ str1 str2 resrow = listResultStream [resrow | str1 /= str2 ]
            exists freevars _ conj resrow = doFilter db (Query freevars conj) (return resrow)
            notExists freevars _ conj resrow = do
                e <- eos (doFilter db (Query freevars conj) (return resrow))
                listResultStream (if e
                    then [resrow]
                    else [])
            expand1 _ var1 str2 resrow = listResultStream [insert var1 str2 resrow ]
            expand2 _ str1 var2 resrow = listResultStream [insert var2 str1 resrow ]
            expand12 _ _ _ _ = error "unconstrained eq predicate"
    doInsert _ _ = do
        error "cannot update equality constaints"



-- UI

instance Show Query where
    show (Query vars disjs) = show disjs ++ " return " ++ intercalate " " vars

instance Show Insert where
    show (Insert atoms disjs) = show disjs ++ " insert " ++ intercalate " " (map show atoms)

instance Show Pred where
    show (Pred name t) = name ++ show t

instance Show PredType where
    show (PredType ObjectPred types) = "OP" ++ "(" ++ intercalate "," (map show types) ++ ")"
    show (PredType PropertyPred types) = "PP" ++ "(" ++ intercalate "," (map show types) ++ ")"

instance Show ParamType where
    show (Key type1) = "KEY " ++ type1
    show (Property type1) = "PROP " ++ type1

instance Show Atom where
    show (Atom (Pred name _) args) = name ++ "(" ++ intercalate "," (map show args) ++ ")"

instance Show Expr where
    show (VarExpr var) = var
    show (IntExpr i) = show i
    show (StringExpr s) = show s
    show (PatternExpr p) = show p

instance Show Lit where
    show (Lit thesign theatom) = case thesign of
        Pos -> show theatom
        Neg -> "~" ++ show theatom

instance Show Formula where
    show (Conjunction formulas) = "(" ++ intercalate " " (map show formulas) ++ ")"
    show (Disjunction formulas) = "(" ++ intercalate "|" (map show formulas) ++ ")"
    show (Exists var formula) = "(exists " ++ var ++ "." ++ show formula ++ ")"
    show (Atomic a) = show a
    show (Not formula) = "~" ++ show formula
