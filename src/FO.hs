{-# LANGUAGE TypeFamilies, MultiParamTypeClasses, FunctionalDependencies, ExistentialQuantification, FlexibleInstances, OverloadedStrings,
   RankNTypes, FlexibleContexts, GADTs, DeriveGeneric #-}
module FO where

import ResultStream
import FO.Data
import FO.Domain
import FO.FunSat
import QueryPlan

import Prelude  hiding (lookup)
import Data.Map.Strict (Map, (!), empty, member, insert, foldrWithKey, foldlWithKey, alter, lookup, fromList, toList, unionWith, unionsWith, intersectionWith, elems, delete)
import qualified Data.Map.Strict
import Data.List ((\\), intercalate, union)
import Control.Monad.Trans.State.Strict (StateT, evalStateT,evalState, get, put, State, runState   )
import Control.Applicative ((<$>), liftA2)
import Data.Convertible.Base
import Control.Monad.Logic
import Data.Maybe

-- result value
data ResultValue = StringValue String | IntValue Int | Null deriving (Eq , Show)

instance Convertible ResultValue Expr where
    safeConvert (StringValue s) = Right (StringExpr s)
    safeConvert (IntValue i) = Right (IntExpr i)
    safeConvert _ = Left (ConvertError "" "" "" "")

instance Convertible Expr ResultValue where
    safeConvert (StringExpr s) = Right (StringValue s)
    safeConvert (IntExpr i) = Right (IntValue i)
    safeConvert _ = Left (ConvertError "" "" "" "")

intResultStream :: (Functor m, Monad m) => Int -> ResultStream m MapResultRow
intResultStream i = return (insert (Var "i") (IntValue i) empty)



-- map from predicate name to database names
type PredDBMap = Map String [String]

class SubstituteResultValue a where
    substResultValue :: MapResultRow -> a -> a

instance SubstituteResultValue Expr where
    substResultValue varmap expr@(VarExpr var) = case lookup var varmap of
        Nothing -> expr
        Just res -> convert res

instance SubstituteResultValue Atom where
    substResultValue varmap (Atom thesign theargs) = Atom thesign newargs where
        newargs = map (substResultValue varmap) theargs
instance SubstituteResultValue Lit where
    substResultValue varmap (Lit thesign theatom) = Lit thesign newatom where
        newatom = substResultValue varmap theatom



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
getAllResults :: (Monad m, ResultRow row) => Query -> DBSession m row [row]
getAllResults query = do
    stream <- execQuery query
    lift $ lift $ getAllResultsInStream stream

execPlan :: (ResultRow row, Monad m) => [Database m row] -> Formula -> QueryPlan 
execPlan dbs formula = 
    let qp = formulaToQueryPlan dbs formula
        qp2 = simplifyQueryPlan qp in
        optimizeQueryPlan dbs qp2
        
execQuery :: (ResultRow row, Monad m) => Query -> DBSession m row (ResultStream m row)
execQuery (Query vars formula) = do
    dbs <- getDBsFromDBSession
    let qp3 = execPlan dbs formula
    effective <- lift (lift (checkQueryPlan' dbs qp3))
    case effective of
        Left formula -> error ("can't find effective literals, try reordering the literals: " ++ show formula)
        Right _ ->
            let(vars2, rs) = (execQueryPlan dbs Nothing qp3 ) in
                return (transformResultStream vars2 vars rs)
{-
    
    (do
    let conjuncts = getConjuncts formula
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
                            return (rest1, map1, doQuery db (Query (vars `union` freevars1) (conj effective1)))

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
                            return (rest3, map3, doFilter db (Query (vars `union` freevars3) (conj effective3)) results3)
-}

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
        return (if all (\freevar -> case lookupDomainSize freevar map2 of
            Bounded _ -> True
            Unbounded -> False) freevars
                then (Just (db, map2))
                else Nothing)
    db_mapmaybes <- mapM (isFormulaEffective formula) db_maps
    let db_mapsnew = catMaybes db_mapmaybes
    if null db_mapsnew
        then return (db_maps, effective, candidates)
        else findEffectiveFormulas (db_mapsnew, effective ++ [formula], rest)

combineLits :: [Lit] -> ([Atom] -> [Atom] -> a) -> ([Atom] -> a) -> (Atom -> a) -> a
combineLits lits generateUpdate generateInsert generateDelete = do
    let objpredlits = filter isObjectPredLit lits
    let proppredlits = lits \\ objpredlits
    let (posobjpredatoms, negobjpredatoms) = splitPosNegLits objpredlits
    let (pospropredatoms, negproppredatoms) = splitPosNegLits proppredlits
    case (posobjpredatoms, negobjpredatoms) of
        ([], []) ->  generateUpdate pospropredatoms negproppredatoms   -- update property
        ([posobjatom], []) ->  case negproppredatoms of
            [] -> generateInsert (posobjatom:pospropredatoms)      -- insert
            _ -> error "trying to delete properties of an object to be created"
        ([], [negobjatom]) ->   case (pospropredatoms, negproppredatoms) of
            ([], _) -> generateDelete negobjatom
            _ -> error "tyring to modify propertiese of an object to be deleted" -- delete
        ([posobjatom], [negobjatom]) -> generateUpdate (posobjatom:pospropredatoms) (negobjatom:negproppredatoms) -- update property




-- example MapDB



-- result row
type MapResultRow = Map Var ResultValue

instance ResultRow MapResultRow where
    transform vars vars2 map1 = foldr (\var map2 -> case lookup var map1 of
                                                        Nothing -> insert var Null map1
                                                        Just rv -> insert var rv map1) empty vars2
                                                        
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
                else error ("unconstrained variable " ++ show var1 ++ " or " ++ show var2)
        VarExpr var1 : StringExpr str2 : _ ->
            if var1 `member` resrow
                then excludeBy thepred (resrow ! var1) (StringValue str2) resrow
                else error ("unconstrained variable " ++ show var1 ++ " in " ++ show (Lit Neg (Atom thepred args)))
        StringExpr str1 : VarExpr var2 : _ ->
            if var2 `member` resrow
                then excludeBy thepred (StringValue str1) (resrow ! var2) resrow
                else error ("unconstrained variable " ++ show var2 ++ " in " ++ show (Lit Neg (Atom thepred args)))
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
    supported (MapDB _ predname _) (Atomic (Atom (Pred p _) _)) | p == predname = True
    supported (MapDB _ predname _) (Not (Atomic (Atom (Pred p _) _))) | p == predname = True
    supported _ _ = False





mapDBFilterResults :: (Functor m, Monad m) => MapDB m -> [Var] -> ResultStream m MapResultRow -> Formula -> ResultStream m MapResultRow
mapDBFilterResults db vars results formula =
        filterResults vars filterBy expand1 expand2 expand12 exists excludeBy notExists results (convertForall Pos formula) where
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
        row2 <- filterResults vars filterBy expand1 expand2 expand12 exists excludeBy notExists stream (convertForall Pos disjs)
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
    supported _ (Atomic (Atom (Pred "eq" _) _)) = True
    supported _ (Not (Atomic (Atom (Pred "eq" _) _))) = True
    supported _ _ = False





-- UI

instance Show Query where
    show (Query vars disjs) = show disjs ++ " return " ++ intercalate " " (map show vars)

instance Show Insert where
    show (Insert atoms disjs) = show disjs ++ " insert " ++ intercalate " " (map show atoms)

-- The purpose of this function is to eliminate the situation where the negative literals in the insert clause
-- causes additional constraints to be added to the translated statement. These constraints are cause by the non-key
-- parameters. For example, if we have P(x,y) where x is a key parameter and y is a non key parameter, the mapping of
-- P to a target database contains two parts. One is a condition under which a data structure (such as a row or a subgraph)
-- matches the key argument (thereby fixing the data structure). The other is a data strcuture which stores the
-- information in y. When inserting P(a,b) for arbitrary a and b, P is translated to (schematically):
-- INSERT b WHERE a, whereas when deleting P(a, b), P is translated to: DELETE WHERE a AND b. This create an asymmetry.
-- In the deletion case, if b is added to the condition and subsequently merged to the condition of the whole query, then
-- it may constrain the value of a, thereby constraining other literals in the insertion clause (possibly indirectly)
-- constrained by a. For example, insert ~P(a,b) Q(a,c) /= insert ~P(a,b); insert Q(a,c)
-- To avoid this situation, we require that when a negative literal (~P(a, b)) appears in the insert clause,
-- its positive counterpart (P(a, b)) must be a logical consequence of condition of the statement, given a set of rules.
-- This ensures that this literal (~P(a, b)) doesn't constraint a more than the condition of the statement already does.
-- The set of rules given has to reflect the mapping f, i.e., the mapping must have the following property:
-- If condition & rule -> b in FO then f(condition) -> f(b) in the target database.
validate :: Insert -> Maybe (Lit, CounterExample)
validate (Insert lits cond) =
    if length lits == 1
        then Nothing -- if there is only one literal we don't have to validate
        else validateNegLits cond neg where
            (_, neg) = splitPosNegLits lits
            validateNegLits _ [] = Nothing
            validateNegLits cond (a : as) =
                case valid (cond --> Atomic a) of
                    Nothing -> validateNegLits cond as
                    Just ce -> Just (Lit Neg a, ce)

type ValEnv a = StateT (Map Pred Pred) NewEnv a


instance New Pred Pred where
    new pred@(Pred p pt) = do
      newp <- new (StringWrapper p)
      return (Pred newp pt)

newPred :: Pred -> ValEnv Pred
newPred pred@(Pred p pt) = do
    newpred <- lift $ new pred
    map1 <- get
    put (insert pred newpred map1)
    return newpred

getPredCurrent :: Pred -> ValEnv Pred
getPredCurrent pred = do
    map1 <- get
    return (fromMaybe pred (lookup pred map1))
    
generalize formula = let fv = freeVars formula in foldr Forall formula fv
                         
-- here we implement a restricted version functional dependency
-- given a predicate P with parameters x, and parameters y such that
-- y functionally depend on x, written P(x,y) | x -> y
-- the create action: C insert P(s,t) where C is a condition modifies P -> P'
-- P' is uniquely determined by the following formula:
-- let vc = fv C \\ fv s \\ fv t
--     vs = fv s
--     (fv t \\ fv s should be [] since y functionally dependent on x)
-- forall vs. (exists vc. C & x == s) -> (P'(x, y) <-> y == t)) &
--           ~(exists vc. C & x == s) -> (P'(x, y) <-> P(x, y))
ruleP' :: Formula -> Pred -> [Expr] -> ValEnv Formula
ruleP' cond pred@(Pred p pt@(PredType _ paramtypes)) args = do
    lift $ registerVars (freeVars args)
    lift $ register [p]
    newvars <- lift $ new args
    let s = keyComponents pt args
        x = keyComponents pt newvars
        t = propComponents pt args
        y = propComponents pt newvars
        vc = (freeVars cond `union` freeVars s) \\ freeVars t
        vc1 = (freeVars cond `union` freeVars s)
    oldpred <- getPredCurrent pred
    newpred <- newPred pred
    let newcond = foldr Exists (cond & x === s) vc
    let newcond1 = foldr Exists (cond & x === s) vc1
    return (if not (null (freeVars t \\ freeVars s))
        then error "unbounded vars"
        else generalize ((newcond --> (newpred @@ newvars <--> y === t)) &
                (Not newcond1 --> (newpred @@ newvars <--> oldpred @@ newvars))))

-- given a predicate P with parameters x, and parameters y, such that P(x,y) | x -> y
-- the delete action: C insert ~P(s,t) where C is a condition modifies P -> P'
-- P' is uniquely determined by the following formula:
-- let vc = fv C \\ fv t \\ fv s
--     vs = fv s
-- forall vs. (exists vc. C & P(s, t) & x == s & y == t) -> ~P'(x, y) &
--           ~(exists vc. C & P(s, t) & x == s & y == t) -> (P'(x, y) <-> P(x, y))
ruleQ' :: Formula -> Pred -> [Expr] -> ValEnv Formula
ruleQ' cond pred@(Pred p pt@(PredType _ paramtypes)) args = do
    lift $ registerVars (freeVars args)
    lift $ register [p]
    newvars <- lift $ new args
    let s = keyComponents pt args
        x = keyComponents pt newvars
        t = propComponents pt args
        y = propComponents pt newvars
        vc = freeVars cond `union` freeVars t `union` freeVars s
    oldpred <- getPredCurrent pred
    newpred <- newPred pred
    let newcond = foldr Exists (cond & oldpred @@ args & newvars === args) vc
    return (generalize (((newcond --> Not (newpred @@ newvars)) &
                (Not newcond --> (newpred @@ newvars <--> oldpred @@ newvars)))))

-- the update action: C insert L1 ... Ln is similar to TL's \otimes
rulePQ' :: Formula -> [Lit] -> ValEnv [Formula]
rulePQ' cond lits = mapM insertOneLit lits where
    insertOneLit (Lit Pos (Atom pred args)) =
        ruleP' cond pred args
    insertOneLit (Lit Neg (Atom pred args)) =
        ruleQ' cond pred args

-- inserts and deletes preserves invariants
ruleInsert :: [Formula] -> Formula -> [Lit] -> ValEnv ([Formula], Formula)
ruleInsert rules cond atoms = do
    formulas <- rulePQ' cond atoms
    map1 <- get
    let newrules =  substPred map1 (conj rules)
    return (rules ++ formulas, newrules)

-- insert preserves condition
ruleInsertCond :: [Formula] -> Formula -> [Lit] -> ValEnv ([Formula], Formula)
ruleInsertCond rules cond lits = do
    formulas <- rulePQ' cond lits
    map1 <- get
    let newcond =  substPred map1 cond
    return (rules ++ formulas, cond --> newcond)

validateInsert :: TheoremProver -> [Formula] -> Insert -> IO (Maybe Bool)
validateInsert (TheoremProver a) rules i@(Insert insertlits cond) = (
    do
            let execplan = elems (sortByKey insertlits)
            let steps = take (length execplan - 1) execplan
            result <- validateRules insertlits
            case result of
                Nothing -> return Nothing
                Just False -> return (Just False)
                _ -> validateCond steps) where
                validateCond [] = return (Just True)
                validateCond (s : ss) = do
                    let (axioms, conjecture) = runNew (evalStateT (ruleInsertCond rules cond s) empty)
                    putStrLn ("validating that " ++ show s ++ " doesn't change the condition")
                    mapM (\a -> putStrLn ("axiom: " ++ show a)) axioms
                    putStrLn ("conjecture: " ++ show conjecture)
                    result <- prove a axioms conjecture
                    case result of
                        Nothing -> return Nothing
                        Just False -> return (Just False)
                        _ -> validateCond ss
                validateRules lits = do
                    let (axioms, conjecture) = runNew (evalStateT (ruleInsert rules cond lits) empty)
                    putStrLn ("validating that " ++ show lits ++ " doesn't change the invariant")
                    mapM (\a -> putStrLn ("axiom: " ++ show a)) axioms
                    putStrLn ("conjecture: " ++ show conjecture)
                    prove a axioms conjecture
