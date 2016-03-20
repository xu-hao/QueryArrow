{-# LANGUAGE TypeFamilies, MultiParamTypeClasses, FunctionalDependencies, ExistentialQuantification, FlexibleInstances, OverloadedStrings,
   RankNTypes, FlexibleContexts, GADTs, DeriveGeneric #-}
module FO where

import ResultStream
import FO.Data
import FO.Domain
import FO.Parser
import QueryPlan
import Rewriting
import Config
import Parser
import DBQuery
import Utils
-- import Plugins
import SQL.HDBC.PostgreSQL
import FO.E

import Prelude  hiding (lookup)
import Data.Map.Strict (Map, (!), empty, member, insert, foldrWithKey, foldlWithKey, alter, lookup, fromList, toList, unionWith, unionsWith, intersectionWith, elems, delete, singleton, keys, filterWithKey)
import qualified Data.Map.Strict
import Data.List ((\\), intercalate, union)
import Control.Monad.Trans.State.Strict (StateT, evalStateT,evalState, get, put, State, runState   )
import Control.Applicative ((<$>), liftA2)
import Data.Convertible.Base
import Control.Monad.Logic
import Control.Monad.Except
import Data.Maybe
import qualified Data.ByteString.Lazy as B
import qualified Data.ByteString.Lazy.Char8 as B8
import Text.ParserCombinators.Parsec hiding (State)
import Debug.Trace

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
    dbs <- getDBsFromDBSession
    qp <- lift $ lift $ prepareQuery' dbs query []
    let (_, stream) = execQueryPlan ([], pure mempty) qp
    lift $ lift $ getAllResultsInStream stream

queryPlan :: (ResultRow row, Monad m) => [Database m row] -> Formula -> QueryPlan
queryPlan dbs formula =
    let qp = formulaToQueryPlan dbs formula
        qp1 = simplifyQueryPlan qp
        qp2 = optimizeQueryPlan dbs qp1 in
        qp2

insertPlan :: (ResultRow row, Monad m) => [Database m row] -> InsertMap -> Insert -> QueryPlan
insertPlan dbs insmap ins =
    let qp = insertToQueryPlan dbs insmap ins
        qp1 = simplifyQueryPlan qp
        qp2 = optimizeQueryPlan dbs qp1 in
        qp2

{- execQuery :: (ResultRow row, Monad m) => Query -> DBSession m row (ResultStream m row)
execQuery qu = do
    dbs <- getDBsFromDBSession
    return (execQuery' dbs qu [])
-}
prepareQuery' :: (ResultRow row, Monad m) => [Database m row] -> Query -> [Var] -> m (QueryPlan2 m row)
prepareQuery' dbs (Query vars formula) rsvars = do
    let qp3 = queryPlan dbs formula
    effective <- (runExceptT (checkQueryPlan' dbs qp3))
    case effective of
        Left formula -> error ("can't find effective literals, try reordering the literals: " ++ show formula)
        Right _ ->
            prepareQueryPlan dbs (calculateVars rsvars vars qp3)
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
{- execInsert :: (ResultRow row, Monad m, MonadIO m) => TheoremProver -> [Input] -> InsertMap -> Insert -> DBSession m row (ResultStream m row)
execInsert  verifier rules insmap qu = do
    dbs <- getDBsFromDBSession
    return (execInsert' dbs verifier rules insmap qu [])
-}
prepareInsert' :: (ResultRow row, Monad m, MonadIO m) => [Database m row] -> TheoremProver -> [Input] -> InsertMap -> Insert -> [Var] -> m (QueryPlan2 m row)
prepareInsert' dbs verifier rules insmap qu vars = do
    let formulas = map (\(_,_,a) -> a) rules
    let insp = insertPlan dbs insmap qu
    effective <- (runExceptT (checkQueryPlan' dbs insp))
    case effective of
        Left formula -> error ("can't find effective literals, try reordering the literals: " ++ show qu)
        Right _ ->
            prepareQueryPlan dbs (calculateVars vars [] insp)
{-    liftIO $ print "calling verifier"
    vres0 <- liftIO $    validate verifier qu
    case vres0 of
                Just lit -> error ("Deleting a literal not implied by condition " ++ show lit)
                Nothing -> do
                    vres1 <- liftIO $ validateInsert verifier formulas qu
                    case vres1 of
                        Just True -> do
                            let insp = insertPlan dbs insmap qu
                            let (vars, rs) = execQueryPlan dbs (vars, rs) insp
                            rs
                        _ -> error "cannot verify insert statement is correct"
-}


defaultRewritingLimit = 100

type RewritingRuleSets = ([QueryRewritingRule], [InsertRewritingRule], [InsertRewritingRule])
data TransDB m row = TransDB String [Database m row] TheoremProver [Input] [Pred] RewritingRuleSets InsertMap

rewriteQuery :: [QueryRewritingRule] -> Query -> Query
rewriteQuery qr (Query vars form) = Query vars (rewrites defaultRewritingLimit form qr)

rewriteInsert :: [QueryRewritingRule] -> [InsertRewritingRule] -> [InsertRewritingRule] -> Insert -> Insert
rewriteInsert qr ir dr (Insert lits form) = (Insert lits2 (Conjunction (form : forms))) where
    (lits2, forms) = foldMap (\lit@(Lit s a) ->
                            case rewrites1 a (case s of
                                    Pos -> ir
                                    Neg -> dr) of
                                        Nothing -> ([lit], [])
                                        Just (atoms, h) -> (map (Lit s) atoms, [rewrites defaultRewritingLimit h qr]) ) lits

instance (Monad m, ResultRow row) => DBStatement m row (QueryPlan2 m row) where
    dbStmtClose qp = return ()
    dbStmtExec qp vars rs = snd (execQueryPlan  (vars, rs) qp)

instance (MonadIO m, ResultRow row) => Database_ (TransDB m row) m row (QueryPlan2 m row) where
    dbBegin (TransDB _ dbs _ _ _ _ _) = mapM_ (\(Database db) -> dbBegin db) dbs
    dbCommit (TransDB _ dbs _ _ _ _ _) =     mapM_ (\(Database db) -> dbCommit db) dbs
    dbRollback (TransDB _ dbs _ _ _ _ _) =    mapM_ (\(Database db) -> dbRollback db) dbs
    dbBeginCommand (TransDB _ dbs _ _ _ _ _) = mapM_ (\(Database db) -> dbBeginCommand db) dbs
    dbEndCommand (TransDB _ dbs _ _ _ _ _) =     mapM_ (\(Database db) -> dbEndCommand db) dbs
    getName (TransDB name _ _ _ _ _ _) = name
    getPreds (TransDB _ dbs _ _ predmap _ _) = predmap -- unions (map (\(Database db) -> getPreds db) dbs)
    domainSize (TransDB _ dbs _ _ _ _ _) varDomainSize sign atom@(Atom pred args) = do
        mps <- mapM (\ (Database db) ->
                if pred `elem` getPreds db then do
                    map2 <- (domainSize db varDomainSize sign atom)
                    return [map2]
                else
                    return []) dbs
        return (foldl1 (intersectionWith (+)) (concat mps))
    prepareQuery (TransDB _ dbs _ _  _ (qr, _, _) _) qu =
        prepareQuery' dbs (rewriteQuery qr qu)
    prepareInsert (TransDB _ dbs verifier rules _ (qr, ir, dr) insmap) qu =
        prepareInsert' dbs verifier rules insmap (rewriteInsert qr ir dr qu)
    -- exec (TransDB _ dbs _ _  _ _ _) qp vars stream = snd (execQueryPlan dbs (vars, stream ) qp)
    supported _ _ = True
    supportedInsert _ _ _ = True

getRules :: PredMap -> VerificationInfo -> IO  [Input]
getRules predmap ps = do
    d0 <- B8.unpack <$> B.readFile (rule_file_path ps)
    let d = parseTPTP predmap d0
    return d

getRewriting :: PredMap -> TranslationInfo -> IO RewritingRuleSets
getRewriting predmap ps = do
    d0 <- B8.unpack <$> B.readFile (rewriting_file_path ps)
    case runParser rulesp predmap "" d0 of
        Left err -> error (show err)
        Right rules -> return rules

transDB :: String -> TranslationInfo -> IO (TransDB DBAdapterMonad MapResultRow)
transDB name transinfo = do
    dbs <- concat <$> mapM (\(DBTrans ps) -> do
            db <- getDB ps
            return db) (db_plugins transinfo)
    let veriinfo = verifier transinfo
    let predmap0 = constructPredMap dbs
    let hiding = hide_predicate transinfo
    let predmap1 = filterWithKey (\k _ -> not (k `elem` hiding)) predmap0
    let add = add_predicate transinfo
    let predmap2 = foldMap (\(PredInfo n arity) -> singleton n (Pred n (PredType ObjectPred (replicate arity (Key "Any"))))) add
    tp <- getVerifier veriinfo
    rules <- getRules predmap0 veriinfo
    rewriting <- getRewriting (predmap0 `Data.Map.Strict.union` predmap2) transinfo
    let insmap = fromList (map (\(InsMapInfo k i d) -> (predmap0 ! k, ([i], d))) (insert_map transinfo))
    return (TransDB name dbs tp rules (elems (predmap1 `Data.Map.Strict.union` predmap2)) rewriting insmap)





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
validate :: TheoremProver -> Insert -> IO (Maybe Lit)
validate (TheoremProver a) (Insert lits cond) =
    if length lits == 1
        then return Nothing -- if there is only one literal we don't have to validate
        else validateNegLits cond neg where
            (_, neg) = splitPosNegLits lits
            validateNegLits _ [] = return Nothing
            validateNegLits cond (a1 : as) = do
                p <- prove a [cond] (Atomic a1)
                case p of
                    Just True -> validateNegLits cond as
                    _ -> return (Just (Lit Neg a1))

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
                _ -> return (Just True)) where
{-                validateCond [] =
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
-}
                validateRules lits = do
                    let (axioms, conjecture) = runNew (evalStateT (ruleInsert rules cond lits) empty)
                    putStrLn ("validating that " ++ show lits ++ " doesn't change the invariant")
                    mapM (\a -> putStrLn ("axiom: " ++ show a)) axioms
                    putStrLn ("conjecture: " ++ show conjecture)
                    prove a axioms conjecture
