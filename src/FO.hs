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
    stream <- execQuery query
    lift $ lift $ getAllResultsInStream stream

queryPlan :: (ResultRow row, Monad m) => [Database m row] -> Formula -> QueryPlan
queryPlan dbs formula =
    let qp = formulaToQueryPlan dbs formula
        qp2 = simplifyQueryPlan qp in
        optimizeQueryPlan dbs qp2

insertPlan :: (ResultRow row, Monad m) => [Database m row] -> InsertMap -> Insert -> QueryPlan
insertPlan dbs insmap ins =
    let qp = insertToQueryPlan dbs insmap ins
        qp2 = simplifyQueryPlan qp in
        optimizeQueryPlan dbs qp2

execQuery :: (ResultRow row, Monad m) => Query -> DBSession m row (ResultStream m row)
execQuery qu = do
    dbs <- getDBsFromDBSession
    return (execQuery' dbs qu [] (pure mempty))

execQuery' :: (ResultRow row, Monad m) => [Database m row] -> Query -> [Var] -> ResultStream m row -> (ResultStream m row)
execQuery' dbs (Query vars formula) rsvars rs = do
    let qp3 = queryPlan dbs formula
    effective <- lift (runExceptT (checkQueryPlan' dbs qp3))
    case effective of
        Left formula -> error ("can't find effective literals, try reordering the literals: " ++ show formula)
        Right _ ->
            let (vars2, rs') = (execQueryPlan dbs (rsvars, rs)  qp3 ) in
                transformResultStream vars2 vars rs'
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
execInsert :: (ResultRow row, Monad m, MonadIO m) => TheoremProver -> [Input] -> InsertMap -> Insert -> DBSession m row (ResultStream m row)
execInsert  verifier rules insmap qu = do
    dbs <- getDBsFromDBSession
    return (execInsert' dbs verifier rules insmap qu [] (pure mempty))

execInsert' :: (ResultRow row, Monad m, MonadIO m) => [Database m row] -> TheoremProver -> [Input] -> InsertMap -> Insert -> [Var] -> ResultStream m row -> (ResultStream m row)
execInsert' dbs verifier rules insmap qu vars rs = do
    let formulas = map (\(_,_,a) -> a) rules
    let insp = insertPlan dbs insmap qu
    let (vars2, rs2) = execQueryPlan dbs (vars, rs) insp
    rs2
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

instance (MonadIO m, ResultRow row) => Database_ (TransDB m row) m row where
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
    doQuery (TransDB _ dbs _ _  _ (qr, _, _) _) qu = execQuery' dbs (rewriteQuery qr qu)
    doInsert (TransDB _ dbs verifier rules _ (qr, ir, dr) insmap) qu =
        execInsert' dbs verifier rules insmap (rewriteInsert qr ir dr qu)
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




-- example MapDB


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
limitvarsInRow vars row = transform (keys row) vars row

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

    doQuery (MapDB _ _ rows) (Query vars disjs) _ stream  = do
        row2 <- mapDBFilterResults rows  stream disjs
        return (limitvarsInRow vars row2)
    doInsert db (Insert lits disjs) rsvars stream = do
        let (MapDB _ _ rows) = db
        let freevars = freeVars lits
        let add rows1 row = rows1 `union` [row]
        let remove rows1 row = rows1 \\ [row]
        let arg12 (Lit _ (Atom _ [a,b])) = (convert a, convert b)
            arg12 _ = error "wrong number of args"
        rows1 <- lift (getAllResultsInStream (doQuery db (Query freevars disjs) rsvars stream))
        let rows2 = foldl (\rows' lit@(Lit thesign (Atom _ _)) ->
                foldl (\rows'' row1 -> (case thesign of
                        Pos -> add
                        Neg -> remove) rows'' (arg12 (substResultValue row1 lit))) rows' rows1) rows lits
        return empty
    supported (MapDB _ predname _) (Atomic (Atom (Pred p _) _)) | p == predname = True
    supported _ _ = False
    supportedInsert _ _ _ = False


-- update mapdb

data StateMapDB (m :: * -> * )= StateMapDB String String deriving Show

instance (Functor m, Monad m) => Database_ (StateMapDB m) (StateT [(ResultValue, ResultValue)] m) MapResultRow where
    dbBegin _ = return ()
    dbCommit _ = return ()
    dbRollback _ = return ()
    getName (StateMapDB name _) = name
    getPreds (StateMapDB _ predname) = [ Pred predname (PredType ObjectPred [Key "String", Key "String"]) ]
    domainSize db varDomainSize thesign (Atom thepred args)
        | thepred `elem` getPreds db = case thesign of
            Pos -> do
                rows <- get
                return (mmins (map (exprDomainSizeMap varDomainSize (Bounded (length rows))) args))
            Neg ->
                return (mmins (map (exprDomainSizeMap varDomainSize Unbounded) args)) -- this just look up each var from the varDomainSize
    domainSize _ _ _ _ = return empty

    doQuery db (Query vars disjs) _ stream  = do
        rows <- lift get
        row2 <- mapDBFilterResults rows stream disjs
        return (limitvarsInRow vars row2)
    doInsert db (Insert lits disjs) rsvars stream = do
        rows <- lift get
        let freevars = freeVars lits
        let add rows1 row = rows1 `union` [row]
        let remove rows1 row = rows1 \\ [row]
        let arg12 (Lit _ (Atom _ [a,b])) = (convert a, convert b)
            arg12 _ = error "wrong number of args"
        rows1 <- lift (getAllResultsInStream (doQuery db (Query freevars disjs) rsvars stream))
        let rows2 = foldl (\rows' lit@(Lit thesign (Atom _ _)) ->
                foldl (\rows'' row1 -> (case thesign of
                        Pos -> add
                        Neg -> remove) rows'' (arg12 (substResultValue row1 lit))) rows' rows1) rows lits
        lift $ put rows2
        return empty
    supported _ (Atomic _) = True
    supported _ _ = False
    supportedInsert _ (Conjunction []) [_]  = True
    supportedInsert _ _ _ = False




mapDBFilterResults :: (Functor m, Monad m) => [(ResultValue, ResultValue)] -> ResultStream m MapResultRow -> Formula -> ResultStream m MapResultRow
mapDBFilterResults rows  results (Atomic (Atom thepred args)) = do
    resrow <- results
    case args of
        VarExpr var1 : (VarExpr var2 : _)
            | var1 `member` resrow ->
                if var2 `member` resrow then do
                    guard ((resrow ! var1, resrow ! var2) `elem` rows)
                    return mempty
                else
                    listResultStream [singleton var2 (snd x) | x <- rows, fst x == resrow ! var1]

            | var2 `member` resrow ->
                listResultStream [singleton var1 (fst x) | x <- rows, snd x == resrow ! var2]
            | otherwise ->
                listResultStream [fromList [(var1, fst x), (var2, snd x)] | x <- rows]
        VarExpr var1 : StringExpr str2 : _ ->
            if var1 `member` resrow then do
                guard ((resrow ! var1, StringValue str2) `elem` rows)
                return mempty
            else
                listResultStream [singleton var1 (fst x) | x <- rows, snd x == StringValue str2]
        StringExpr str1 : VarExpr var2 : _ ->
            if var2 `member` resrow then do
                guard ((StringValue str1, resrow ! var2) `elem` rows)
                return mempty
            else
                listResultStream [singleton var2 (snd x) | x <- rows, snd x == StringValue str1]
        StringExpr str1 : StringExpr str2 : _ -> do
                guard ((StringValue str1, StringValue str2) `elem` rows)
                return mempty
        _ -> do
            guard False
            return mempty


-- example EqDB

data EqDB (m :: * -> *) = EqDB String
instance (Functor m, Monad m) => Database_ (EqDB m) m MapResultRow where
    dbBegin _ = return ()
    dbCommit _ = return ()
    dbRollback _ = return ()
    getName (EqDB name) = name
    getPreds _ = [ Pred "eq" (PredType EssentialPred [Key "String", Key "String"]) ]
    domainSize db varDomainSize thesign (Atom thepred args)
        | thepred `elem` getPreds db =
            let [d1, d2] = map (exprDomainSizeMap varDomainSize Unbounded) args in
                return (case thesign of
                    Pos -> case toList d1 of
                        [] -> d2
                        [(var1, ds1)] -> case toList d2 of
                            [] -> d1
                            [(var2, ds2)] ->
                                let ds = min ds1 ds2 in
                                    fromList [(var, ds) | var <- [var1, var2]]
                    Neg -> mmin d1 d2)
    domainSize _ _ _ _ = return empty

    doQuery db (Query vars disjs) rsvars stream = (do
        row2 <- filterResults vars filterBy expand1 expand2 expand12 exists excludeBy notExists stream (convertForall Pos disjs)
        return (limitvarsInRow vars row2)) where
            filterBy _ str1 str2 resrow = listResultStream [resrow | str1 == str2]
            excludeBy _ str1 str2 resrow = listResultStream [resrow | str1 /= str2 ]
            exists freevars _ conj resrow = doQuery db (Query freevars conj) rsvars (return resrow)
            notExists freevars _ conj resrow = do
                e <- eos (doQuery db (Query freevars conj) rsvars (return resrow))
                listResultStream (if e
                    then [resrow]
                    else [])
            expand1 _ var1 str2 resrow = listResultStream [insert var1 str2 resrow ]
            expand2 _ str1 var2 resrow = listResultStream [insert var2 str1 resrow ]
            expand12 _ _ _ _ = error "unconstrained eq predicate"
    doInsert _ _ _ _ = do
        error "cannot update equality constaints"
    supported _ (Atomic (Atom (Pred "eq" _) _)) = True
    supported _ (Not (Atomic (Atom (Pred "eq" _) _))) = True
    supported _ _ = False
    supportedInsert _ _ _ = False


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
