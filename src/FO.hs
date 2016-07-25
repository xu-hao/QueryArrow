{-# LANGUAGE TypeFamilies, MultiParamTypeClasses, FunctionalDependencies, ExistentialQuantification, FlexibleInstances, OverloadedStrings,
   RankNTypes, FlexibleContexts, GADTs, DeriveGeneric #-}
module FO where

import ResultStream
import FO.Data
import FO.Domain
import QueryPlan
import Rewriting
import Config
import Parser
import DBQuery
import Utils
import ListUtils
-- import Plugins
import qualified SQL.HDBC.PostgreSQL as PostgreSQL
import qualified SQL.HDBC.CockroachDB as CockroachDB
import qualified Cypher.Neo4j as Neo4j
import qualified InMemory as InMemory
import qualified ElasticSearch.ElasticSearch as ElasticSearch

import Prelude  hiding (lookup)
import Data.ByteString.Lazy.UTF8 (toString)
import Data.Map.Strict (Map, (!), empty, member, insert, foldrWithKey, foldlWithKey, alter, lookup, fromList, toList, unionWith, unionsWith, intersectionWith, elems, delete, singleton, keys, filterWithKey)
import qualified Data.Map.Strict
import Data.List ((\\), intercalate, union, find, intersect)
import Control.Monad.Trans.State.Strict (StateT, evalStateT,evalState, get, put, State, runState   )
import Control.Applicative ((<$>), liftA2)
import Data.Convertible.Base
import Control.Monad.Logic
import Control.Monad.Except
import Control.Monad.Trans.Control
import Data.Maybe
import qualified Data.ByteString.Lazy as B
import Text.ParserCombinators.Parsec hiding (State)
import System.Log.Logger
import Data.Tree
import Data.Namespace.Namespace
import Algebra.SemiBoundedLattice
import Algebra.Lattice
import Data.Set (toAscList, Set, intersection)
import Data.Monoid
-- exec query from dbname

getAllResults2 :: (MonadIO m, MonadBaseControl IO m, ResultRow row, Num (ElemType row), Ord (ElemType row)) => [Database m row] -> MSet Var -> Query -> m [row]
getAllResults2 dbs rvars query = do
    qp <- prepareQuery' dbs rvars query bottom
    let (_, stream) = execQueryPlan ([], pure mempty) qp
    getAllResultsInStream stream

queryPlan :: (ResultRow row, Monad m) => [Database m row ] -> Query ->  QueryPlan
queryPlan dbs qu@(Query  formula) =
    let qp = formulaToQueryPlan dbs formula
        qp1 = simplifyQueryPlan qp in
        qp1

queryPlan2 :: (ResultRow row, Monad m) => [Database m row ] -> Set Var -> MSet Var -> Query -> QueryPlan2 m row
queryPlan2 dbs vars vars2 qu@(Query  formula) =
    let qp = formulaToQueryPlan dbs formula
        qp1 = simplifyQueryPlan qp
        qp2 = calculateVars vars vars2 qp1 in
        optimizeQueryPlan dbs qp2

rewriteQuery' :: (MonadIO m) => [InsertRewritingRule] -> [InsertRewritingRule] -> [InsertRewritingRule] -> MSet Var -> Query -> Set Var -> m Query
rewriteQuery' qr ir dr rvars qu0 vars = do
    liftIO $ infoM "QA" ("original query: " ++ show qu0)
    let qu@(Query form) = rewriteQuery  qr ir dr rvars qu0 vars
    liftIO $ infoM "QA" ("rewritten query: " ++ show qu)
    return qu

prepareQuery' :: (ResultRow row, MonadIO m) => [Database m row ] -> MSet Var -> Query -> Set Var -> m (QueryPlan2 m row)
prepareQuery' dbs rvars qu0 vars = do
    let insp = queryPlan dbs qu0
    let qp2 = calculateVars vars rvars insp
    effective <- runExceptT (checkQueryPlan dbs qp2)
    case effective of
        Left errmsg -> error (errmsg ++ ". can't find effective literals, try reordering the literals: " ++ show qu0)
        Right _ -> do
            let qp3 = optimizeQueryPlan dbs qp2
            -- let qp3' = addTransaction' qp3
            liftIO $ printQueryPlan qp3
            -- qp4 <- prepareTransaction dbs [] qp3'
            prepareQueryPlan dbs qp3

printQueryPlan qp = do
    infoM "QA" ("query plan:")
    infoM "QA" (drawTree (toTree  qp))

defaultRewritingLimit :: Int
defaultRewritingLimit = 100

type RewritingRuleSets = ([InsertRewritingRule],  [InsertRewritingRule], [InsertRewritingRule])

rewriteQuery :: [InsertRewritingRule] -> [InsertRewritingRule] -> [InsertRewritingRule] -> MSet Var -> Query -> Set Var -> Query
rewriteQuery  qr ir dr vars (Query form) ext = Query (runNew (do
    registerVars (toAscList ((case vars of
        Include vars -> vars
        Exclude vars -> vars)  \/ freeVars form))
    rewrites defaultRewritingLimit ext   qr ir dr form))

instance (Monad m, ResultRow row) => DBStatementClose m (QueryPlan2 m row) where
    dbStmtClose qp = return ()

instance (MonadIO m, MonadBaseControl IO m, ResultRow row, Num (ElemType row), Ord (ElemType row)) => DBStatementExec m row (QueryPlan2 m row) where
    dbStmtExec qp vars rs = snd (execQueryPlan  (vars, rs) qp)

data SumDB m row = SumDB String [Database m row]

instance (MonadIO m, MonadBaseControl IO m, ResultRow row, Num (ElemType row), Ord (ElemType row)) => Database_ (SumDB m row) m row (QueryPlan2 m row) where
    dbOpen (SumDB _ dbs   ) = mapM_ (\(Database db) -> dbOpen db) dbs
    dbClose (SumDB _ dbs  ) = mapM_ (\(Database db) -> dbClose db) dbs
    dbBegin (SumDB _ dbs  ) = mapM_ (\(Database db) -> dbBegin db) dbs
    dbCommit (SumDB _ dbs  ) = and <$> mapM (\(Database db) -> dbCommit db) dbs
    dbPrepare (SumDB _ dbs  ) = and <$> mapM (\(Database db) -> dbPrepare db) dbs
    dbRollback (SumDB _ dbs  ) = mapM_ (\(Database db) -> dbRollback db) dbs
    getName (SumDB name _ ) = name
    getPreds (SumDB _ dbs ) = unions (map (\(Database db) -> getPreds db) dbs)
    determinateVars (SumDB _ dbs ) vars  atom@(Atom pred args) = do
        mps <- mapM (\ (Database db) ->
                if pred `elem` getPreds db then do
                    map2 <- determinateVars db vars  atom
                    return [map2]
                else
                    return []) dbs
        return (foldl1 intersection (concat mps))
    prepareQuery (SumDB _ dbs ) vars2 qu vars =
        prepareQuery' dbs (Include vars2) qu vars
    -- exec (TransDB _ dbs _ _  _ _ _) qp vars stream = snd (execQueryPlan dbs (vars, stream ) qp)
    supported _ _ _ = True

data TransDB m row where
    TransDB :: Database_ db m row (QueryPlan2 m row) => String -> db -> [Pred] -> RewritingRuleSets -> TransDB m row

instance (MonadIO m, MonadBaseControl IO m, ResultRow row, Num (ElemType row), Ord (ElemType row)) => Database_ (TransDB m row) m row (QueryPlan2 m row) where
    dbOpen (TransDB _ db  _ _ ) = dbOpen db
    dbClose (TransDB _ db  _ _ ) = dbClose db
    dbBegin (TransDB _ db  _ _ ) = dbBegin db
    dbCommit (TransDB _ db  _ _ ) = dbCommit db
    dbPrepare (TransDB _ db  _ _ ) = dbPrepare db
    dbRollback (TransDB _ db  _ _ ) = dbRollback db
    getName (TransDB name _ _ _ ) = name
    getPreds (TransDB _ _ predmap _ ) = predmap
    determinateVars (TransDB _ db _ _ ) vars  atom = determinateVars db vars atom
    prepareQuery (TransDB _ db _ (qr, ir, dr) ) vars2 qu vars = do
        qu' <- rewriteQuery' qr ir dr (Include vars2) qu vars
        prepareQuery db vars2 qu' vars
    -- exec (TransDB _ dbs _ _  _ _ _) qp vars stream = snd (execQueryPlan dbs (vars, stream ) qp)
    supported _ _ _ = True

getRewriting :: PredMap -> TranslationInfo -> IO (RewritingRuleSets, PredMap, PredMap)
getRewriting predmap ps = do
    d0 <- toString <$> B.readFile (rewriting_file_path ps)
    case runParser rulesp (predmap, mempty, mempty) "" d0 of
        Left err -> error (show err)
        Right ((qr, ir, dr), predmap, exports) ->
            return ((qr, ir, dr), predmap, exports)

dbMap :: Map String (ICATDBConnInfo -> IO [Database DBAdapterMonad MapResultRow])
dbMap = fromList [
    ("SQL/HDBC/PostgreSQL", PostgreSQL.getDB),
    ("SQL/HDBC/CockroachDB", CockroachDB.getDB),
    ("Cypher/Neo4j", Neo4j.getDB),
    ("InMemory/EqDB", \ps -> return [Database (InMemory.EqDB (db_name ps !! 0))]),
    ("InMemory/RegexDB", \ps -> return [Database (InMemory.RegexDB (db_name ps !! 0))]),
    ("InMemory/UtilsDB", \ps -> return [Database (InMemory.UtilsDB (db_name ps !! 0))]),
    ("ElasticSearch/ElasticSearch", ElasticSearch.getDB)
    ];

getDB :: ICATDBConnInfo -> IO [Database DBAdapterMonad MapResultRow]
getDB ps = case lookup (catalog_database_type ps) dbMap of
    Just getDBFunc -> getDBFunc ps
    Nothing -> error ("unimplemented database type " ++ (catalog_database_type ps))


transDB :: String -> TranslationInfo -> IO (TransDB DBAdapterMonad MapResultRow)
transDB name transinfo = do
    dbs <- concat <$> mapM (\(DBTrans ps) -> do
            db <- getDB ps
            return db) (db_plugins transinfo)
    let sumdb = SumDB (name ++ "db") dbs
    let predmap0 = constructDBPredMap (Database sumdb)
    -- trace ("preds:\n" ++ intercalate "\n" (map show (elems predmap0))) $ return ()
    (rewriting, predmap0', exports) <- getRewriting predmap0 transinfo
    let exportmap = allObjects exports
    let (rules0, exportedpreds) = foldrWithKey (\key pred1@(Pred pn predtype@(PredType _ paramTypes)) (rules0', exportedpreds') ->
            if key /= pn
                then
                    let pred0 = Pred key predtype
                        params = map (\i -> VarExpr (Var ("var" ++ show i))) [0..length paramTypes - 1]
                        atom = Atom pred0 params
                        atom1 = Atom pred1 params in
                        (([InsertRewritingRule atom (FAtomic atom1)], [InsertRewritingRule atom (FInsert (Lit Pos atom1))], [InsertRewritingRule atom (FInsert (Lit Neg atom1))]) <> rules0', pred0 : exportedpreds')
                else
                    (rules0', pred1 : exportedpreds')) (([], [], []), []) (allObjects exports)

    -- trace (intercalate "\n" (map show (exports))) $ return ()
    -- trace (intercalate "\n" (map show (predmap1))) $ return ()
    return (TransDB name sumdb exportedpreds (rewriting <> rules0) )
