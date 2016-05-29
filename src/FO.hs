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
-- import Plugins
import qualified SQL.HDBC.PostgreSQL as PostgreSQL
import qualified Cypher.Neo4j as Neo4j
import qualified InMemory as InMemory
import qualified ElasticSearch.ElasticSearch as ElasticSearch

import Prelude  hiding (lookup)
import Data.ByteString.Lazy.UTF8 (toString)
import Data.Map.Strict (Map, (!), empty, member, insert, foldrWithKey, foldlWithKey, alter, lookup, fromList, toList, unionWith, unionsWith, intersectionWith, elems, delete, singleton, keys, filterWithKey)
import qualified Data.Map.Strict
import Data.List ((\\), intercalate, union, find)
import Control.Monad.Trans.State.Strict (StateT, evalStateT,evalState, get, put, State, runState   )
import Control.Applicative ((<$>), liftA2)
import Data.Convertible.Base
import Control.Monad.Logic
import Control.Monad.Except
import Data.Maybe
import qualified Data.ByteString.Lazy as B
import Text.ParserCombinators.Parsec hiding (State)
import System.Log.Logger
import Data.Tree

-- exec query from dbname

getAllResults2 :: (MonadIO m, ResultRow row) => [Database m row] -> Query -> m [row]
getAllResults2 dbs query = do
    qp <- prepareQuery' dbs  [] [] [] [] query []
    let (_, stream) = execQueryPlan ([], pure mempty) qp
    getAllResultsInStream stream

queryPlan :: (ResultRow row, Monad m) => [Database m row ] -> Query ->  QueryPlan
queryPlan dbs qu@(Query vars formula) =
    let qp = formulaToQueryPlan dbs formula
        qp1 = simplifyQueryPlan qp in
        qp1

queryPlan2 :: (ResultRow row, Monad m) => [Database m row ] -> [Var] -> [Var] -> Query -> QueryPlan2 m row
queryPlan2 dbs vars vars2 qu@(Query vars0 formula) =
    let qp = formulaToQueryPlan dbs formula
        qp1 = simplifyQueryPlan qp
        qp2 = calculateVars vars vars2 qp1 in
        optimizeQueryPlan dbs qp2

prepareQuery' :: (ResultRow row, MonadIO m) => [Database m row ] -> [QueryRewritingRule] -> [InsertRewritingRule] -> [InsertRewritingRule] -> [InsertRewritingRule] -> Query -> [Var] -> m (QueryPlan2 m row)
prepareQuery' dbs qr qr2 ir dr qu0 vars = do
    liftIO $ infoM "QA" ("original query: " ++ show qu0)
    let qu@(Query rvars form) = rewriteQuery vars qr qr2 ir dr qu0
    liftIO $ infoM "QA" ("rewritten query: " ++ show qu)
    let insp = queryPlan dbs qu
    let qp2 = calculateVars vars rvars insp
    effective <- (runExceptT (checkQueryPlan dbs qp2))
    case effective of
        Left (errmsg, formula) -> error (errmsg ++ ". can't find effective literals, try reordering the literals: " ++ show qu ++ show formula)
        Right _ -> do
            let qp3 = optimizeQueryPlan dbs qp2
            let qp3' = addTransaction' qp3
            liftIO $ printQueryPlan qp3'
            qp4 <- prepareTransaction dbs [] qp3'
            prepareQueryPlan dbs (snd qp4)

printQueryPlan qp = do
    infoM "QA" ("query plan:")
    infoM "QA" (drawTree (toTree  qp))

defaultRewritingLimit :: Int
defaultRewritingLimit = 100

type RewritingRuleSets = ([QueryRewritingRule], [InsertRewritingRule], [InsertRewritingRule], [InsertRewritingRule])
data TransDB m row = TransDB String [Database m row ]  [Pred] RewritingRuleSets

rewriteQuery :: [Var] -> [QueryRewritingRule] -> [InsertRewritingRule] -> [InsertRewritingRule] -> [InsertRewritingRule] -> Query -> Query
rewriteQuery ext qr qr2 ir dr (Query vars form) = Query vars (runNew (do
    registerVars (vars `union` freeVars form)
    rewrites defaultRewritingLimit ext   qr qr2 ir dr form))

instance (Monad m, ResultRow row) => DBStatementClose m (QueryPlan2 m row) where
    dbStmtClose qp = return ()

instance (MonadIO m, ResultRow row) => DBStatementExec m row (QueryPlan2 m row) where
    dbStmtExec qp vars rs = snd (execQueryPlan  (vars, rs) qp)

instance (MonadIO m, ResultRow row) => Database_ (TransDB m row) m row (QueryPlan2 m row) where
    dbBegin (TransDB _ dbs  _ _ ) = mapM_ (\(Database db) -> dbBegin db) dbs
    dbCommit (TransDB _ dbs  _ _ ) =     all id <$> mapM (\(Database db) -> dbCommit db) dbs
    dbPrepare (TransDB _ dbs  _ _ ) =     all id <$> mapM (\(Database db) -> dbPrepare db) dbs
    dbRollback (TransDB _ dbs  _ _ ) =    mapM_ (\(Database db) -> dbRollback db) dbs
    getName (TransDB name _ _ _ ) = name
    getPreds (TransDB _ dbs predmap _ ) = predmap
    domainSize (TransDB _ dbs _ _ ) varDomainSize  atom@(Atom pred args) = do
        mps <- mapM (\ (Database db) ->
                if pred `elem` getPreds db then do
                    map2 <- (domainSize db varDomainSize  atom)
                    return [map2]
                else
                    return []) dbs
        return (foldl1 (intersectionWith (+)) (concat mps))
    prepareQuery (TransDB _ dbs _ (qr, qr2, ir, dr) ) qu =
        prepareQuery' dbs  qr qr2 ir dr qu
    -- exec (TransDB _ dbs _ _  _ _ _) qp vars stream = snd (execQueryPlan dbs (vars, stream ) qp)
    supported _ _ _ = True
    supported' _ _ _ = True

data RestrictDB m row stmt where
    RestrictDB :: Database_ db m row stmt => [Pred] -> Map Pred Pred -> db -> RestrictDB m row stmt

instance (MonadIO m, ResultRow row, DBStatementExec m row stmt, DBStatementClose m stmt) => Database_ (RestrictDB m row stmt) m row stmt where
    dbBegin (RestrictDB _ _ db ) = dbBegin db
    dbCommit (RestrictDB _ _ db  ) = dbCommit db
    dbPrepare (RestrictDB _ _ db  ) = dbPrepare db
    dbRollback (RestrictDB _ _ db ) = dbRollback db
    getName (RestrictDB _ _ db ) = "RestrictDB " ++ getName db
    getPreds (RestrictDB preds _ _) = preds
    domainSize (RestrictDB _ pmap db) domainsizemap form = domainSize db domainsizemap (substPred pmap form)
    prepareQuery (RestrictDB _ pmap db) (Query vars form) = prepareQuery db (Query vars (substPred pmap form))
    supported (RestrictDB _ pmap db) form = supported db (substPred pmap form)
    supported' (RestrictDB _ pmap db) form = supported' db (substPred pmap form)
    translateQuery (RestrictDB _ pmap db) (Query vars form) = translateQuery db (Query vars (substPred pmap form))

extractClassicalRules rules = concatMap (\r -> case extractClassicalRule r of
                                                    Left _ -> []
                                                    Right qr -> [qr]) rules

extractClassicalRule :: InsertRewritingRule -> Either ConvertError QueryRewritingRule
extractClassicalRule (InsertRewritingRule a form) = QueryRewritingRule a <$> safeConvert form

getRewriting :: PredMap -> TranslationInfo -> IO (RewritingRuleSets, PredMap, [Export])
getRewriting predmap ps = do
    d0 <- toString <$> B.readFile (rewriting_file_path ps)
    case runParser rulesp (predmap, empty) "" d0 of
        Left err -> error (show err)
        Right ((qr, ir, dr), predmap, exports) -> do
            return ((extractClassicalRules qr, qr, ir, dr), predmap, exports)

dbMap :: Map String (ICATDBConnInfo -> IO [Database DBAdapterMonad MapResultRow])
dbMap = fromList [
    ("SQL/HDBC/PostgreSQL", PostgreSQL.getDB),
    ("Cypher/Neo4j", Neo4j.getDB),
    ("InMemory/EqDB", \ps -> return [Database (InMemory.EqDB (db_name ps !! 0))]),
    ("ElasticSearch/ElasticSearch", ElasticSearch.getDB)
    ];

getDB :: ICATDBConnInfo -> IO [Database DBAdapterMonad MapResultRow]
getDB ps = case lookup (catalog_database_type ps) dbMap of
    Just getDBFunc -> getDBFunc ps
    Nothing -> error ("unimplemented database type " ++ (catalog_database_type ps))


restrictDB :: (MonadIO m, ResultRow row) => [Export] -> Database m row -> ([Pred] , Database m row)
restrictDB exports (Database db) =
    let preds = getPreds db
        predmap = constructPredMap preds
        (preds', preds1, pmap) = foldr (\export (preds', preds1', pmap') -> case export of
                            ExportQualified predname ->
                                if namespace predname == Just (getName db)
                                    then case lookup predname predmap of
                                        Nothing -> error "cannot find predicate"
                                        Just pred1 -> (preds', preds1' ++ [pred1], pmap')
                                    else (preds', preds1', pmap')
                            ExportUnqualified predname ->
                                if namespace predname == Just (getName db)
                                    then case lookup predname predmap of
                                        Nothing -> error "cannot find predicate"
                                        Just pred1 ->
                                            let predname' = UQPredName (name (predName pred1))
                                                pred1' = Pred predname' (predType pred1) in
                                                (preds' ++ [pred1'], preds' ++ [pred1'], insert pred1' pred1 pmap')
                                    else (preds', preds1', pmap')
                            _ ->
                                (preds', preds1', pmap')) (preds, [], empty) exports in
        (preds1, Database (RestrictDB preds' pmap db))

transDB :: String -> TranslationInfo -> IO (TransDB DBAdapterMonad MapResultRow)
transDB name transinfo = do
    dbs <- concat <$> mapM (\(DBTrans ps) -> do
            db <- getDB ps
            return db) (db_plugins transinfo)
    let predmap0 = constructDBPredMap dbs
    -- trace ("preds:\n" ++ intercalate "\n" (map show (elems predmap0))) $ return ()
    (rewriting, predmap0', exports) <- getRewriting predmap0 transinfo
    -- trace (intercalate "\n" (map show (exports))) $ return ()
    let (preds1s, dbs') = unzip (map (restrictDB exports) dbs)
    let predmap1 = unions preds1s
    -- trace (intercalate "\n" (map show (predmap1))) $ return ()
    let predmap2 = foldr (\export preds' -> case export of
                            ExportAdd predname ->
                                case lookup predname predmap0' of
                                    Nothing -> error "cannot find predicate"
                                    Just pred1 -> (preds' ++ [pred1])
                            _ -> preds') [] exports
    return (TransDB name dbs' (predmap1 `union` predmap2 ) rewriting )
