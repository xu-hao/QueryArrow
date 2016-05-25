{-# LANGUAGE TypeFamilies, MultiParamTypeClasses, FunctionalDependencies, ExistentialQuantification, FlexibleInstances, OverloadedStrings,
   RankNTypes, FlexibleContexts, GADTs, DeriveGeneric #-}
module FO where

import ResultStream
import FO.Data
import FO.Domain
import Prover.Parser
import QueryPlan
import Rewriting
import Config
import Parser
import DBQuery
import Utils
import Prover.Prover
-- import Plugins
import qualified SQL.HDBC.PostgreSQL as PostgreSQL
import qualified Cypher.Neo4j as Neo4j
-- import Prover.E

import Prelude  hiding (lookup)
import Data.ByteString.Lazy.UTF8 (toString)
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

-- exec query from dbname

getAllResults2 :: (Monad m, ResultRow row) => [Database m row] -> Query -> m [row]
getAllResults2 dbs query = do
    qp <- prepareQuery' dbs Nothing  [] [] [] [] query []
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

prepareQuery' :: (ResultRow row, Monad m) => [Database m row ] -> Maybe (TheoremProver, [Input]) -> [QueryRewritingRule] -> [InsertRewritingRule] -> [InsertRewritingRule] -> [InsertRewritingRule] -> Query -> [Var] -> m (QueryPlan2 m row)
prepareQuery' dbs v qr qr2 ir dr qu0 vars = do
    let qu@(Query rvars _) = rewriteQuery vars qr qr2 ir dr qu0
    let insp = queryPlan dbs qu
    effective <- (runExceptT (checkQueryPlan2 dbs insp))
    case effective of
        Left (errmsg, formula, _) -> error (errmsg ++ ". can't find effective literals, try reordering the literals: " ++ show qu)
        Right _ ->
            let qp2 = calculateVars vars rvars insp
                qp3 = optimizeQueryPlan dbs qp2
                qp4 = addTransaction qp3 in
                prepareQueryPlan dbs qp4


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

instance (Monad m, ResultRow row) => DBStatementExec m row (QueryPlan2 m row) where
    dbStmtExec qp vars rs = snd (execQueryPlan  (vars, rs) qp)

instance (MonadIO m, ResultRow row) => Database_ (TransDB m row) m row (QueryPlan2 m row) where
    dbBegin (TransDB _ dbs  _ _ ) = mapM_ (\(Database db) -> dbBegin db) dbs
    dbCommit (TransDB _ dbs  _ _ ) =     all id <$> mapM (\(Database db) -> dbCommit db) dbs
    dbPrepare (TransDB _ dbs  _ _ ) =     all id <$> mapM (\(Database db) -> dbPrepare db) dbs
    dbRollback (TransDB _ dbs  _ _ ) =    mapM_ (\(Database db) -> dbRollback db) dbs
    getName (TransDB name _ _ _ ) = name
    getPreds (TransDB _ dbs predmap _ ) = predmap -- unions (map (\(Database db) -> getPreds db) dbs)
    domainSize (TransDB _ dbs _ _ ) varDomainSize  atom@(Atom pred args) = do
        mps <- mapM (\ (Database db) ->
                if pred `elem` getPreds db then do
                    map2 <- (domainSize db varDomainSize  atom)
                    return [map2]
                else
                    return []) dbs
        return (foldl1 (intersectionWith (+)) (concat mps))
    prepareQuery (TransDB _ dbs _ (qr, qr2, ir, dr) ) qu =
        prepareQuery' dbs  Nothing qr qr2 ir dr qu
    -- exec (TransDB _ dbs _ _  _ _ _) qp vars stream = snd (execQueryPlan dbs (vars, stream ) qp)
    supported _ _ _ = True
    supported' _ _ _ = True

getRewriting :: PredMap -> TranslationInfo -> IO (RewritingRuleSets, PredMap)
getRewriting predmap ps = do
    d0 <- toString <$> B.readFile (rewriting_file_path ps)
    case runParser rulesp predmap "" d0 of
        Left err -> error (show err)
        Right rules -> return rules

dbMap :: Map String (ICATDBConnInfo -> IO [Database DBAdapterMonad MapResultRow])
dbMap = fromList [("SQL/HDBC/PostgreSQL", PostgreSQL.getDB), ("Cypher/Neo4j", Neo4j.getDB)];

getDB :: ICATDBConnInfo -> IO [Database DBAdapterMonad MapResultRow]
getDB ps = case lookup (catalog_database_type ps) dbMap of
    Just getDBFunc -> getDBFunc ps
    Nothing -> error ("unimplemented database type " ++ (catalog_database_type ps))

transDB :: String -> TranslationInfo -> IO (TransDB DBAdapterMonad MapResultRow)
transDB name transinfo = do
    dbs <- concat <$> mapM (\(DBTrans ps) -> do
            db <- getDB ps
            return db) (db_plugins transinfo)
    let predmap0 = constructPredMap dbs
    let hiding = hide_predicate transinfo
    let predmap1 = filterWithKey (\k _ -> not (k `elem` hiding)) predmap0
    let add = add_predicate transinfo
    (rewriting, predmap0') <- getRewriting predmap0 transinfo
    let predmap2 = foldMap (\n -> singleton n (case lookup n predmap0' of
            Just a -> a
            Nothing -> error ("add predicate " ++ n ++ " is not defined"))) add
    return (TransDB name dbs (elems (predmap1 `Data.Map.Strict.union` predmap2)) rewriting )
