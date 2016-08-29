{-# LANGUAGE TypeFamilies, MultiParamTypeClasses, FlexibleInstances, UndecidableInstances, FlexibleContexts #-}
module DBMap where

import DB.DB
import DB.NoConnection
import FO.Data
import Rewriting
import Config
import Utils
import Sum
import Translation
import ListUtils

import Prelude  hiding (lookup)
import Data.Map.Strict (foldrWithKey, fromList)
import Control.Monad.Except
import Data.Namespace.Namespace
import Data.Monoid
import System.Log.Logger

-- import Plugins
import qualified SQL.HDBC.PostgreSQL as PostgreSQL
import qualified SQL.HDBC.CockroachDB as CockroachDB
import qualified Cypher.Neo4j as Neo4j
import qualified InMemory as InMemory
import qualified ElasticSearch.ElasticSearch as ElasticSearch
import qualified SQL.HDBC.Sqlite3 as Sqlite3


dbMap :: DBMap
dbMap = fromList [
    ("SQL/HDBC/PostgreSQL", PostgreSQL.getDB),
    ("SQL/HDBC/CockroachDB", CockroachDB.getDB),
    ("SQL/HDBC/Sqlite3", Sqlite3.getDB),
    ("Cypher/Neo4j", Neo4j.getDB),
    ("InMemory/EqDB", \ps ->  AbstractDatabase (NoConnectionDatabase (InMemory.EqDB (db_name ps)))),
    ("InMemory/RegexDB", \ps ->  AbstractDatabase (NoConnectionDatabase (InMemory.RegexDB (db_name ps)))),
    ("InMemory/UtilsDB", \ps ->  AbstractDatabase (NoConnectionDatabase (InMemory.UtilsDB (db_name ps)))),
    ("ElasticSearch/ElasticSearch", ElasticSearch.getDB)
    ];


transDB :: String -> TranslationInfo -> IO (AbstractDatabase MapResultRow Formula)
transDB name transinfo =
    case getDBs dbMap (db_plugins transinfo) of
        AbstractDBList dbs -> do
            let sumdb = SumDB "sum" dbs
            let predmap0 = constructDBPredMap sumdb
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
            let repeats = findRepeats exportedpreds
            unless (null repeats) $ error ("more than one export for predicates " ++ show repeats)
            let rules1@(qr, ir, dr) = rules0 <> rewriting
            let checkPatterns rules = do
                    let repeats = findRepeats (map (\(InsertRewritingRule (Atom p _) _) -> p) rules)
                    unless (null repeats) $ error ("more than one definition for predicates " ++ show repeats)
            checkPatterns qr
            checkPatterns ir
            checkPatterns dr
            mapM_ (debugM "QA" . show) qr
            mapM_ (debugM "QA" . show) ir
            mapM_ (debugM "QA" . show) dr
            return (AbstractDatabase (TransDB name sumdb exportedpreds rules1))
