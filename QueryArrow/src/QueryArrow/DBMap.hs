{-# LANGUAGE TypeFamilies, MultiParamTypeClasses, FlexibleInstances, UndecidableInstances, FlexibleContexts #-}
module QueryArrow.DBMap where

import QueryArrow.DB.DB
import QueryArrow.DB.NoConnection
import QueryArrow.FO.Data
import QueryArrow.Rewriting
import QueryArrow.Config
import QueryArrow.Utils
import QueryArrow.Sum
import QueryArrow.Translation
import qualified QueryArrow.Translation as T
import QueryArrow.ListUtils
import QueryArrow.Cache

import Prelude  hiding (lookup)
import Data.Map.Strict (foldrWithKey, fromList, Map, lookup, elems)
import Control.Monad.Except
import Data.Namespace.Namespace
import Data.Monoid
import System.Log.Logger

-- import Plugins
import qualified QueryArrow.SQL.HDBC.PostgreSQL as PostgreSQL
import qualified QueryArrow.SQL.HDBC.CockroachDB as CockroachDB
import qualified QueryArrow.Cypher.Neo4j as Neo4j
import qualified QueryArrow.InMemory as InMemory
import qualified QueryArrow.ElasticSearch.ElasticSearch as ElasticSearch
import qualified QueryArrow.SQL.HDBC.Sqlite3 as Sqlite3
import qualified QueryArrow.Remote.NoTranslation.TCP.TCP as Remote.TCP
import qualified QueryArrow.FileSystem.FileSystem as FileSystem
import QueryArrow.Data.Heterogeneous.List

type DBMap = Map String (ICATDBConnInfo -> IO (AbstractDatabase MapResultRow Formula))

getDB :: DBMap -> ICATDBConnInfo -> IO (AbstractDatabase MapResultRow Formula)
getDB dbMap ps = case lookup (catalog_database_type ps) dbMap of
    Just getDBFunc -> getDBFunc ps
    Nothing -> error ("unimplemented database type " ++ (catalog_database_type ps))

getDBs :: DBMap -> [DBTrans] -> IO (AbstractDBList MapResultRow)
getDBs _ [] = return (AbstractDBList HNil)
getDBs dbMap (DBTrans ps : l) = do
    db0 <- getDB dbMap ps
    case db0 of
      AbstractDatabase db -> do
        dbs <- getDBs dbMap l
        case dbs of
          AbstractDBList dbs -> return (AbstractDBList (HCons db dbs))

dbMap :: DBMap
dbMap = fromList [
    ("SQL/HDBC/PostgreSQL", PostgreSQL.getDB),
    ("SQL/HDBC/CockroachDB", CockroachDB.getDB),
    ("SQL/HDBC/Sqlite3", Sqlite3.getDB),
    ("Cypher/Neo4j", Neo4j.getDB),
    ("InMemory/EqDB", \ps ->  return (AbstractDatabase (NoConnectionDatabase (InMemory.EqDB (db_name ps))))),
    ("InMemory/RegexDB", \ps -> return (AbstractDatabase (NoConnectionDatabase (InMemory.RegexDB (db_name ps))))),
    ("InMemory/UtilsDB", \ps -> return (AbstractDatabase (NoConnectionDatabase (InMemory.UtilsDB (db_name ps))))),
    ("ElasticSearch/ElasticSearch", ElasticSearch.getDB),
    ("Remote/TCP", Remote.TCP.getDB),
    ("FileSystem", FileSystem.getDB)
    ];


transDB :: String -> TranslationInfo -> IO (AbstractDatabase MapResultRow Formula)
transDB name transinfo = do
    dbs <- getDBs dbMap (db_plugins transinfo)
    case dbs of
        AbstractDBList dbs -> do
            let sumdb = SumDB "sum" dbs
            tdb <- T.transDB name (AbstractDatabase sumdb) transinfo
            case tdb of
              AbstractDatabase db ->
                AbstractDatabase <$> cacheDB name db (Just (max_cc transinfo))
