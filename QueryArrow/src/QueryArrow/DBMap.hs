{-# LANGUAGE TypeFamilies, MultiParamTypeClasses, FlexibleInstances, UndecidableInstances, FlexibleContexts, RankNTypes, GADTs #-}
module QueryArrow.DBMap where

import QueryArrow.DB.DB
import QueryArrow.FO.Data
import QueryArrow.Config
import QueryArrow.Translation
import QueryArrow.Cache
import QueryArrow.DB.AbstractDatabaseList
import QueryArrow.Plugin

import Prelude  hiding (lookup)
import Data.Map.Strict (fromList, Map, lookup)

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

type DBMap = Map String (AbstractPlugin MapResultRow)

getDB2 :: DBMap -> DBTrans -> IO (AbstractDatabase MapResultRow Formula)
getDB2 dbMap (DBTrans ps plugins) = case lookup (catalog_database_type ps) dbMap of
    Just (AbstractPlugin getDBFunc) -> do
      dbs <- case plugins of
                  Nothing -> return (AbstractDBList HNil)
                  Just pluginsx -> getDBs dbMap pluginsx
      getDB getDBFunc ps dbs
    Nothing -> error ("unimplemented database type " ++ (catalog_database_type ps))

getDBs :: DBMap -> [DBTrans] -> IO (AbstractDBList MapResultRow)
getDBs _ [] = return (AbstractDBList HNil)
getDBs dbMap (transinfo : l) = do
    db0 <- getDB2 dbMap transinfo
    case db0 of
      AbstractDatabase db -> do
        dbs <- getDBs dbMap l
        case dbs of
          AbstractDBList dbs -> return (AbstractDBList (HCons db dbs))

dbMap0 :: DBMap
dbMap0 = fromList [
    ("SQL/HDBC/PostgreSQL", AbstractPlugin PostgreSQL.PostgreSQLPlugin),
    ("SQL/HDBC/CockroachDB", AbstractPlugin CockroachDB.CockroachDBPlugin),
    ("SQL/HDBC/Sqlite3", AbstractPlugin Sqlite3.SQLite3Plugin),
    ("Cypher/Neo4j", AbstractPlugin Neo4j.Neo4jPlugin),
    ("InMemory/Eq", AbstractPlugin (InMemory.NoConnectionDatabasePlugin InMemory.EqDB)),
    ("InMemory/Regex", AbstractPlugin (InMemory.NoConnectionDatabasePlugin InMemory.RegexDB)),
    ("InMemory/Utils", AbstractPlugin (InMemory.NoConnectionDatabasePlugin InMemory.UtilsDB)),
    ("InMemory/Text", AbstractPlugin (InMemory.NoConnectionDatabasePlugin InMemory.TextDB)),
    ("InMemory/Map", AbstractPlugin (InMemory.NoConnectionDatabasePlugin2 InMemory.MapDB)),
    ("InMemory/MutableMap", AbstractPlugin (InMemory.NoConnectionDatabasePlugin2 InMemory.StateMapDB)),
    ("ElasticSearch/ElasticSearch", AbstractPlugin ElasticSearch.ElasticSearchPlugin),
    ("Remote/TCP", AbstractPlugin Remote.TCP.RemoteTCPPlugin),
    ("FileSystem", AbstractPlugin FileSystem.FileSystemPlugin),
    ("Cache", AbstractPlugin CachePlugin),
    ("Translation", AbstractPlugin TransPlugin)
    ];


transDB :: TranslationInfo -> IO (AbstractDatabase MapResultRow Formula)
transDB transinfo =
    getDB2 dbMap0 (db_plugin transinfo)
