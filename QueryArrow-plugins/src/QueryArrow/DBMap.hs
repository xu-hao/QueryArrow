{-# LANGUAGE TypeFamilies, MultiParamTypeClasses, FlexibleInstances, UndecidableInstances, FlexibleContexts, RankNTypes, GADTs #-}
module QueryArrow.DBMap where

import QueryArrow.DB.DB
import QueryArrow.Semantics.TypeChecker
import QueryArrow.Config
import QueryArrow.DB.Trans.Translation
import QueryArrow.DB.Cache
import QueryArrow.Plugin
import QueryArrow.DB.Sum
import QueryArrow.DB.Include
import Data.Maybe

import Prelude  hiding (lookup)
import Data.Map.Strict (fromList, Map, lookup)

-- import Plugins
import qualified QueryArrow.DB.SQL.HDBC.PostgreSQL as PostgreSQL
import qualified QueryArrow.DB.SQL.HDBC.CockroachDB as CockroachDB
import qualified QueryArrow.DB.Cypher.Neo4j as Neo4j
import QueryArrow.DB.InMemory.BuiltIn
import QueryArrow.DB.InMemory.Map
import qualified QueryArrow.DB.ElasticSearch.ElasticSearch as ElasticSearch
import qualified QueryArrow.DB.SQL.HDBC.Sqlite3 as Sqlite3
-- import qualified QueryArrow.Remote.NoTranslation.TCP.TCP as Remote.TCP
-- import qualified QueryArrow.FileSystem.FileSystem as FileSystem
import qualified QueryArrow.DB.SQL.LibPQ.PostgreSQL as LibPQ

type DBMap = Map String (AbstractPlugin MapResultRow)

getDB2 :: DBMap -> GetDBFunction MapResultRow
getDB2 dbMap ps = case lookup (catalog_database_type ps) dbMap of
    Just (AbstractPlugin getDBFunc) ->
        getDB getDBFunc (getDB2 dbMap) ps
    Nothing -> error ("unimplemented database type " ++ (catalog_database_type ps))

dbMap0 :: DBMap
dbMap0 = fromList [
    ("SQL/HDBC/PostgreSQL", AbstractPlugin PostgreSQL.PostgreSQLPlugin),
    ("SQL/HDBC/CockroachDB", AbstractPlugin CockroachDB.CockroachDBPlugin),
    ("SQL/HDBC/Sqlite3", AbstractPlugin Sqlite3.SQLite3Plugin),
    ("SQL/LibPQ", AbstractPlugin LibPQ.PostgreSQLPlugin),
    ("Cypher/Neo4j", AbstractPlugin Neo4j.Neo4jPlugin),
    ("InMemory/BuiltIn", AbstractPlugin builtInPlugin),
    ("InMemory/Map", AbstractPlugin mapPlugin),
    ("InMemory/MutableMap", AbstractPlugin stateMapPlugin),
    ("ElasticSearch/ElasticSearch", AbstractPlugin ElasticSearch.ElasticSearchPlugin),
--    ("Remote/TCP", AbstractPlugin Remote.TCP.RemoteTCPPlugin),
--    ("FileSystem", AbstractPlugin FileSystem.FileSystemPlugin),
    ("Cache", AbstractPlugin CachePlugin),
    ("Translation", AbstractPlugin TransPlugin),
    ("Include", AbstractPlugin IncludePlugin),
    ("Sum", AbstractPlugin SumPlugin)
    ];


transDB :: TranslationInfo -> IO (AbstractDatabase MapResultRow FormulaT)
transDB transinfo =
    getDB2 dbMap0 (db_plugin transinfo)
