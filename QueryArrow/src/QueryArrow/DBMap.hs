{-# LANGUAGE TypeFamilies, MultiParamTypeClasses, FlexibleInstances, UndecidableInstances, FlexibleContexts, RankNTypes, GADTs, DeriveGeneric #-}
module QueryArrow.DBMap where

import QueryArrow.DB.DB
import QueryArrow.FO.Data
import QueryArrow.Config
import QueryArrow.Translation
import QueryArrow.Cache
import QueryArrow.Plugin
import QueryArrow.Sum
import QueryArrow.Binding.Binding
import Data.Aeson
import GHC.Generics
import Data.Maybe

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

type DBMap = Map String (AbstractPlugin MapResultRow)

getDB2 :: DBMap -> GetDBFunction MapResultRow
getDB2 dbMap ps = case lookup (catalog_database_type ps) dbMap of
    Just (AbstractPlugin getDBFunc) ->
        getDB getDBFunc (getDB2 dbMap) ps
    Nothing -> error ("unimplemented database type " ++ (catalog_database_type ps))

data IncludePlugin = IncludePlugin

data IncludePluginConfig = IncludePluginConfig {
    include :: String
} deriving Generic

instance FromJSON IncludePluginConfig

instance ToJSON IncludePluginConfig

instance Plugin IncludePlugin MapResultRow where
  getDB _ _ ps0 = do
    let ps = getDBSpecificConfig ps0
    ps3 <- getConfig (include ps)
    getDB2 dbMap0 ps3

dbMap0 :: DBMap
dbMap0 = fromList [
    ("SQL/HDBC/PostgreSQL", AbstractPlugin PostgreSQL.PostgreSQLPlugin),
    ("SQL/HDBC/CockroachDB", AbstractPlugin CockroachDB.CockroachDBPlugin),
    ("SQL/HDBC/Sqlite3", AbstractPlugin Sqlite3.SQLite3Plugin),
    ("Cypher/Neo4j", AbstractPlugin Neo4j.Neo4jPlugin),
    ("InMemory/Eq", AbstractPlugin (InMemory.NoConnectionDatabasePlugin ((EffectFreeBinaryBindingDatabase .) . InMemory.eqDB))),
    ("InMemory/Ne", AbstractPlugin (InMemory.NoConnectionDatabasePlugin ((EffectFreeBinaryBindingDatabase .) . InMemory.neDB))),
    ("InMemory/Le", AbstractPlugin (InMemory.NoConnectionDatabasePlugin ((EffectFreeBinaryBindingDatabase .) . InMemory.leDB))),
    ("InMemory/Ge", AbstractPlugin (InMemory.NoConnectionDatabasePlugin ((EffectFreeBinaryBindingDatabase .) . InMemory.geDB))),
    ("InMemory/Lt", AbstractPlugin (InMemory.NoConnectionDatabasePlugin ((EffectFreeBinaryBindingDatabase .) . InMemory.ltDB))),
    ("InMemory/Gt", AbstractPlugin (InMemory.NoConnectionDatabasePlugin ((EffectFreeBinaryBindingDatabase .) . InMemory.gtDB))),
    ("InMemory/Add", AbstractPlugin (InMemory.NoConnectionDatabasePlugin ((EffectFreeTernaryBindingDatabase .) . InMemory.addDB))),
    ("InMemory/Sub", AbstractPlugin (InMemory.NoConnectionDatabasePlugin ((EffectFreeTernaryBindingDatabase .) . InMemory.mulDB))),
    ("InMemory/Mul", AbstractPlugin (InMemory.NoConnectionDatabasePlugin ((EffectFreeTernaryBindingDatabase .) . InMemory.subDB))),
    ("InMemory/Div", AbstractPlugin (InMemory.NoConnectionDatabasePlugin ((EffectFreeTernaryBindingDatabase .) . InMemory.divDB))),
    ("InMemory/Mod", AbstractPlugin (InMemory.NoConnectionDatabasePlugin ((EffectFreeTernaryBindingDatabase .) . InMemory.modDB))),
    ("InMemory/Exp", AbstractPlugin (InMemory.NoConnectionDatabasePlugin ((EffectFreeTernaryBindingDatabase .) . InMemory.expDB))),
    ("InMemory/Concat", AbstractPlugin (InMemory.NoConnectionDatabasePlugin ((EffectFreeTernaryBindingDatabase .) . InMemory.concatDB))),
    ("InMemory/Strlen", AbstractPlugin (InMemory.NoConnectionDatabasePlugin ((EffectFreeBinaryBindingDatabase .) . InMemory.strlenDB))),
    ("InMemory/LikeRegex", AbstractPlugin (InMemory.NoConnectionDatabasePlugin ((EffectFreeBinaryBindingDatabase .) . InMemory.likeRegexDB))),
    ("InMemory/NotLikeRegex", AbstractPlugin (InMemory.NoConnectionDatabasePlugin ((EffectFreeBinaryBindingDatabase .) . InMemory.notLikeRegexDB))),
    ("InMemory/Sleep", AbstractPlugin (InMemory.NoConnectionDatabasePlugin ((EffectfulUnaryBindingDatabase .) . InMemory.sleepDB))),
    ("InMemory/Encode", AbstractPlugin (InMemory.NoConnectionDatabasePlugin ((EffectFreeTernaryBindingDatabase .) . InMemory.encodeDB))),
    ("InMemory/Map", AbstractPlugin (InMemory.NoConnectionDatabasePlugin2 ((((EffectFreeBinaryBindingDatabase .) .) .) . InMemory.MapDB))),
    ("InMemory/MutableMap", AbstractPlugin (InMemory.NoConnectionDatabasePlugin2 ((((EffectfulBinaryBindingDatabase .) .) .) . InMemory.StateMapDB))),
    ("ElasticSearch/ElasticSearch", AbstractPlugin ElasticSearch.ElasticSearchPlugin),
    ("Remote/TCP", AbstractPlugin Remote.TCP.RemoteTCPPlugin),
    ("FileSystem", AbstractPlugin FileSystem.FileSystemPlugin),
    ("Cache", AbstractPlugin CachePlugin),
    ("Translation", AbstractPlugin TransPlugin),
    ("Include", AbstractPlugin IncludePlugin),
    ("Sum", AbstractPlugin SumPlugin)
    ];


transDB :: TranslationInfo -> IO (AbstractDatabase MapResultRow Formula)
transDB transinfo =
    getDB2 dbMap0 (db_plugin transinfo)
