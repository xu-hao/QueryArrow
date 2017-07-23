{-# LANGUAGE TypeFamilies, MultiParamTypeClasses, FlexibleInstances, UndecidableInstances, FlexibleContexts, DeriveGeneric #-}
module QueryArrow.Cache where

import QueryArrow.DB.DB
import QueryArrow.Syntax.Data
import QueryArrow.Syntax.Types
import QueryArrow.QueryPlan
import QueryArrow.Plugin
import QueryArrow.Config

import Prelude  hiding (lookup)
import Control.Applicative ((<$>))
import Data.Set (Set)
import Data.Cache.LRU.IO
import System.Log.Logger
import Data.Aeson
import GHC.Generics

type TransCache query = AtomicLRU (Set Var, FormulaT, Set Var) query

data CacheTransDB db = CacheTransDB String db (TransCache (DBQueryType db))

instance (IDatabaseUniformDBFormula FormulaT db) => IDatabase0 (CacheTransDB db) where
    type DBFormulaType (CacheTransDB db) = FormulaT
    getName (CacheTransDB name _ _ ) = name
    getPreds (CacheTransDB _ db _ ) = getPreds db
    supported (CacheTransDB _ db _) = supported db

instance (IDatabaseUniformDBFormula FormulaT db) => IDatabase1 (CacheTransDB db) where
    type DBQueryType (CacheTransDB db) = DBQueryType db
    translateQuery (CacheTransDB _ db cache ) vars2 qu vars = do
        infoM "TransCache" ("looking up " ++ show qu)
        qu' <- lookup (vars2, qu, vars) cache
        case qu' of
            Just qu2 -> do
                infoM "TransCache" ("found translated query")
                return qu2
            Nothing -> do
                infoM "TransCache" ("did not find translated query")
                qu' <- translateQuery db vars2 qu vars
                insert (vars2, qu, vars) qu' cache
                return qu'

instance (IDatabase db) => IDatabase2 (CacheTransDB db) where
    newtype ConnectionType (CacheTransDB db) = CacheTransDBConnection (ConnectionType db)
    dbOpen (CacheTransDB _ db _ ) = CacheTransDBConnection <$> dbOpen db

instance IDatabaseUniformDBFormula FormulaT db => IDatabase (CacheTransDB db)

instance (IDatabase db) => IDBConnection0 (ConnectionType (CacheTransDB db)) where
    dbClose (CacheTransDBConnection db ) = dbClose db
    dbBegin (CacheTransDBConnection db) = dbBegin db
    dbCommit (CacheTransDBConnection db) = dbCommit db
    dbPrepare (CacheTransDBConnection db) = dbPrepare db
    dbRollback (CacheTransDBConnection db) = dbRollback db

instance (IDatabase db) => IDBConnection (ConnectionType (CacheTransDB db)) where
    type QueryType (ConnectionType (CacheTransDB db)) = QueryType (ConnectionType db)
    type StatementType (ConnectionType (CacheTransDB db)) = StatementType (ConnectionType db)
    prepareQuery (CacheTransDBConnection db) = prepareQuery db
    -- exec (TransDB _ dbs _ _  _ _ _) qp vars stream = snd (execQueryPlan dbs (vars, stream ) qp)

cacheDB :: IDatabase db => String -> db -> Maybe Integer -> IO (CacheTransDB db)
cacheDB name db capacity = do
  cache <- newAtomicLRU capacity
  return (CacheTransDB name db cache)

data ICATCacheConnInfo = ICATCacheConnInfo {
  max_cc :: Integer,
  cache_db_plugin :: ICATDBConnInfo
} deriving (Show, Generic)

instance ToJSON ICATCacheConnInfo
instance FromJSON ICATCacheConnInfo

data CachePlugin = CachePlugin
instance Plugin CachePlugin trans row where
  getDB _ getDB0 ps = do
    let fsconf = getDBSpecificConfig ps
    db0 <- getDB0 (cache_db_plugin fsconf)
    case db0 of
        AbstractDatabase db ->
            AbstractDatabase <$> cacheDB (qap_name ps) db (Just (max_cc fsconf))
