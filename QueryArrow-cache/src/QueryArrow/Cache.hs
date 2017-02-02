{-# LANGUAGE TypeFamilies, MultiParamTypeClasses, FlexibleInstances, UndecidableInstances, FlexibleContexts, DeriveGeneric #-}
module QueryArrow.Cache where

import QueryArrow.DB.DB
import QueryArrow.FO.Data
import QueryArrow.FO.Types
import QueryArrow.QueryPlan
import QueryArrow.Plugin
import QueryArrow.Data.Heterogeneous.List
import QueryArrow.DB.AbstractDatabaseList
import QueryArrow.Config

import Prelude  hiding (lookup)
import Control.Applicative ((<$>))
import Data.Set (Set)
import Data.Cache.LRU.IO
import System.Log.Logger
import Data.Aeson
import GHC.Generics
import Data.Maybe

type TransCache query = (AtomicLRU (Set Var, Formula, Set Var) query, AtomicLRU (VarTypeMap, Formula, VarTypeMap) (Either String ()))

data CacheTransDB db = CacheTransDB String db (TransCache (DBQueryType db))

instance (IDatabaseUniformDBFormula Formula db) => IDatabase0 (CacheTransDB db) where
    type DBFormulaType (CacheTransDB db) = Formula
    getName (CacheTransDB name _ _ ) = name
    getPreds (CacheTransDB _ db _ ) = getPreds db
    supported (CacheTransDB _ db _) = supported db
    checkQuery (CacheTransDB _ db (_, cache)) vars2 qu vars = do
        infoM "TransCache" ("looking up " ++ show qu)
        qu' <- lookup (vars2, qu, vars) cache
        case qu' of
            Just qu2 -> do
                infoM "TransCache" ("found checked query")
                return qu2
            Nothing -> do
                infoM "TransCache" ("did not find checked query")
                qu' <- checkQuery db vars2 qu vars
                insert (vars2, qu, vars) qu' cache
                return qu'

instance (IDatabaseUniformDBFormula Formula db) => IDatabase1 (CacheTransDB db) where
    type DBQueryType (CacheTransDB db) = DBQueryType db
    translateQuery (CacheTransDB _ db (cache, _) ) vars2 qu vars = do
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

instance IDatabaseUniformDBFormula Formula db => IDatabase (CacheTransDB db)

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
  cache2 <- newAtomicLRU capacity
  return (CacheTransDB name db (cache, cache2))

data ICATCacheConnInfo = ICATCacheConnInfo {
  max_cc :: Integer
} deriving (Show, Generic)

instance ToJSON ICATCacheConnInfo
instance FromJSON ICATCacheConnInfo

data CachePlugin = CachePlugin
instance Plugin CachePlugin row where
  getDB _ ps (AbstractDBList (HCons db HNil)) =
    case fromJSON (fromJust (db_config ps)) of
      Error err -> error err
      Success fsconf -> AbstractDatabase <$> cacheDB (qap_name ps) db (Just (max_cc fsconf))
