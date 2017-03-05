{-# LANGUAGE TypeFamilies, FlexibleContexts, FlexibleInstances #-}

module QueryArrow.DB.NoTranslation where

import QueryArrow.FO.Data
import QueryArrow.DB.DB
import Data.Map.Strict
import QueryArrow.FO.Types

import Data.Set

data NoTranslationDatabase db = NoTranslationDatabase db
data NTDBQuery form = NTDBQuery (Set Var) form (Set Var)

class INoTranslationDatabase01 db where
  type NTDBFormulaType db
  ntGetName :: db -> String
  ntGetPreds :: db -> [Pred]
  ntSupported :: db -> Set Var -> NTDBFormulaType db -> Set Var -> Bool

instance (INoTranslationDatabase01 db) => IDatabase0 (NoTranslationDatabase db) where
  type DBFormulaType (NoTranslationDatabase db) = NTDBFormulaType db
  getName (NoTranslationDatabase db) = ntGetName db
  getPreds (NoTranslationDatabase db) = ntGetPreds db
  supported (NoTranslationDatabase db) = ntSupported db

instance (INoTranslationDatabase01 db) => IDatabase1 (NoTranslationDatabase db) where
  type DBQueryType (NoTranslationDatabase db) = NTDBQuery (NTDBFormulaType db)
  translateQuery _ vars2 form vars = return (NTDBQuery vars2 form vars)

instance IDBConnection0 (ConnectionType db) => IDBConnection0 (ConnectionType (NoTranslationDatabase db)) where
  dbClose (NTDBConnection conn) = dbClose conn
  dbBegin (NTDBConnection conn) = dbBegin conn
  dbPrepare (NTDBConnection conn) = dbPrepare conn
  dbCommit (NTDBConnection conn) = dbCommit conn
  dbRollback (NTDBConnection conn) = dbRollback conn

instance IDatabase2 db => IDBConnection (ConnectionType (NoTranslationDatabase db)) where
  type StatementType (ConnectionType (NoTranslationDatabase db)) = StatementType (ConnectionType db)
  type QueryType (ConnectionType (NoTranslationDatabase db)) = QueryType (ConnectionType db)
  prepareQuery (NTDBConnection conn) = prepareQuery conn

instance IDatabase2 db => IDatabase2 (NoTranslationDatabase db) where
  newtype ConnectionType (NoTranslationDatabase db) = NTDBConnection (ConnectionType db)
  dbOpen (NoTranslationDatabase db) = NTDBConnection <$> dbOpen db
