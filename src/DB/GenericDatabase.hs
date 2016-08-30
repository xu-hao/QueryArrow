{-# LANGUAGE TypeFamilies #-}

module DB.GenericDatabase where

import FO.Data
import DB.DB

import Data.Set

data GenericDatabase db a = GenericDatabase db a String [Pred]

class IGenericDatabase01 db where
  type GDBQueryType db
  type GDBFormulaType db
  gDeterminateVars :: db -> Set Var -> Atom -> Set Var
  gSupported :: db -> GDBFormulaType db -> Set Var -> Bool
  gTranslateQuery :: db -> Set Var -> GDBFormulaType db -> Set Var -> IO (GDBQueryType db)

instance (IGenericDatabase01 db) => IDatabase0 (GenericDatabase db a) where
  type DBFormulaType (GenericDatabase db a) = GDBFormulaType db
  getName (GenericDatabase _ _ name _) = name
  getPreds (GenericDatabase _ _ _ preds) = preds
  determinateVars (GenericDatabase db _ _ _) = gDeterminateVars db
  supported (GenericDatabase db _ _ _) = gSupported db
instance (IGenericDatabase01 db) => IDatabase1 (GenericDatabase db a) where
  type DBQueryType (GenericDatabase db a) = GDBQueryType db
  translateQuery (GenericDatabase db _ _ _) = gTranslateQuery db
