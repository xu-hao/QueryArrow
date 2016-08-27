{-# LANGUAGE TypeFamilies #-}

module DB.GenericDatabase where

import FO.Data
import DB.DB

import Data.Set

data GenericDatabase db a = GenericDatabase db a String [Pred]

class IGenericDatabase0 db where
  type GDBQueryType db
  gDeterminateVars :: db -> Set Var -> Atom -> Set Var
  gSupported :: db -> Formula -> Set Var -> Bool
  gTranslateQuery :: db -> Set Var -> Query -> Set Var -> GDBQueryType db

instance (IGenericDatabase0 db) => IDatabase0 (GenericDatabase db a) where
  type DBQueryType (GenericDatabase db a) = GDBQueryType db
  getName (GenericDatabase _ _ name _) = name
  getPreds (GenericDatabase _ _ _ preds) = preds
  determinateVars (GenericDatabase db _ _ _) = gDeterminateVars db
  supported (GenericDatabase db _ _ _) = gSupported db
  translateQuery (GenericDatabase db _ _ _) = gTranslateQuery db
