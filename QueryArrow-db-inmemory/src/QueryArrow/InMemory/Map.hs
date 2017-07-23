{-# LANGUAGE TypeFamilies, MultiParamTypeClasses, FlexibleInstances, FlexibleContexts, PatternSynonyms, DeriveGeneric, RankNTypes, GADTs #-}
module QueryArrow.InMemory.Map where

import QueryArrow.Syntax.Data
import QueryArrow.Syntax.Types
import QueryArrow.Semantics.ResultValue
import QueryArrow.Semantics.ResultValue.AbstractResultValue
import QueryArrow.Semantics.ResultRow
import QueryArrow.Semantics.ResultRow.VectorResultRow
import QueryArrow.Semantics.ResultSet
import QueryArrow.DB.DB
import QueryArrow.DB.NoConnection
import QueryArrow.Plugin
import QueryArrow.Binding.Binding
import QueryArrow.Config

import Prelude  hiding (lookup)
import Data.List ((\\), union)
import Data.Convertible.Base
import Control.Monad.IO.Class
import Data.IORef
import Data.Aeson
import GHC.Generics


-- example MapDB


data MapBinding = MapBinding String String [(ConcreteResultValue, ConcreteResultValue)] deriving Show
instance Binding MapBinding where
    bindingPred (MapBinding ns predname _) = Pred (QPredName ns [] predname) (PredType ObjectPred [ParamType True True True TextType, ParamType True True True TextType])
    bindingSupport _ [_,_] = True
    bindingSupport _ _ = False
    bindingSupportInsert _ = False
    bindingSupportDelete _ = False
    bindingExec (MapBinding _ _ rows) [I,I] [aval, bval] =
      return (if (aval, bval) `elem` rows
        then [[]]
        else [])
    bindingExec (MapBinding _ _ rows) [I,O] [aval] =
        return [[snd x] | x <- rows, fst x == aval]
    bindingExec (MapBinding _ _ rows) [O,I] [bval] =
        return [[fst x] | x <- rows, snd x == bval]
    bindingExec (MapBinding _ _ rows) [O,O] [] =
        return (map (\(a,b)->[a,b]) rows)

mapDB :: String -> String -> String -> [(ConcreteResultValue, ConcreteResultValue)] -> BindingDatabase
mapDB dbname ns n rows = BindingDatabase dbname [AbstractBinding (MapBinding ns n rows)]
-- update mapdb

instance Show (IORef a) where
    show _ = "ioref"

data StateMapBinding = StateMapBinding String String (IORef [(ConcreteResultValue, ConcreteResultValue)]) deriving Show

instance Binding StateMapBinding where
    bindingPred (StateMapBinding ns predname _) = Pred (QPredName ns [] predname) (PredType ObjectPred [ParamType True True True TextType, ParamType True True True TextType])
    bindingSupport _ [_,_] = True
    bindingSupport _ _ = False
    bindingSupportInsert _ = True
    bindingSupportDelete _ = True
    bindingExec (StateMapBinding _ _ map1) [I,I] [aval, bval] = do
      rows <- liftIO $ readIORef map1
      return (if (aval, bval) `elem` rows
        then [[]]
        else [])
    bindingExec (StateMapBinding _ _ map1) [I,O] [aval] = do
      rows <- liftIO $ readIORef map1
      return [[snd x] | x <- rows, fst x == aval]
    bindingExec (StateMapBinding _ _ map1) [O,I] [bval] = do
      rows <- liftIO $ readIORef map1
      return [[fst x] | x <- rows, snd x == bval]
    bindingExec (StateMapBinding _ _ map1) [O,O] [] = do
      rows <- liftIO $ readIORef map1
      return (map (\(a,b)->[a,b]) rows)
    bindingInsert (StateMapBinding _ _ map1) [aval, bval] = do
        rows <- liftIO $ readIORef map1
        let add rows1 row = rows1 `union` [row]
        let rows2 = add rows (aval, bval)
        liftIO $ writeIORef map1 rows2
    bindingDelete (StateMapBinding _ _ map1) [aval, bval] = do
        rows <- liftIO $ readIORef map1
        let remove rows1 row = rows1 \\ [row]
        let rows2 = remove rows (aval, bval)
        liftIO $ writeIORef map1 rows2

stateMapDB :: String -> String -> String ->  IORef [(ConcreteResultValue, ConcreteResultValue)] -> BindingDatabase
stateMapDB dbname ns n map1 =
  BindingDatabase dbname [AbstractBinding (StateMapBinding ns n map1)]

data ICATMapDBInfo = ICATMapDBInfo {
  db_namespace :: String,
  predicate_name :: String,
  db_map:: Value
} deriving (Show, Generic)

instance ToJSON ICATMapDBInfo
instance FromJSON ICATMapDBInfo

data NoConnectionDatabasePlugin2 db a = (IDatabase0 db, IDatabase1 db, INoConnectionDatabase2 db, DBQueryType db ~ NoConnectionQueryType db, DBFormulaType db ~ FormulaT) => NoConnectionDatabasePlugin2 (String -> String -> String -> a -> db)

instance (Convertible Value (IO a),
          ResultSetTransType (NoConnectionResultSetType db) ~ trans,
          ResultSetRowType (NoConnectionResultSetType db) ~ VectorResultRow AbstractResultValue,
          NoConnectionInputRowType db ~ VectorResultRow AbstractResultValue) => Plugin (NoConnectionDatabasePlugin2 db a) trans (VectorResultRow AbstractResultValue) where
  getDB (NoConnectionDatabasePlugin2 db) _ ps = do
      let fsconf = getDBSpecificConfig ps
      dbdata <- convert (db_map fsconf)
      return (AbstractDatabase (NoConnectionDatabase (db (qap_name ps) (db_namespace fsconf) (predicate_name fsconf) dbdata)))

instance Convertible Value (IO [(ConcreteResultValue, ConcreteResultValue)]) where
  safeConvert a = case fromJSON a of
    Error err -> error err
    Success b -> Right (return (map (\(a,b) -> (StringValue a, StringValue b)) b))

instance Convertible Value (IO (IORef [(ConcreteResultValue, ConcreteResultValue)])) where
  safeConvert a = Right (convert a >>= newIORef)

mapPlugin :: NoConnectionDatabasePlugin2 BindingDatabase [(ConcreteResultValue, ConcreteResultValue)]
mapPlugin = NoConnectionDatabasePlugin2 mapDB

stateMapPlugin :: NoConnectionDatabasePlugin2 BindingDatabase (IORef [(ConcreteResultValue, ConcreteResultValue)])
stateMapPlugin = NoConnectionDatabasePlugin2 stateMapDB
