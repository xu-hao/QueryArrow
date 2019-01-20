{-# LANGUAGE ExistentialQuantification, MultiParamTypeClasses, TypeSynonymInstances, FlexibleInstances, FlexibleContexts, TypeFamilies, ScopedTypeVariables, RankNTypes #-}
module QueryArrow.DB.DB where

import QueryArrow.DB.ResultStream
import QueryArrow.Syntax.Term

import Prelude  hiding (lookup, null)
import Data.Map.Strict (Map, empty, insert, lookup, singleton, keysSet, unionWithKey)
import Control.Monad.Except
import Data.Convertible.Base
import Data.Maybe
import Control.Monad.Trans.Resource
import Data.Set (Set)
import System.Log.Logger (errorM, noticeM)
import Control.Exception (catch, SomeException)
import QueryArrow.Semantics.TypeChecker
import QueryArrow.Semantics.Value

-- result row
type MapResultRow = Map Var ResultValue

instance Convertible MapResultRow MapResultRow where
  safeConvert = Right

instance IResultRow MapResultRow where
    type ElemType MapResultRow = ResultValue
    proj vars2 map1 = foldr (\var map2 -> case lookup var map1 of
                                                        Nothing -> insert var Null map2
                                                        Just rv -> insert var rv map2) empty vars2
    get v map1 = fromMaybe (error ("cannot find key " ++ show v)) (lookup v map1)
    sing = singleton

-- query

{-
    class A a b | a -> b
    class B a b | a -> b

    data C a = C a

    instance B a b => A (C a) b
-}

type DBResultStream row = ResultStream (ResourceT IO) row

type DBResultStreamTrans row = ResultStreamTrans (ResourceT IO) row

class IDBStatement stmt where
    type RowType stmt
    dbStmtExec :: stmt -> DBResultStream (RowType stmt) -> DBResultStream (RowType stmt)
    dbStmtClose :: stmt -> IO ()

data AbstractDBStatement row = forall stmt. (IDBStatement stmt, row ~ RowType stmt) => AbstractDBStatement {unAbstractDBStatement :: stmt}


-- connection
class IDBConnection0 conn where
    dbClose :: conn -> IO ()
    dbBegin :: conn -> IO ()
    dbPrepare :: conn -> IO ()
    dbCommit :: conn -> IO ()
    dbRollback :: conn -> IO ()

class (IDBConnection0 conn, IDBStatement (StatementType conn)) => IDBConnection conn where
    type QueryType conn
    type StatementType conn
    prepareQuery :: conn -> QueryType conn -> IO (StatementType conn)

data AbstractDBConnection = forall conn. (IDBConnection conn) => AbstractDBConnection { unAbstractDBConnection :: conn }

instance IDBConnection0 AbstractDBConnection where
    dbClose (AbstractDBConnection conn) = dbClose conn
    dbBegin (AbstractDBConnection conn) = dbBegin conn
    dbPrepare (AbstractDBConnection conn) = dbPrepare conn
    dbCommit (AbstractDBConnection conn) = dbCommit conn
    dbRollback (AbstractDBConnection conn) = dbRollback conn

-- database
class IDatabase0 db where
    type DBFormulaType db
    getName :: db -> String
    getPreds :: db -> [Pred]
    supported :: db -> Set Var -> DBFormulaType db -> Set Var -> Bool

class IDatabase1 db where
    type DBFormulaType1 db
    type DBQueryType db
    translateQuery :: db -> Set Var -> DBFormulaType1 db -> Set Var -> IO (DBQueryType db)

class (IDBConnection (ConnectionType db)) => IDatabase2 db where
    data ConnectionType db
    dbOpen :: db -> IO (ConnectionType db)

class (IDatabase0 db, IDatabase1 db, IDatabase2 db, DBFormulaType db ~ DBFormulaType1 db, DBQueryType db ~ QueryType (ConnectionType db)) => IDatabase db

-- https://wiki.haskell.org/Existential_type#Dynamic_dispatch_mechanism_of_OOP
data AbstractDatabase row form = forall db. (IDatabase db, row ~ RowType (StatementType (ConnectionType db)), form ~ DBFormulaType db) => AbstractDatabase { unAbstractDatabase :: db }

instance IDatabase0 (AbstractDatabase row form) where
    type DBFormulaType (AbstractDatabase row form) = form
    getName (AbstractDatabase db) = getName db
    getPreds (AbstractDatabase db) = getPreds db
    supported (AbstractDatabase db) = supported db

doQuery :: (IDatabase db, DBFormulaType db ~ FormulaT) => db -> VarTypeMap -> Formula -> VarTypeMap -> DBResultStream (RowType (StatementType (ConnectionType db))) -> DBResultStream (RowType (StatementType (ConnectionType db)))
doQuery db vars2 qu vars rs =
        bracketPStream (dbOpen db) dbClose (\conn -> doQueryWithConn db conn vars2 qu vars rs)

checkQuery :: (IDatabase db) => db -> VarTypeMap -> Formula -> VarTypeMap -> Either String FormulaT
checkQuery db vars2 qu vars = do
  let preds = getPreds db
  let ptm = constructPredTypeMap preds
  let vtm = unionWithKey (\k l r -> if l == r then l else error ("translateQuery: input output var has different types" ++ show k)) vars2 vars
  typeCheckFormula ptm vtm qu

doQueryWithConn :: (IDatabase db, DBFormulaType db ~ FormulaT) => db -> ConnectionType db -> VarTypeMap -> Formula -> VarTypeMap -> DBResultStream (RowType (StatementType (ConnectionType db))) -> DBResultStream (RowType (StatementType (ConnectionType db)))
doQueryWithConn db conn vars2 qu vars rs = do
  let res = checkQuery db vars2 qu vars
  case res of
    Left err -> error ("doQueryWithConn: " ++ err)
    Right qu -> do
      qu' <- liftIO $ translateQuery db (keysSet vars2) qu (keysSet vars)
      stmt <- liftIO $ prepareQuery conn qu'
      dbStmtExec stmt rs
      liftIO $ catch (do
                    dbCommit conn
                    noticeM "QA" "doQuery: commit succeeded") (\e -> errorM "QA" ("doQuery: commit failed with exception " ++ show (e :: SomeException)))

newtype QueryTypeIso conn = QueryTypeIso (QueryType conn)
newtype DBQueryTypeIso db = DBQueryTypeIso (DBQueryType db)

-- class Bidirectional a b where
--  toLeft :: b -> a
--  toRight :: a -> b
