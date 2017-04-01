{-# LANGUAGE ExistentialQuantification, MultiParamTypeClasses, TypeSynonymInstances, FlexibleInstances, FlexibleContexts, TypeFamilies, ScopedTypeVariables #-}
module QueryArrow.DB.DB where

import QueryArrow.Semantics.ResultRow
import QueryArrow.Semantics.ResultStream
import QueryArrow.Syntax.Data

import Prelude  hiding (lookup, null)
import Data.Map.Strict (keysSet, unionWithKey)
import Control.Monad.Except
import Data.Set (Set)
import System.Log.Logger (errorM, noticeM)
import QueryArrow.Syntax.Types
import QueryArrow.Semantics.ResultSet
import Control.Exception
import Data.Conduit



-- query

data Command = Begin | Prepare | Commit | Rollback | Execute Formula

{-
    class A a b | a -> b
    class B a b | a -> b

    data C a = C a

    instance B a b => A (C a) b
-}


class (ResultSet (ResultSetType stmt), ResultSetRowType (ResultSetType stmt) ~ RowType stmt) => IDBStatement stmt where
    type InputRowType stmt
    type RowType stmt
    type ResultSetType stmt
    -- give a stream of stores, return a stream of combined stores
    dbStmtExec :: (ResultSet b, ResultSetRowType b ~ InputRowType stmt) => stmt -> b -> IO (ResultSetType stmt)
    dbStmtClose :: stmt -> IO ()

data AbstractDBStatement trans inputrow row = forall stmt. (IDBStatement stmt, trans ~ ResultSetTransType (ResultSetType stmt), row ~ RowType stmt, inputrow ~ InputRowType stmt) => AbstractDBStatement {unAbstractDBStatement :: stmt}


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

class IDatabase0 db => IDatabase1 db where
    type DBQueryType db
    translateQuery :: db -> Set Var -> DBFormulaType db -> Set Var -> IO (DBQueryType db)

class (IDBConnection (ConnectionType db)) => IDatabase2 db where
    data ConnectionType db
    dbOpen :: db -> IO (ConnectionType db)

class (IDatabase0 db, IDatabase1 db, IDatabase2 db, DBQueryType db ~ QueryType (ConnectionType db)) => IDatabase db where

-- https://wiki.haskell.org/Existential_type#Dynamic_dispatch_mechanism_of_OOP
data AbstractDatabase trans row form = forall db. (IDatabase db, row ~ RowType (StatementType (ConnectionType db)), form ~ DBFormulaType db, ResultSetTransType (ResultSetType (StatementType (ConnectionType db))) ~ trans, InputRowType (StatementType (ConnectionType db)) ~ row) => AbstractDatabase { unAbstractDatabase :: db }

instance IDatabase0 (AbstractDatabase trans row form) where
    type DBFormulaType (AbstractDatabase trans row form) = form
    getName (AbstractDatabase db) = getName db
    getPreds (AbstractDatabase db) = getPreds db
    supported (AbstractDatabase db) = supported db

checkQuery :: (IDatabase db) => db -> VarTypeMap -> Formula -> VarTypeMap -> Either String FormulaT
checkQuery db vars2 qu vars = do
  let preds = getPreds db
  let ptm = constructPredTypeMap preds
  let vtm = unionWithKey (\k l r -> if l == r then l else error ("translateQuery: input output var has different types" ++ show k)) vars2 vars
  typeCheckFormula ptm vtm qu

-- | Runs query/update and returns a result set, without commit. The result set may pull rows lazily from the connection.
doQueryWithConnResultSet :: forall db a . (IDatabase db, DBFormulaType db ~ FormulaT, ResultSet a, ResultSetRowType a ~ InputRowType (StatementType (ConnectionType db))) =>
                    db -> ConnectionType db -> VarTypeMap -> Formula -> VarTypeMap -> a ->
                    IO (ResultSetType (StatementType (ConnectionType db)))
doQueryWithConnResultSet db conn vars2 qu vars  rs = do
  let res = checkQuery db vars2 qu vars
  case res of
    Left err -> error ("doQueryWithConn: " ++ err)
    Right qu -> do
      qu' <- translateQuery db (keysSet vars2) qu (keysSet vars)
      stmt <- prepareQuery conn qu'
      dbStmtExec stmt rs

doQuery :: (IDatabase db, DBFormulaType db ~ FormulaT, ResultSet a, ResultSetRowType a ~ InputRowType (StatementType (ConnectionType db))) =>
            db -> VarTypeMap -> Formula -> VarTypeMap -> a ->
            IO (HeaderType (RowType (StatementType (ConnectionType db))), DBResultStream (RowType (StatementType (ConnectionType db))))
doQuery db vars2 qu vars rs = do
        conn <- dbOpen db
        (hdr, ResultStream rs2) <- onException (doQueryWithConn db conn vars2 qu vars rs) (dbClose conn)
        return (hdr, ResultStream (bracketP (return conn) dbClose (const rs2)))

-- | Runs query/update and returns a result stream. Adds commit at the end of the stream.
doQueryWithConn :: forall db a . (IDatabase db, DBFormulaType db ~ FormulaT, ResultSet a, ResultSetRowType a ~ InputRowType (StatementType (ConnectionType db))) =>
                    db -> ConnectionType db -> VarTypeMap -> Formula -> VarTypeMap -> a ->
                    IO (HeaderType (RowType (StatementType (ConnectionType db))), DBResultStream (RowType (StatementType (ConnectionType db))))
doQueryWithConn db conn vars2 qu vars  rs = do
      rset <- doQueryWithConnResultSet db conn vars2 qu vars rs
      let hdr = getHeader rset
      let (ResultStream rs2) = toResultStream rset
      return (hdr, ResultStream (do
          rs2
          liftIO $ catch (do
                        dbCommit conn
                        noticeM "QA" "doQuery: commit succeeded") (\e -> errorM "QA" ("doQuery: commit failed with exception " ++ show (e :: SomeException)))))

newtype QueryTypeIso conn = QueryTypeIso (QueryType conn)
newtype DBQueryTypeIso db = DBQueryTypeIso (DBQueryType db)

-- class Bidirectional a b where
--  toLeft :: b -> a
--  toRight :: a -> b
