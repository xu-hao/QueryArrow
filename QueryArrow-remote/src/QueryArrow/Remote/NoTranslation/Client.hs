{-# LANGUAGE TypeFamilies, MultiParamTypeClasses, StaticPointers, RankNTypes, GADTs, ConstraintKinds, FlexibleContexts, TypeApplications, ScopedTypeVariables, FlexibleInstances, UndecidableInstances #-}

module QueryArrow.Remote.NoTranslation.Client where

import QueryArrow.DB.DB
import QueryArrow.DB.NoTranslation
import QueryArrow.DB.NoPreparedStatement
import QueryArrow.DB.ResultStream
import QueryArrow.FO.Data
import Foreign.Ptr
import Control.Monad.IO.Class
import QueryArrow.Remote.NoTranslation.Definitions
import QueryArrow.Remote.Definitions
import Control.Applicative ((<|>))
import System.IO.Unsafe (unsafePerformIO)
import Data.Map.Strict

data QueryArrowClient a where
  QueryArrowClient :: a -> String -> [Pred] -> Map Pred [Int] -> QueryArrowClient a

getQueryArrowClient :: (Channel a, SendType a ~ RemoteCommand, ReceiveType a ~ RemoteResultSet) => a -> IO (QueryArrowClient a)
getQueryArrowClient chan = do
  StringResult name <- rpc chan GetName
  PredListResult preds <- rpc chan GetPreds
  PredIntListMapResult pm <- rpc chan DeterminateVars
  return (QueryArrowClient chan name preds pm)

instance (Channel a, SendType a ~ RemoteCommand, ReceiveType a ~ RemoteResultSet) => INoTranslationDatabase01 (QueryArrowClient a) where
  type NTDBFormulaType (QueryArrowClient chan) = Formula
  ntGetName (QueryArrowClient _ n _ _) = n
  ntGetPreds (QueryArrowClient _ _ ps _) = ps
  ntDeterminateVars (QueryArrowClient chan _ _ pm) = pm
  ntSupported (QueryArrowClient chan _ _ _) form vars = True -- assuming that the server is running a TransDB

instance (Channel a, SendType a ~ RemoteCommand, ReceiveType a ~ RemoteResultSet) => IDBConnection0 (ConnectionType (QueryArrowClient  a)) where
  dbClose (QueryArrowClientDBConnection chan connSP) = do
    UnitResult <- rpc chan (DBClose connSP)
    return ()
  dbBegin (QueryArrowClientDBConnection chan connSP) = do
    UnitResult <- rpc chan (DBBegin connSP)
    return ()
  dbPrepare (QueryArrowClientDBConnection chan connSP) = do
    BoolResult b <- rpc chan (DBPrepare connSP)
    return b
  dbCommit (QueryArrowClientDBConnection chan connSP) = do
    BoolResult b <- rpc chan (DBCommit connSP)
    return b
  dbRollback (QueryArrowClientDBConnection chan connSP) = do
    UnitResult <- rpc chan (DBRollback connSP)
    return ()

instance (Channel a, SendType a ~ RemoteCommand, ReceiveType a ~ RemoteResultSet) => IDBStatement (ConnectionType (QueryArrowClient  a), NTDBQuery Formula) where
  type RowType (ConnectionType (QueryArrowClient  a), NTDBQuery Formula) = MapResultRow
  dbStmtExec (QueryArrowClientDBConnection chan connSP, qu) rows = do
      row <- rows
      RowListResult rows' <- liftIO $ rpc chan (DBStmtExec connSP qu [row])
      listResultStream rows'
  dbStmtClose _ = return ()

instance (Channel a, SendType a ~ RemoteCommand, ReceiveType a ~ RemoteResultSet) => IDBConnection (ConnectionType (QueryArrowClient  a)) where
  type StatementType (ConnectionType (QueryArrowClient  a)) = (ConnectionType (QueryArrowClient  a), NTDBQuery Formula)
  type QueryType (ConnectionType (QueryArrowClient  a)) = NTDBQuery Formula
  prepareQuery conn qu = return (conn, qu)


instance (Channel a, SendType a ~ RemoteCommand, ReceiveType a ~ RemoteResultSet) => IDatabase2 (QueryArrowClient  a) where
  data ConnectionType (QueryArrowClient a) = (SendType a ~ RemoteCommand, ReceiveType a ~ RemoteResultSet) => QueryArrowClientDBConnection a (Ptr ())
  dbOpen (QueryArrowClient chan _ _ _) = do
    ConnectionResult connSP <- rpc chan DBOpen
    return (QueryArrowClientDBConnection chan connSP)

instance (Channel a, SendType a ~ RemoteCommand, ReceiveType a ~ RemoteResultSet) => IDatabase (NoTranslationDatabase (QueryArrowClient  a))
