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

data QueryArrowClient a where
  QueryArrowClient :: a -> String -> [Pred] -> QueryArrowClient a

getQueryArrowClient :: (Channel a, SendType a ~ RemoteCommand, ReceiveType a ~ RemoteResultSet) => a -> IO (QueryArrowClient a)
getQueryArrowClient chan = do
  StringResult name <- rpc chan GetName
  PredListResult preds <- rpc chan GetPreds
  return (QueryArrowClient chan name preds)

instance (Channel a, SendType a ~ RemoteCommand, ReceiveType a ~ RemoteResultSet) => INoTranslationDatabase01 (QueryArrowClient a) where
  type NTDBFormulaType (QueryArrowClient chan) = Formula
  ntGetName (QueryArrowClient _ n _) = n
  ntGetPreds (QueryArrowClient _ _ ps) = ps
  ntSupported (QueryArrowClient chan _ _) ret form vars = True -- assuming that the server is running a TransDB

processRes :: RemoteResultSet -> IO ()
processRes res =
  case res of
    UnitResult ->
      return ()
    ErrorResult (_, err) ->
      fail err
    _ ->
      fail ("unsupported result type: " ++ show res)

instance (Channel a, SendType a ~ RemoteCommand, ReceiveType a ~ RemoteResultSet) => IDBConnection0 (ConnectionType (QueryArrowClient a)) where
  dbClose (QueryArrowClientDBConnection chan connSP) = do
    res <- rpc chan (DBClose connSP)
    processRes res
  dbBegin (QueryArrowClientDBConnection chan connSP) = do
    res <- rpc chan (DBBegin connSP)
    processRes res
  dbPrepare (QueryArrowClientDBConnection chan connSP) = do
    res <- rpc chan (DBPrepare connSP)
    processRes res
  dbCommit (QueryArrowClientDBConnection chan connSP) = do
    res <- rpc chan (DBCommit connSP)
    processRes res
  dbRollback (QueryArrowClientDBConnection chan connSP) = do
    res <- rpc chan (DBRollback connSP)
    processRes res

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
  dbOpen (QueryArrowClient chan _ _) = do
    ConnectionResult connSP <- rpc chan DBOpen
    return (QueryArrowClientDBConnection chan connSP)

instance (Channel a, SendType a ~ RemoteCommand, ReceiveType a ~ RemoteResultSet) => IDatabase (NoTranslationDatabase (QueryArrowClient  a))
