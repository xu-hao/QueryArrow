{-# LANGUAGE TypeFamilies, MultiParamTypeClasses, StaticPointers, RankNTypes, GADTs, ConstraintKinds, FlexibleContexts, TypeApplications, ScopedTypeVariables, FlexibleInstances, UndecidableInstances #-}

module QueryArrow.Remote.NoTranslation.Client where

import QueryArrow.DB.DB
import QueryArrow.DB.NoTranslation
import QueryArrow.Semantics.ResultHeader
import QueryArrow.Semantics.ResultStream
import QueryArrow.Semantics.ResultRow.VectorResultRow
import QueryArrow.Semantics.ResultValue.AbstractResultValue
import QueryArrow.Semantics.ResultSet.ResultStreamResultSet
import QueryArrow.Semantics.ResultSet.HandleResultSet
import QueryArrow.Semantics.ResultSet.VectorResultSetTransformer
import QueryArrow.Semantics.ResultSet
import QueryArrow.Syntax.Data
import QueryArrow.Syntax.Types
import Foreign.Ptr
import Control.Monad.IO.Class
import QueryArrow.Remote.NoTranslation.Definitions
import QueryArrow.Remote.Definitions
import Data.Conduit
import Data.Conduit.List
import Data.Set (toAscList)

data QueryArrowClient a where
  QueryArrowClient :: a -> String -> [Pred] -> QueryArrowClient a

getQueryArrowClient :: (Channel a, SendType a ~ RemoteCommand, ReceiveType a ~ RemoteResultSet) => a -> IO (QueryArrowClient a)
getQueryArrowClient chan = do
  StringResult name <- rpc chan GetName
  PredListResult preds <- rpc chan GetPreds
  return (QueryArrowClient chan name preds)

instance (Channel a, SendType a ~ RemoteCommand, ReceiveType a ~ RemoteResultSet) => INoTranslationDatabase01 (QueryArrowClient a) where
  type NTDBFormulaType (QueryArrowClient chan) = FormulaT
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
      fail ("processRes: unsupported result type: " ++ show res)

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

instance (Channel a, SendType a ~ RemoteCommand, ReceiveType a ~ RemoteResultSet) => IDBStatement (ConnectionType (QueryArrowClient  a), NTDBQuery FormulaT) where
  type InputRowType (ConnectionType (QueryArrowClient  a), NTDBQuery FormulaT) = VectorResultRow AbstractResultValue
  type ResultSetType (ConnectionType (QueryArrowClient  a), NTDBQuery FormulaT) = ResultStreamResultSet (ResultSetTransformer AbstractResultValue) (VectorResultRow AbstractResultValue)
  dbStmtExec (QueryArrowClientDBConnection chan connSP, qu@(NTDBQuery ret _ _)) rset = do
      let rs = toResultStream rset
      let hdr = toHeader (toAscList ret)
      return (ResultStreamResultSet RSId hdr (ResultStream (runResultStream rs =$= awaitForever (\ row -> do
        RowListResult rows' <- liftIO $ rpc chan (DBStmtExec connSP  qu hdr [row]) -- assuming that remote will return rows with the same header
        sourceList rows'))))
  dbStmtClose _ = return ()

instance (Channel a, SendType a ~ RemoteCommand, ReceiveType a ~ RemoteResultSet) => IDBConnection (ConnectionType (QueryArrowClient  a)) where
  type StatementType (ConnectionType (QueryArrowClient  a)) = (ConnectionType (QueryArrowClient  a), NTDBQuery FormulaT)
  type QueryType (ConnectionType (QueryArrowClient  a)) = NTDBQuery FormulaT
  prepareQuery conn qu = return (conn, qu)


instance (Channel a, SendType a ~ RemoteCommand, ReceiveType a ~ RemoteResultSet) => IDatabase2 (QueryArrowClient  a) where
  data ConnectionType (QueryArrowClient a) = (SendType a ~ RemoteCommand, ReceiveType a ~ RemoteResultSet) => QueryArrowClientDBConnection a (Ptr ())
  dbOpen (QueryArrowClient chan _ _) = do
    ConnectionResult connSP <- rpc chan DBOpen
    return (QueryArrowClientDBConnection chan connSP)

instance (Channel a, SendType a ~ RemoteCommand, ReceiveType a ~ RemoteResultSet) => IDatabase (NoTranslationDatabase (QueryArrowClient  a))
