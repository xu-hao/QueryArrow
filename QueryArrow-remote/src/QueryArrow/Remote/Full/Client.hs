{-# LANGUAGE TypeFamilies, MultiParamTypeClasses, StaticPointers, RankNTypes, GADTs, ConstraintKinds, FlexibleContexts, TypeApplications, ScopedTypeVariables, FlexibleInstances, UndecidableInstances #-}

module QueryArrow.Remote.Full.Client where

import QueryArrow.DB.DB
import QueryArrow.DB.ResultStream
import QueryArrow.FO.Data
import Data.Set
import GHC.StaticPtr
import Foreign.StablePtr
import Data.Map.Strict
import Control.Monad.IO.Class
import QueryArrow.Remote.Definitions
import QueryArrow.Remote.Full.Definitions

data QueryArrowClient db a where
  QueryArrowClient :: (Channel a, SendType a ~ RemoteCommand db, ReceiveType a ~ RemoteResultSet db, IDatabase db) => db -> a -> String -> [Pred] -> Map Pred [Int] -> (db -> DBFormulaType db -> Set Var -> Bool) -> QueryArrowClient db a

getQueryArrowClient :: forall db a . (Channel a, SendType a ~ RemoteCommand db, ReceiveType a ~ RemoteResultSet db, IDatabase db) => db -> a -> IO (QueryArrowClient db a)
getQueryArrowClient _ chan = do
  DBResult db <- rpc chan GetDB
  StringResult name <- rpc chan GetName
  PredListResult preds <- rpc chan GetPreds
  PredIntListMapResult dvs <- rpc chan DeterminateVars
  SFResult sfSP <- rpc chan GetSF
  let sf = deRefStaticPtr sfSP
  return (QueryArrowClient db chan name preds dvs (sf Dict))

instance IDatabase0 (QueryArrowClient db chan) where
  type DBFormulaType (QueryArrowClient db chan) = DBFormulaType db
  getName (QueryArrowClient _ _ n _ _ _) = n
  getPreds (QueryArrowClient _ _ _ ps _ _) = ps
  determinateVars (QueryArrowClient _ _ _ _ dvf _) = dvf
  supported (QueryArrowClient db _ _ _ _ sf) = sf db

instance IDatabase1 (QueryArrowClient db chan) where
  type DBQueryType (QueryArrowClient db chan) = StablePtr (DBQueryType db)
  translateQuery (QueryArrowClient _ chan _ _ _ _) vars form vars2 = do
      send chan (TranslateQuery vars form vars2)
      QueryResult quSP <- receive chan
      return quSP

instance IDBStatement (QueryArrowClientDBStatement db chan) where
  type RowType (QueryArrowClientDBStatement db chan) = RowType (StatementType (ConnectionType db))
  dbStmtExec (QueryArrowClientDBStatement chan stmtSP) rs = do
    row <- rs
    RowListResult res <- liftIO $ rpc chan (DBStmtExec stmtSP [row])
    listResultStream res
  dbStmtClose (QueryArrowClientDBStatement chan stmtSP) = do
    UnitResult <- rpc chan (DBStmtClose stmtSP)
    return ()

instance IDBConnection0 (ConnectionType (QueryArrowClient db chan)) where
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

data QueryArrowClientDBStatement db chan where
   QueryArrowClientDBStatement :: (Channel chan, SendType chan ~ RemoteCommand db, ReceiveType chan ~ RemoteResultSet db, IDatabase db) => chan -> StablePtr (StatementType (ConnectionType db)) -> QueryArrowClientDBStatement db chan

data QueryArrowClientDBQuery db  = QueryArrowClientDBQuery   (StablePtr (QueryType (ConnectionType db)))

instance IDBConnection (ConnectionType (QueryArrowClient db chan)) where
  type QueryType (ConnectionType (QueryArrowClient db chan)) = QueryArrowClientDBQuery db
  type StatementType (ConnectionType (QueryArrowClient db chan)) = QueryArrowClientDBStatement db chan
  prepareQuery (QueryArrowClientDBConnection chan connSP) (QueryArrowClientDBQuery quSP) = do
    StatementResult stmtSP <- rpc chan (PrepareQuery connSP quSP)
    return (QueryArrowClientDBStatement chan stmtSP)

instance IDatabase2 (QueryArrowClient db chan) where
  data ConnectionType (QueryArrowClient db chan) where
     QueryArrowClientDBConnection :: (Channel chan, SendType chan ~ RemoteCommand db, ReceiveType chan ~ RemoteResultSet db, IDatabase db) => chan -> StablePtr (ConnectionType db) -> ConnectionType (QueryArrowClient db chan)
  dbOpen (QueryArrowClient _ chan _ _ _ _) = do
    ConnectionResult connSP <- rpc chan DBOpen
    return (QueryArrowClientDBConnection chan connSP)
