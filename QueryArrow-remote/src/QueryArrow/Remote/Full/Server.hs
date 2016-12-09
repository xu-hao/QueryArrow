{-# LANGUAGE TypeFamilies, MultiParamTypeClasses, StaticPointers, RankNTypes, GADTs, ConstraintKinds, FlexibleContexts, TypeApplications, ScopedTypeVariables, FlexibleInstances #-}

module QueryArrow.Remote.Full.Server where

import Prelude hiding (lookup)
import QueryArrow.DB.DB
import QueryArrow.DB.ResultStream
import Foreign.StablePtr
import Control.Monad.Trans.Resource
import Data.Typeable
import QueryArrow.Remote.Definitions
import QueryArrow.Remote.Full.Definitions
import Data.Map.Strict (lookup)

runQueryArrowServer :: (Typeable db, Typeable (DBFormulaType db), Channel a, SendType a ~ RemoteResultSet db, ReceiveType a ~ RemoteCommand db, IDatabase db) => a -> db -> IO ()
runQueryArrowServer chan db = do
  cmd <- receive chan
  case cmd of
    Quit -> return ()
    _ -> do
      res <- case cmd of
        GetDB -> return (DBResult db)
        GetName -> return (StringResult (getName db))
        GetPreds -> return (PredListResult (getPreds db))
        GetSF -> return (SFResult (static (\dict -> case dict of Dict -> supported)))
        TranslateQuery retvars form env -> do
          qu <- translateQuery db retvars form env
          QueryResult <$> newStablePtr qu
        DBOpen -> do
          conn <- dbOpen db
          ConnectionResult <$> newStablePtr conn
        DBClose connSP -> do
          conn <- deRefStablePtr connSP
          dbClose conn
          return UnitResult
        DBBegin connSP -> do
          conn <- deRefStablePtr connSP
          dbBegin conn
          return UnitResult
        DBPrepare connSP -> do
          conn <- deRefStablePtr connSP
          b <- dbPrepare conn
          return (BoolResult b)
        DBCommit connSP -> do
          conn <- deRefStablePtr connSP
          b <- dbCommit conn
          return (BoolResult b)
        DBRollback connSP -> do
          conn <- deRefStablePtr connSP
          dbRollback conn
          return UnitResult
        PrepareQuery connSP quSP -> do
          conn <- deRefStablePtr connSP
          qu <- deRefStablePtr quSP
          stmt <- prepareQuery conn qu
          StatementResult <$> newStablePtr stmt
        DBStmtExec stmtSP rows -> do
          stmt <- deRefStablePtr stmtSP
          let rs = dbStmtExec stmt (listResultStream rows)
          rows' <- runResourceT (getAllResultsInStream rs)
          return (RowListResult rows')
        DBStmtClose stmtSP -> do
          stmt <- deRefStablePtr stmtSP
          dbStmtClose stmt
          return UnitResult
        Quit -> error "error"
      send chan res
      runQueryArrowServer chan db
