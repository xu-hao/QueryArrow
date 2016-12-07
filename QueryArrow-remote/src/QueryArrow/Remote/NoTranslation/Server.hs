{-# LANGUAGE TypeFamilies, MultiParamTypeClasses, StaticPointers, RankNTypes, GADTs, ConstraintKinds, FlexibleContexts, TypeApplications, ScopedTypeVariables, FlexibleInstances #-}

module QueryArrow.Remote.NoTranslation.Server where

import QueryArrow.DB.DB
import QueryArrow.DB.NoTranslation
import QueryArrow.DB.ResultStream
import Foreign.StablePtr
import Control.Monad.Trans.Resource
import Control.Monad.IO.Class
import QueryArrow.Remote.NoTranslation.Definitions
import QueryArrow.Remote.Definitions
import QueryArrow.FO.Data

runQueryArrowServer :: forall db a. (Channel a, SendType a ~ RemoteResultSet, ReceiveType a ~ RemoteCommand,
        DBFormulaType db ~ Formula,
        RowType (StatementType (ConnectionType db)) ~ MapResultRow, IDatabase db) => a -> db -> ResourceT IO ()
runQueryArrowServer chan db = do
  cmd <- liftIO $ receive chan
  case cmd of
    Quit -> return ()
    _ -> do
      res <- case cmd of
        GetName -> return (StringResult (getName db))
        GetPreds -> return (PredListResult (getPreds db))
        DeterminateVars ->
            return (PredIntListMapResult (determinateVars db))
        Supported form vars ->
            return (BoolResult (supported db form vars))
        DBOpen -> do
          (conn, key) <- allocate (dbOpen db) dbClose
          liftIO $ ConnectionResult . castStablePtrToPtr <$> newStablePtr (db, conn, key)
        DBClose connP -> do
          let connSP = castPtrToStablePtr connP :: StablePtr (db, ConnectionType db, ReleaseKey)
          (_, _, key) <- liftIO $ deRefStablePtr connSP
          release key
          liftIO $ freeStablePtr connSP
          return UnitResult
        DBBegin connSP -> liftIO $ do
          (_, conn, _) <- deRefStablePtr (castPtrToStablePtr connSP :: StablePtr (db, ConnectionType db, ReleaseKey))
          dbBegin conn
          return UnitResult
        DBPrepare connSP -> liftIO $ do
          (_, conn, _) <- deRefStablePtr (castPtrToStablePtr connSP :: StablePtr (db, ConnectionType db, ReleaseKey))
          b <- dbPrepare conn
          return (BoolResult b)
        DBCommit connSP -> liftIO $ do
          (_, conn, _) <- deRefStablePtr (castPtrToStablePtr connSP :: StablePtr (db, ConnectionType db, ReleaseKey))
          b <- dbCommit conn
          return (BoolResult b)
        DBRollback connSP -> liftIO $ do
          (_, conn, _) <- deRefStablePtr (castPtrToStablePtr connSP :: StablePtr (db, ConnectionType db, ReleaseKey))
          dbRollback conn
          return UnitResult
        DBStmtExec connSP (NTDBQuery vars2 form vars) rows -> do
          (_, conn, _) <- liftIO $ deRefStablePtr (castPtrToStablePtr connSP :: StablePtr (db, ConnectionType db, ReleaseKey))
          qu <- liftIO $ translateQuery db vars2 form vars
          stmt <- liftIO $ prepareQuery conn qu
          rows' <- getAllResultsInStream (dbStmtExec stmt (listResultStream rows))
          liftIO $ dbStmtClose stmt
          return (RowListResult rows')
        Quit -> error "error"
      liftIO $ send chan res
      runQueryArrowServer chan db