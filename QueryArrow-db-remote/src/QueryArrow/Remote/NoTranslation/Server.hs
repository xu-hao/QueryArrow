{-# LANGUAGE TypeFamilies, MultiParamTypeClasses, StaticPointers, RankNTypes, GADTs, ConstraintKinds, FlexibleContexts, TypeApplications, ScopedTypeVariables, FlexibleInstances #-}

module QueryArrow.Remote.NoTranslation.Server where

import QueryArrow.DB.DB
import QueryArrow.DB.NoTranslation
import QueryArrow.Semantics.ResultSet.ResultStreamResultSet
import QueryArrow.Semantics.ResultSet.VectorResultSetTransformer
import QueryArrow.Semantics.ResultSet
import QueryArrow.Semantics.ResultStream
import QueryArrow.Semantics.ResultRow.VectorResultRow
import QueryArrow.Semantics.ResultValue.AbstractResultValue
import Foreign.StablePtr
import Control.Monad.Trans.Resource
import Control.Monad.IO.Class
import QueryArrow.Remote.NoTranslation.Definitions
import QueryArrow.Remote.Definitions
import QueryArrow.Syntax.Types
import Control.Exception.Lifted (catch, SomeException)
import Data.Conduit.List (sourceList)


runQueryArrowServer :: forall db a. (Channel a, SendType a ~ RemoteResultSet, ReceiveType a ~ RemoteCommand,
        DBFormulaType db ~ FormulaT,
        ResultSetRowType (ResultSetType (StatementType (ConnectionType db))) ~ VectorResultRow AbstractResultValue,
        ResultSetTransType (ResultSetType (StatementType (ConnectionType db))) ~ ResultSetTransformer AbstractResultValue,
        InputRowType (StatementType (ConnectionType db)) ~ VectorResultRow AbstractResultValue,
        IDatabase db) => a -> db -> ResourceT IO ()
runQueryArrowServer chan db = do
  cmd <- liftIO $ receive chan
  case cmd of
    Quit -> return ()
    _ -> do
      res <- case cmd of
        GetName -> return (StringResult (getName db))
        GetPreds -> return (PredListResult (getPreds db))
        Supported ret form vars ->
            return (BoolResult (supported db ret form vars))
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
          catch (do
            dbBegin conn
            return UnitResult
            ) (\e -> return (ErrorResult (-1, show (e::SomeException))))
        DBPrepare connSP -> liftIO $
          catch (do
              (_, conn, _) <- deRefStablePtr (castPtrToStablePtr connSP :: StablePtr (db, ConnectionType db, ReleaseKey))
              dbPrepare conn
              return UnitResult
              ) (\e -> return (ErrorResult (-1, show (e::SomeException))))
        DBCommit connSP -> liftIO $
          catch (do
              (_, conn, _) <- deRefStablePtr (castPtrToStablePtr connSP :: StablePtr (db, ConnectionType db, ReleaseKey))
              dbCommit conn
              return UnitResult
              ) (\e -> return (ErrorResult (-1, show (e::SomeException))))
        DBRollback connSP -> liftIO $
          catch (do
              (_, conn, _) <- deRefStablePtr (castPtrToStablePtr connSP :: StablePtr (db, ConnectionType db, ReleaseKey))
              dbRollback conn
              return UnitResult
              ) (\e -> return (ErrorResult (-1, show (e::SomeException))))
        DBStmtExec connSP (NTDBQuery vars2 form vars) hdr rows ->
          catch (do
            (_, conn, _) <- liftIO $ deRefStablePtr (castPtrToStablePtr connSP :: StablePtr (db, ConnectionType db, ReleaseKey))
            qu <- liftIO $ translateQuery db vars2 form vars
            stmt <- liftIO $ prepareQuery conn qu
            rset <- liftIO $ dbStmtExec stmt (ResultStreamResultSet RSId hdr (ResultStream (sourceList rows)))
            rows' <- getAllResultsInStream (toResultStream rset)
            liftIO $ dbStmtClose stmt
            return (RowListResult rows')
            ) (\e -> return (ErrorResult (-1, show (e::SomeException))))
        Quit -> error "error"
      liftIO $ send chan res
      runQueryArrowServer chan db
