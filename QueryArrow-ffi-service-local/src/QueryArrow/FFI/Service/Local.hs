{-# LANGUAGE FlexibleContexts, MultiParamTypeClasses, FlexibleInstances, OverloadedStrings, GADTs, ExistentialQuantification, TypeApplications, ScopedTypeVariables #-}

module QueryArrow.FFI.Service.Local where

import QueryArrow.Syntax.Data
import QueryArrow.DB.DB
import QueryArrow.QueryPlan
import QueryArrow.Semantics.ResultStream
import QueryArrow.Semantics.ResultRow.VectorResultRow
import QueryArrow.Semantics.ResultValue.AbstractResultValue

import Prelude hiding (lookup)
import Data.Set (fromList, empty)
import Control.Monad.Trans.Resource (runResourceT)
import qualified Data.Map.Strict as Map
import Control.Monad.IO.Class (liftIO)
import System.Log.Logger (infoM)
import QueryArrow.FFI.Service
import QueryArrow.Config
import QueryArrow.DBMap
import QueryArrow.Syntax.Types
import qualified Data.Vector as V
import QueryArrow.Semantics.ResultSet.ResultStreamResultSet
import QueryArrow.Semantics.ResultSet.VectorResultSetTransformer
import Data.Conduit

data Session = forall db. (IDatabaseUniformRowAndDBFormula (ResultSetTransformer AbstractResultValue) (VectorResultRow AbstractResultValue) FormulaT db) => Session db (ConnectionType db)

localService :: QueryArrowService Session
localService = QueryArrowService {
  execQuery =  \(Session db conn ) form hdr params -> do
    -- liftIO $ putStrLn ("execQuery: " ++ serialize form ++ show params)
    let (varstinp, varstout) = setToMap (fromList (V.toList hdr)) empty
    (_, rset) <- liftIO $ doQueryWithConn db conn varstout form varstinp (ResultStreamResultSet RSId hdr (ResultStream (yield params)))
    liftIO $ runResourceT (depleteResultStream rset),
  getAllResult = \(Session db conn ) vars form hdr params -> do
    let (varstinp, varstout) = setToMap (fromList (V.toList hdr)) (fromList vars)
    (_, rset) <- liftIO $ doQueryWithConn db conn varstout form varstinp (ResultStreamResultSet RSId hdr (ResultStream (yield params)))
    liftIO $ runResourceT (getAllResultsInStream rset),
  qasConnect = \ path -> liftIO $ do
    infoM "Plugin" ("loading configuration from " ++ path)
    ps <- getConfig path
    infoM "Plugin" ("configuration: " ++ show ps)
    db <- transDB ps
    case db of
        AbstractDatabase db -> do
            conn <- dbOpen db
            return (Session db conn),
  qasDisconnect = \ (Session _ conn) ->
            liftIO $ dbClose conn,
  qasCommit = \ (Session _ conn) ->
          liftIO $ dbCommit conn,
  qasRollback = \ (Session _ conn) ->
      liftIO $ dbRollback conn,
  qasPrepare = \ (Session _ conn) ->
      liftIO $ dbPrepare conn,
  qasBegin = \ (Session _ conn) ->
      liftIO $ dbBegin conn
}
