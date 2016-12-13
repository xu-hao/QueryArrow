{-# LANGUAGE FlexibleContexts, MultiParamTypeClasses, FlexibleInstances, OverloadedStrings, GADTs, ExistentialQuantification, TypeApplications, ScopedTypeVariables #-}

module QueryArrow.FFI.Service.Local where

import QueryArrow.FO.Data
import QueryArrow.DB.DB
import QueryArrow.QueryPlan
import QueryArrow.DB.ResultStream

import Prelude hiding (lookup)
import Data.Set (fromList, empty)
import Control.Monad.Trans.Resource (runResourceT)
import qualified Data.Map.Strict as Map
import Control.Monad.IO.Class (liftIO)
import System.Log.Logger (infoM)
import QueryArrow.FFI.Service
import QueryArrow.Config
import QueryArrow.DBMap
import QueryArrow.FO.Types

data Session = forall db. (IDatabaseUniformRowAndDBFormula MapResultRow Formula db) => Session db (ConnectionType db)

localService :: QueryArrowService Session
localService = QueryArrowService {
  execQuery =  \(Session db conn ) form params -> do
    liftIO $ putStrLn ("execQuery: " ++ serialize form ++ show params)
    let (varstinp, varstout) = setToMap (fromList (Map.keys params)) empty
    liftIO $ runResourceT (depleteResultStream (doQueryWithConn db conn varstout form varstinp (listResultStream [params]))),
  getAllResult = \(Session db conn ) vars form params -> do
    let (varstinp, varstout) = setToMap (fromList (Map.keys params)) (fromList vars)
    liftIO $ runResourceT (getAllResultsInStream (doQueryWithConn db conn varstout form varstinp (listResultStream [params]))),
  qasConnect = \ path -> liftIO $ do
    infoM "Plugin" ("loading configuration from " ++ path)
    ps <- getConfig path
    infoM "Plugin" ("configuration: " ++ show ps)
    db <- transDB "plugin" ps
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
