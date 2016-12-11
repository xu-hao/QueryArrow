{-# LANGUAGE FlexibleContexts, MultiParamTypeClasses, FlexibleInstances, OverloadedStrings, GADTs, ExistentialQuantification, TypeApplications, ScopedTypeVariables #-}

module QueryArrow.FFI.Service.Local where

import QueryArrow.FO.Data
import QueryArrow.DB.DB
import QueryArrow.QueryPlan
import QueryArrow.DB.ResultStream

import Prelude hiding (lookup)
import Data.Set (fromList, empty)
import Data.Text (pack)
import Control.Exception (SomeException, try)
import Control.Monad.Trans.Resource (runResourceT)
import Control.Monad.Trans.Either (left)
import Control.Monad.Error.Class (throwError)
import qualified Data.Map.Strict as Map
import Control.Monad.IO.Class (liftIO)
import System.Log.Logger (infoM)
import QueryArrow.FFI.Service
import QueryArrow.Config
import QueryArrow.DBMap

data Session = forall db. (IDatabaseUniformRowAndDBFormula MapResultRow Formula db) => Session db (ConnectionType db)

localService :: QueryArrowService Session
localService = QueryArrowService {
  execQuery =  \(Session db conn ) form params -> do
    liftIO $ putStrLn ("execQuery: " ++ serialize (  form) ++ show params)
    liftIO $ runResourceT (depleteResultStream (doQueryWithConn db conn empty (  form) (fromList (Map.keys params)) (listResultStream [params]))),
  getAllResult = \(Session db conn ) vars form params ->
    liftIO $ runResourceT (getAllResultsInStream (doQueryWithConn db conn (fromList vars) (  form) (fromList (Map.keys params)) (listResultStream [params]))),
  qasConnect = \ path -> do
    liftIO $ infoM "Plugin" ("loading configuration from " ++ path)
    ps <- liftIO $ getConfig path
    liftIO $ infoM "Plugin" ("configuration: " ++ show ps)
    db <- liftIO $ transDB "plugin" ps
    case db of
        AbstractDatabase db -> do
            res <- liftIO $ try (do
                conn <- dbOpen db
                return (Session db conn))
            case res of
                Right a -> return a
                Left e ->
                    left (0-1, pack (show (e :: SomeException))),
  qasDisconnect = \ session@(Session _ conn) -> do
      res <- liftIO $ try (dbClose conn)
      case res of
        Right a -> return a
        Left e -> left (0-1, pack (show (e :: SomeException))),
  qasCommit = \ session@(Session _ conn) -> do
      res <- liftIO $ try (do
          b <- dbCommit conn
          if b
              then return ()
              else error ("qasCommit: commit error")
          )
      case res of
        Right a -> return a
        Left e -> throwError (0-1, pack (show (e :: SomeException))),

  qasRollback = \ session@(Session _ conn) -> do
      res <- liftIO $ try (dbRollback conn)
      case res of
        Right a -> return a
        Left e -> left (0-1, pack (show (e :: SomeException)))
}
