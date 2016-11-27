{-# LANGUAGE FlexibleContexts, MultiParamTypeClasses, FlexibleInstances, OverloadedStrings, GADTs, ExistentialQuantification, TypeApplications, ScopedTypeVariables #-}

module QueryArrow.FFI.Service.Local where

import FO.Data
import DB.DB
import QueryPlan
import DB.ResultStream

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
import QueryArrow.Data.Abstract
import QueryArrow.FFI.Service
import QueryArrow.Data.PredicatesGen
import Config
import Utils
import DBMap

data Session = forall db. (IDatabaseUniformRowAndDBFormula MapResultRow Formula db) => Session db (ConnectionType db) Predicates

localService :: QueryArrowService Session
localService = QueryArrowService {
  execQuery =  \(Session db conn predicates) form params -> do
    liftIO $ putStrLn ("execQuery: " ++ serialize (formula predicates form) ++ show params)
    liftIO $ runResourceT (depleteResultStream (doQueryWithConn db conn empty (formula predicates form) (fromList (Map.keys params)) (listResultStream [params]))),
  getAllResult = \(Session db conn predicates) vars form params ->
    liftIO $ runResourceT (getAllResultsInStream (doQueryWithConn db conn (fromList vars) (formula predicates form) (fromList (Map.keys params)) (listResultStream [params]))),
  getSomeResults = \ session vars form params n ->
    getAllResult localService session vars (aggregate (Limit n) form) params,
  qasConnect = \ path -> do
    liftIO $ infoM "Plugin" ("loading configuration from " ++ path)
    ps <- liftIO $ getConfig path
    liftIO $ infoM "Plugin" ("configuration: " ++ show ps)
    db <- liftIO $ transDB "plugin" ps
    case db of
        AbstractDatabase db -> do
            res <- liftIO $ try (do
                conn <- dbOpen db
                let pm = constructDBPredMap db
                return (Session db conn (predicates pm)))
            case res of
                Right a -> return a
                Left e ->
                    left (0-1, pack (show (e :: SomeException))),
  qasDisconnect = \ session@(Session _ conn _) -> do
      res <- liftIO $ try (dbClose conn)
      case res of
        Right a -> return a
        Left e -> left (0-1, pack (show (e :: SomeException))),
  qasCommit = \ session@(Session _ conn _) -> do
      res <- liftIO $ try (do
          b <- dbCommit conn
          if b
              then return ()
              else error ("qasCommit: commit error")
          )
      case res of
        Right a -> return a
        Left e -> throwError (0-1, pack (show (e :: SomeException))),

  qasRollback = \ session@(Session _ conn _) -> do
      res <- liftIO $ try (dbRollback conn)
      case res of
        Right a -> return a
        Left e -> left (0-1, pack (show (e :: SomeException)))
}
