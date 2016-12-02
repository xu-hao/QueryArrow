{-# LANGUAGE FlexibleContexts, MultiParamTypeClasses, FlexibleInstances, OverloadedStrings, GADTs, ExistentialQuantification, TypeApplications, ScopedTypeVariables, DeriveGeneric #-}

module QueryArrow.FFI.Service.TCP where

import QueryArrow.FO.Data
import QueryArrow.DB.DB

import Prelude hiding (lookup)
import Data.Set (fromList)
import Data.Text (pack)
import Control.Exception (SomeException, try)
import Control.Monad.Error.Class (throwError)
import Control.Monad.IO.Class (liftIO)
import System.Log.Logger (infoM, errorM)
import Data.Aeson
import QueryArrow.FFI.Service
import QueryArrow.Data.PredicatesGen
import QueryArrow.RPC.Message
import QueryArrow.Config
import QueryArrow.DBMap
import QueryArrow.Serialization
import GHC.Generics
import System.IO (Handle)
import qualified Data.ByteString.Lazy.Char8 as BS
import Network

data TcpServiceSession = TcpServiceSession Handle Predicates

data TcpClientConfig = TcpClientConfig {
    tcpServerAddr :: String,
    tcpServerPort :: Int
} deriving (Generic, Show)

instance FromJSON TcpClientConfig

tcpService :: String -> QueryArrowService TcpServiceSession
tcpService path0 =
  let   getAllResult0 = \(TcpServiceSession  handle _) vars form params -> do
            let name = QuerySet {
                          qsquery = Static [Execute form],
                          qsheaders = fromList vars,
                          qsparams = params
                          }
            liftIO $ sendMsg handle name
            rep <- liftIO $ receiveMsg handle
            case rep of
                Just (ResultSet err results) ->
                    if null err
                      then
                          return results
                      else
                          throwError (-1, pack err)
                Nothing ->
                    throwError (-1, "cannot parse response")
  in
          QueryArrowService {
            getPredicates = \(TcpServiceSession  handle pm) -> pm,
            execQuery =  \(TcpServiceSession  handle _) form params -> do
              let name = QuerySet {
                            qsquery = Static [Execute (form )],
                            qsheaders = mempty,
                            qsparams = params
                            }
              liftIO $ sendMsg handle name
              rep <- liftIO $ receiveMsg handle
              case rep of
                  Just (ResultSet err results) ->
                      if null err
                        then
                            return ()
                        else
                            throwError (-1, pack err)
                  Nothing ->
                      throwError (-1, "cannot parse response"),
            getAllResult = getAllResult0,
            getSomeResults = \ session vars form params n ->
              getAllResult0 session vars (Aggregate (Limit n) form) params,
            qasConnect = \ path -> do
              liftIO $ infoM "TCP Service" ("parsing configuration from " ++ path)
              let ps = decode (BS.pack path) :: Maybe TcpClientConfig
              case ps of
                  Nothing -> do
                      let msg = "cannot parser json"
                      liftIO $ errorM "TCP Service" msg
                      throwError (-1, pack msg)
                  Just ps -> do
                      ps2 <- liftIO $ getConfig path0
                      (AbstractDatabase db) <- liftIO $ transDB "preds" ps2
                      let pm = predicates (constructPredicateMap (getPreds db))
                      handle <- liftIO $ try (connectTo (tcpServerAddr ps) (PortNumber (fromIntegral (tcpServerPort ps))))
                      case handle of
                        Left e -> throwError (-1, pack (show (e :: SomeException)))
                        Right handle ->
                          return (TcpServiceSession  handle  pm),
            qasDisconnect = \ session@(TcpServiceSession handle _) -> do
              let name2 = QuerySet {
                            qsquery = Quit,
                            qsheaders = mempty,
                            qsparams = mempty
                            }
              liftIO $ sendMsg handle name2,
            qasCommit = \ session@(TcpServiceSession handle _) -> do
              let name2 = QuerySet {
                            qsquery = Static [Commit],
                            qsheaders = mempty,
                            qsparams = mempty
                            }
              liftIO $ sendMsg handle name2
              rep <- liftIO $ receiveMsg handle
              case rep of
                  Just (ResultSet err results) ->
                      if null err
                        then
                            return ()
                        else
                            throwError (-1, pack err)
                  Nothing ->
                      throwError (-1, "cannot parse response"),

            qasRollback = \ session@(TcpServiceSession handle _) -> do
              let name2 = QuerySet {
                            qsquery = Static [Rollback],
                            qsheaders = mempty,
                            qsparams = mempty
                            }
              liftIO $ sendMsg handle name2
              rep <- liftIO $ receiveMsg handle
              case rep of
                  Just (ResultSet err results) ->
                      if null err
                        then
                            return ()
                        else
                            throwError (-1, pack err)
                  Nothing ->
                      throwError (-1, "cannot parse response")
          }
