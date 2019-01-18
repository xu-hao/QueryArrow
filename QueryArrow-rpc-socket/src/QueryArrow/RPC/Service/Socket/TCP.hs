{-# LANGUAGE OverloadedStrings, TypeFamilies #-}
module QueryArrow.RPC.Service.Socket.TCP where

import QueryArrow.DB.DB

import Control.Monad.Trans.Resource
import Control.Monad.Except
import System.Log.Logger
import Network
import QueryArrow.Control.Monad.Logger.HSLogger ()
import Control.Concurrent
import Data.Aeson
import QueryArrow.RPC.Config
import QueryArrow.RPC.Service
import QueryArrow.RPC.Service.Socket.Common

data ServiceTCPRPCService = ServiceTCPRPCService

instance RPCService ServiceTCPRPCService where
  startService _ db ps0 = do
      let ps = case fromJSON ps0 of
                Error err -> error err
                Success ps2 -> ps2
          addr = tcp_server_addr ps
          port = tcp_server_port ps

      infoM "RPC_TCP_SERVER" ("listening at " ++ addr ++ ":" ++ show port)
      case db of
          AbstractDatabase tdb -> do
              let sockHandler sock = forever $ do
                      (handle, clientAddr, clientPort) <- accept sock
                      infoM "RPC_TCP_SERVER" ("client connected from " ++ addr ++ " " ++ show port)
                      forkIO $ runResourceT $ do
                          (_, conn) <- allocate (dbOpen tdb) (\db -> do
                                      dbClose db
                                      infoM "RPC_TCP_SERVER" ("client disconnected"))
                          lift $ worker handle tdb conn
              sock <- listenOn (PortNumber (fromIntegral port))
              sockHandler sock
