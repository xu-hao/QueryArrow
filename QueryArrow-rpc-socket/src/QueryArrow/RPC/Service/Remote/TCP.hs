{-# LANGUAGE OverloadedStrings, TypeFamilies #-}
module QueryArrow.RPC.Service.Remote.TCP where

import QueryArrow.DB.DB
import Control.Monad.Trans.Resource
import Control.Monad.Except
import System.Log.Logger
import Network
import QueryArrow.Control.Monad.Logger.HSLogger ()
import Control.Concurrent
import qualified Network.Socket as NS
import Control.Exception (bracket)
import Data.Aeson
import QueryArrow.RPC.Config
import QueryArrow.RPC.Service
import QueryArrow.RPC.Service.Remote.Common

data RemoteTCPRPCService = RemoteTCPRPCService

instance RPCService RemoteTCPRPCService where
  startService _ db ps0 = do
            let ps = case fromJSON ps0 of
                          Error err -> error err
                          Success ps2 -> ps2
                addr = tcp_server_addr ps
                port = tcp_server_port ps

            infoM "REMOTE_TCP_SERVER" ("listening at " ++ addr ++ ":" ++ show port)
            case db of
                AbstractDatabase tdb -> do
                    let sockHandler sock = forever $ do
                            (handle, clientAddr, clientPort) <- accept sock
                            infoM "REMOTE_TCP_SERVER" ("client connected from " ++ clientAddr ++ " " ++ show clientPort)
                            forkIO $ runResourceT $ worker2 handle tdb
                    bracket (listenOn (PortNumber (fromIntegral port))) NS.close sockHandler
