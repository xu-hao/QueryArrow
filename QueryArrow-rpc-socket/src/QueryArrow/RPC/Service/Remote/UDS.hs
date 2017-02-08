{-# LANGUAGE OverloadedStrings, TypeFamilies #-}
module QueryArrow.RPC.Service.Remote.UDS where

import QueryArrow.DB.DB
import Control.Monad.Trans.Resource
import Control.Monad.Except
import System.Log.Logger
import QueryArrow.Control.Monad.Logger.HSLogger ()
import Control.Concurrent
import System.IO (IOMode(..))
import qualified Network.Socket as NS
import Control.Exception (bracket)
import System.Directory (removeFile)
import Data.Aeson
import QueryArrow.RPC.Config
import QueryArrow.RPC.Service.Remote.Common
import QueryArrow.RPC.Service

data RemoteUDSRPCService = RemoteUDSRPCService

instance RPCService RemoteUDSRPCService where
  startService _ db ps0 = do
      let ps = case fromJSON ps0 of
                    Error err -> error err
                    Success ps2 -> ps2
          addr = uds_server_addr ps
      infoM "REMOTE_UDS_SERVER" ("listening at " ++ addr)
      case db of
          AbstractDatabase tdb -> do
              let sockHandler sock = forever $ do
                      (clientsock, clientaddr) <- NS.accept sock
                      infoM "REMOTE_UDS_SERVER" ("client connected from " ++ show clientaddr)
                      handle <- NS.socketToHandle clientsock ReadWriteMode
                      forkIO $ runResourceT $ worker2 handle tdb
              bracket (do
                sock <- NS.socket NS.AF_UNIX NS.Stream NS.defaultProtocol
                NS.bind sock (NS.SockAddrUnix addr)
                NS.listen sock NS.maxListenQueue
                return sock) (\sock -> do
                    NS.shutdown sock NS.ShutdownBoth
                    NS.close sock
                    removeFile addr
                    infoM "REMOTE_UDS_SERVER" "sockect shutdown and closed") sockHandler
