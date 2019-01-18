{-# LANGUAGE OverloadedStrings, TypeFamilies #-}
module QueryArrow.RPC.Service.Socket.UDS where

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
import QueryArrow.RPC.Service
import QueryArrow.RPC.Service.Socket.Common

data ServiceUDSRPCService = ServiceUDSRPCService

instance RPCService ServiceUDSRPCService where
  startService _ db ps0 = do
            let ps = case fromJSON ps0 of
                          Error err -> error err
                          Success ps2 -> ps2
                addr = uds_server_addr ps
            infoM "RPC_UDS_SERVER" ("listening at " ++ addr)
            case db of
                AbstractDatabase tdb -> do
                    let sockHandler sock = forever $ do
                            (clientsock, addr) <- NS.accept sock
                            infoM "RPC_UDS_SERVER" ("client connected from " ++ show addr)
                            handle <- NS.socketToHandle clientsock ReadWriteMode
                            forkIO $ runResourceT $ do
                                (_, conn) <- allocate (dbOpen tdb) (\db -> do
                                            dbClose db
                                            infoM "RPC_UDS_SERVER" ("client disconnected"))
                                lift $ worker handle tdb conn
                    bracket (do
                      sock <- NS.socket NS.AF_UNIX NS.Stream NS.defaultProtocol
                      NS.bind sock (NS.SockAddrUnix addr)
                      NS.listen sock NS.maxListenQueue
                      return sock) (\sock -> do
                          NS.shutdown sock NS.ShutdownBoth
                          NS.close sock
                          removeFile addr
                          infoM "RPC_TCP_SERVER" "sockect shutdown and closed") sockHandler
