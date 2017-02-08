{-# LANGUAGE OverloadedStrings, TypeFamilies #-}
module QueryArrow.RPC.Service.FileSystem.TCP where

import Control.Monad.Trans.Resource
import Data.Time
import Control.Monad.Except
import System.Log.Logger
import Network
import QueryArrow.Control.Monad.Logger.HSLogger ()
import Control.Concurrent
import QueryArrow.RPC.Message
import System.IO (Handle)
import Data.Aeson
import Data.Maybe
import QueryArrow.RPC.Config
import QueryArrow.FileSystem.Serialization
import QueryArrow.FileSystem.Interpreter
import QueryArrow.RPC.Service

worker3 :: Handle -> Interpreter -> IO ()
worker3 handle i = do
  t0 <- getCurrentTime
  req0 <- receiveMsg handle
  infoM "RPC_FS_SERVER" ("received message " ++ show req0)
  case fromMaybe (error "message error") req0 of
    LocalizedCommandWrapper req -> do
      resp <- interpreter i req
      sendMsg handle resp
      t1 <- getCurrentTime
      infoM "RPC_FS_SERVER" (show (diffUTCTime t1 t0))
      worker3 handle i
    Exit ->
      return ()

data FileSystemRPCService = FileSystemRPCService

instance RPCService FileSystemRPCService where
  startService _ _ ps0 = do
            let ps = case fromJSON ps0 of
                          Error err -> error err
                          Success ps2 -> ps2
            let addr = fs_server_addr ps
                port = fs_server_port ps
                root = fs_server_root ps
                i = makeLocalInterpreter addr port root
            infoM "RPC_FS_SERVER" ("listening at " ++ addr ++ ":" ++ show port)
            let sockHandler sock = forever $ do
                    (handle, clientAddr, clientPort) <- accept sock
                    infoM "RPC_FS_SERVER" ("client connected from " ++ clientAddr ++ " " ++ show clientPort)
                    forkIO $ runResourceT $
                        lift $ worker3 handle i
            sock <- listenOn (PortNumber (fromIntegral port))
            sockHandler sock
