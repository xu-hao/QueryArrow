{-# LANGUAGE TypeFamilies, AllowAmbiguousTypes #-}

module Main where

import Prelude hiding (lookup, length)
import Control.Monad.Trans.Resource
import Data.Time
import Control.Monad.Except
import System.Log.Logger
import QueryArrow.Logging
import Network
import QueryArrow.Control.Monad.Logger.HSLogger ()
import Control.Concurrent
import QueryArrow.RPC.Message
import QueryArrow.Config
import System.IO (Handle)
import Options.Applicative
import QueryArrow.FileSystem.Serialization
import Data.Maybe
import QueryArrow.FileSystem.Interpreter
import QueryArrow.FileSystem.Server.Config

main::IO()
main = execParser opts >>= mainArgs where
opts = info (helper <*> input) (fullDesc <> progDesc "QueryArrow description" <> header "QueryArrow header")

input :: Parser String
input = strArgument (metavar "CONFIG FILE" <> help "config file")

mainArgs :: String -> IO ()
mainArgs arg = do
  setup INFO
  ps <- getConfig arg
  runtcpmulti ps

worker :: Handle -> Interpreter -> IO ()
worker handle i = do
              t0 <- getCurrentTime
              req0 <- receiveMsg handle
              infoM "RPC_FS_SERVER" ("received message " ++ show req0)
              case fromMaybe (error "message error") req0 of
                  LocalizedCommandWrapper req -> do
                      resp <- interpreter i req
                      sendMsg handle resp
                      t1 <- getCurrentTime
                      infoM "RPC_FS_SERVER" (show (diffUTCTime t1 t0))
                      worker handle i
                  Exit ->
                      return ()

runtcpmulti :: FSServerInfo -> IO ()
runtcpmulti ps = do
  let addr = fs_server_addr ps
      port = fs_server_port ps
      protocols = fs_server_protocols ps
      root = fs_server_root ps
      i = makeLocalInterpreter addr port root
  mapM_ (\protocol ->
      case protocol of
          "tcp" -> do
              infoM "RPC_FS_SERVER" ("listening at " ++ addr ++ ":" ++ show port)
              let sockHandler sock = forever $ do
                      (handle, clientAddr, clientPort) <- accept sock
                      infoM "RPC_FS_SERVER" ("client connected from " ++ clientAddr ++ " " ++ show clientPort)
                      forkIO $ runResourceT $
                          lift $ worker handle i
              sock <- listenOn (PortNumber (fromIntegral port))
              sockHandler sock
          _ ->
              errorM "RPC_FS_SERVER" ("unsupported protocol " ++ protocol)) protocols
