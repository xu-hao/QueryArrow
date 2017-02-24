{-# LANGUAGE OverloadedStrings, TypeFamilies #-}
module Main where

import QueryArrow.DB.DB
import QueryArrow.DBMap
import QueryArrow.Config

import Prelude hiding (lookup)
import System.Log.Logger
import QueryArrow.Logging
import QueryArrow.Control.Monad.Logger.HSLogger ()
import QueryArrow.FO.Data
import Options.Applicative hiding (Success)
import Data.Maybe
import Data.Map.Strict
import QueryArrow.RPC.Service
import QueryArrow.RPC.Service.Service.TCP
import QueryArrow.RPC.Service.Service.UDS
import QueryArrow.RPC.Service.Remote.TCP
import QueryArrow.RPC.Service.Remote.UDS
import QueryArrow.RPC.Service.Service.HTTP
import QueryArrow.RPC.Service.FileSystem.TCP
import Data.Monoid ((<>))

main::IO()
main = execParser opts >>= mainArgs where
  opts = info (helper <*> input) (fullDesc <> progDesc "QueryArrow description" <> header "QueryArrow header")

input :: Parser String
input = strArgument (metavar "CONFIG FILE" <> help "config file")

mainArgs :: String -> IO ()
mainArgs arg = do
    setup INFO
    ps <- getConfig arg
    tdb <- transDB ps
    runtcpmulti tdb (servers ps)

serviceMap :: Map String AbstractRPCService
serviceMap = fromList [
    ("service/tcp", AbstractRPCService ServiceTCPRPCService),
    ("service/unix domain socket", AbstractRPCService ServiceUDSRPCService),
    ("service/http", AbstractRPCService ServiceHTTPRPCService),
    ("remote/tcp", AbstractRPCService RemoteTCPRPCService),
    ("remote/unix domain socket", AbstractRPCService RemoteUDSRPCService),
    ("file system/tcp", AbstractRPCService FileSystemRPCService)
    ]

runtcpmulti :: AbstractDatabase MapResultRow Formula -> [DBServer] -> IO ()
runtcpmulti db pss = mapM_ (\ps0 -> do
    let protocol = server_protocol ps0
    case lookup protocol serviceMap of
      Just (AbstractRPCService service) ->
          startService service db (fromJust (server_config ps0))
      Nothing ->
          errorM "RPC_TCP_SERVER" ("unsupported protocol " ++ protocol)) pss
