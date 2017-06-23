{-# LANGUAGE OverloadedStrings, TypeFamilies #-}
module Main where

import QueryArrow.DB.DB
import QueryArrow.DBMap
import QueryArrow.Config

import Prelude hiding (lookup)
import System.Log.Logger
import QueryArrow.Logging
import QueryArrow.Control.Monad.Logger.HSLogger ()
import QueryArrow.FO.Types
import Options.Applicative hiding (Success)
import Data.Maybe
import Data.Map.Strict
import QueryArrow.RPC.Service
import QueryArrow.RPC.Service.Service.TCP
import QueryArrow.RPC.Service.Service.UDS
import QueryArrow.RPC.Service.Service.HTTP
import QueryArrow.RPC.Service.FileSystem.TCP
import Data.Monoid ((<>))

main::IO()
main = execParser opts >>= mainArgs where
  opts = info (helper <*> input) (fullDesc <> progDesc "QueryArrow description" <> header "QueryArrow header")

input :: Parser (String, Maybe Priority, Maybe String)
input = (,,) <$> strArgument (metavar "CONFIG FILE" <> help "config file")
            <*> optional (option auto (long "log-level" <> short 'v' <> metavar "LOG LEVEL" <> help "log level"))
            <*> optional (strOption (long "log-test" <> short 'l' <> metavar "LOG TEST" <> help "log test"))

mainArgs :: (String, Maybe Priority, Maybe String) -> IO ()
mainArgs (arg, ps2, ps3) = do
    setup (fromMaybe INFO ps2) ps3
    ps <- getConfig arg
    tdb <- transDB ps
    runtcpmulti tdb (servers ps)

serviceMap :: Map String AbstractRPCService
serviceMap = fromList [
    ("service/tcp", AbstractRPCService ServiceTCPRPCService),
    ("service/unix domain socket", AbstractRPCService ServiceUDSRPCService),
    ("service/http", AbstractRPCService ServiceHTTPRPCService),
    ("file system/tcp", AbstractRPCService FileSystemRPCService)
    ]

runtcpmulti :: AbstractDatabase MapResultRow FormulaT -> [DBServer] -> IO ()
runtcpmulti db pss = mapM_ (\ps0 -> do
    let protocol = server_protocol ps0
    case lookup protocol serviceMap of
      Just (AbstractRPCService service) ->
          startService service db (fromJust (server_config ps0))
      Nothing ->
          errorM "RPC_TCP_SERVER" ("unsupported protocol " ++ protocol)) pss
