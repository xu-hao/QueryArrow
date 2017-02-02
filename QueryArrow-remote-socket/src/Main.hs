{-# LANGUAGE TypeFamilies, MultiParamTypeClasses, StaticPointers, RankNTypes, GADTs, ConstraintKinds, FlexibleContexts, TypeApplications, ScopedTypeVariables, FlexibleInstances #-}

module Main where

import Control.Monad.Trans.Resource
import QueryArrow.DBMap
import QueryArrow.Remote.Channel
import QueryArrow.Remote.NoTranslation.Server

import QueryArrow.DB.DB hiding (Null)
import QueryArrow.Config

import Prelude hiding (lookup, length)
import Control.Monad.Except
import System.Log.Logger
import QueryArrow.Logging
import Network
import QueryArrow.Control.Monad.Logger.HSLogger ()
import Control.Concurrent
import System.IO (Handle, IOMode(..))
import QueryArrow.FO.Data
import qualified Network.Socket as NS
import Control.Exception (bracket)
import System.Directory (removeFile)
import Options.Applicative

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



worker :: (IDatabase db, DBFormulaType db ~ Formula, RowType (StatementType (ConnectionType db)) ~ MapResultRow) =>
          Handle -> db -> ResourceT IO ()
worker handle tdb = runQueryArrowServer (HandleChannel handle) tdb

runtcpmulti :: TranslationInfo -> IO ()
runtcpmulti ps = do
    let addr = server_addr ps
        port = server_port ps
        protocols = server_protocols ps
    mapM_ (\protocol ->
        case protocol of
            "tcp" -> do
                infoM "REMOTE_TCP_SERVER" ("listening on local port " ++ show port)
                (AbstractDatabase tdb) <- transDB ps
                let sockHandler sock = forever $ do
                        (handle, clientAddr, clientPort) <- accept sock
                        infoM "REMOTE_TCP_SERVER" ("client connected from " ++ clientAddr ++ " " ++ show clientPort)
                        forkIO $ runResourceT $ worker handle tdb
                bracket (listenOn (PortNumber (fromIntegral port))) NS.close sockHandler
            "unix domain socket" -> do
                infoM "REMOTE_UDS_SERVER" ("listening at " ++ addr)
                (AbstractDatabase tdb) <- transDB ps
                let sockHandler sock = forever $ do
                        (clientsock, clientaddr) <- NS.accept sock
                        infoM "REMOTE_UDS_SERVER" ("client connected from " ++ show clientaddr)
                        handle <- NS.socketToHandle clientsock ReadWriteMode
                        forkIO $ runResourceT $ worker handle tdb
                bracket (do
                  sock <- NS.socket NS.AF_UNIX NS.Stream NS.defaultProtocol
                  NS.bind sock (NS.SockAddrUnix addr)
                  NS.listen sock NS.maxListenQueue
                  return sock) (\sock -> do
                      NS.shutdown sock NS.ShutdownBoth
                      NS.close sock
                      removeFile addr
                      infoM "REMOTE_UDS_SERVER" "sockect shutdown and closed") sockHandler
            _ ->
                errorM "REMOTE" ("unsupported protocol " ++ protocol)) protocols
