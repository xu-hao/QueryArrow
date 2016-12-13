{-# LANGUAGE TypeFamilies, MultiParamTypeClasses, StaticPointers, RankNTypes, GADTs, ConstraintKinds, FlexibleContexts, TypeApplications, ScopedTypeVariables, FlexibleInstances #-}

module Main where

import QueryArrow.DB.DB
import QueryArrow.DB.ResultStream
import Foreign.StablePtr
import Control.Monad.Trans.Resource
import Data.Typeable
import QueryArrow.DBMap
import QueryArrow.Remote.Channel
import QueryArrow.Remote.Definitions
import QueryArrow.Remote.NoTranslation.Definitions
import QueryArrow.Remote.NoTranslation.Server

import QueryArrow.DB.DB hiding (Null)
import QueryArrow.Config

import Prelude hiding (lookup, length)
import Control.Monad.Trans.Resource
import System.Environment
import Data.Time
import Control.Monad.Except
import QueryArrow.Serialization
import System.Log.Logger
import QueryArrow.Logging
import Network
import QueryArrow.Control.Monad.Logger.HSLogger ()
import Control.Concurrent
import QueryArrow.RPC.Message
import Control.Monad.Trans.Either
import QueryArrow.RPC.DB
import System.IO (Handle, IOMode(..))
import QueryArrow.FO.Data
import QueryArrow.DB.NoTranslation
import qualified Network.Socket as NS
import Control.Exception (bracket)
import System.Directory (removeFile)

main::IO()
main = do
    args <- getArgs
    mainArgs args

mainArgs :: [String] -> IO ()
mainArgs args = do
    setup INFO
    case args of
      [arg]-> do
          ps <- getConfig arg
          runtcpmulti ps
      _ ->
          putStrLn "usage: \n <command> <config file>"



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
                infoM "RPC_TCP_SERVER" ("listening at " ++ addr ++ ":" ++ show port)
                adb <- transDB "tdb" ps
                case adb of
                  AbstractDatabase tdb -> do
                    let sockHandler sock = forever $ do
                            (handle, clientAddr, clientPort) <- accept sock
                            infoM "RPC_TCP_SERVER" ("client connected from " ++ clientAddr ++ " " ++ show clientPort)
                            forkIO $ runResourceT $ worker handle tdb
                    sock <- listenOn (PortNumber (fromIntegral port))
                    sockHandler sock
            "unix domain socket" -> do
                infoM "RPC_TCP_SERVER" ("listening at " ++ addr)
                (AbstractDatabase tdb) <- transDB "tdb" ps
                let sockHandler sock = forever $ do
                        (clientsock, addr) <- NS.accept sock
                        infoM "RPC_TCP_SERVER" ("client connected from " ++ show addr)
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
                      infoM "RPC_TCP_SERVER" "sockect shutdown and closed") sockHandler
            _ ->
                errorM "RPC_TCP_SERVER" ("unsupported protocol " ++ protocol)) protocols
