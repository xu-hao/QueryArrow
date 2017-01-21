{-# LANGUAGE OverloadedStrings, TypeFamilies #-}
module Main where

--import QueryArrowList
--import QueryArrow
--import Control.Arrow
--import Control.Category
--import Data.Foldable
--import qualified Data.Map as Map
--import Control.Monad.State hiding (lift)
--import Prelude hiding (id,(.), concat, foldr)
--
--graphFromList :: [CGEdge] -> CGraph
--graphFromList = foldr (\ (vout, l, vin) ->
--            insertVertex vin . insertVertex vout . insertEdge vin l (vout, l, vin) .  insertEdge vout l (vout, l, vin)
--        ) Map.empty where
--            insertVertex v m = if Map.member v m then m else Map.insert v Map.empty m
--            insertEdge v l e = Map.adjust (\ em ->
--                                     if Map.member l em then Map.adjust (\es -> es ++ [e]) l em else Map.insert l [e] em) v
--
--l = [1,2,3,1,2,3]
--
--l2 = [ x | x <- l, then group using f] where f l = [l]
--
--g :: CGraph
--g = graphFromList [("1","a","2"), ("1","a","4"), ("4","c","3"), ("4","c","5"), ("3","a","5"), ("5","d","4"), ("2", "b","3"), ("3", "c", "1")]
--
--main :: IO ()
--main =
--    let (a, _) = runCQuery (startV "1" >>> selectE "a" >>> selectOutV >>> groupCount (Kleisli (\ p ->
--            return (case p of VLeaf v -> v
--                              VCons v _ -> v)))) g () in
--        print a
import QueryArrow.DB.DB hiding (Null)
import QueryArrow.DBMap
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



worker :: (IDatabase db, DBFormulaType db ~ Formula, RowType (StatementType (ConnectionType db)) ~ MapResultRow) => Handle -> db -> ConnectionType db -> IO ()
worker handle tdb conn = do
                t0 <- getCurrentTime
                req <- receiveMsg handle
                infoM "RPC_TCP_SERVER" ("received message " ++ show req)
                (t2, t3, b) <- case req of
                    Nothing -> do
                        sendMsg handle (errorSet "cannot parser request")
                        t2 <- getCurrentTime
                        t3 <- getCurrentTime
                        return (t2, t3, False)
                    Just qs -> do
                        let qu = qsquery qs
                            hdr = qsheaders qs
                            par = qsparams qs
                        case qu of
                            Quit -> do -- disconnect when qu field is null
                                t2 <- getCurrentTime
                                t3 <- getCurrentTime
                                return (t2, t3, True)
                            Static qu -> do
                                t2 <- getCurrentTime
                                ret <- runEitherT (run3 hdr qu par tdb conn)
                                t3 <- getCurrentTime
                                case ret of
                                    Left e ->
                                        sendMsg handle (errorSet e)
                                    Right rep ->
                                        sendMsg handle (resultSet rep)
                                return (t2, t3, False)
                            Dynamic qu -> do
                                t2 <- getCurrentTime
                                ret <- runEitherT (run hdr qu par tdb conn)
                                t3 <- getCurrentTime
                                case ret of
                                    Left e ->
                                        sendMsg handle (errorSet e)
                                    Right rep ->
                                        sendMsg handle (resultSet rep)
                                return (t2, t3, False)
                t1 <- getCurrentTime
                infoM "RPC_TCP_SERVER" (show (diffUTCTime t1 t0) ++ "\npre: " ++ show (diffUTCTime t2 t0) ++ "\nquery: " ++ show (diffUTCTime t3 t2) ++ "\npost: " ++ show (diffUTCTime t1 t3))
                if b
                    then return ()
                    else worker handle tdb conn

runtcpmulti :: TranslationInfo -> IO ()
runtcpmulti ps = do
    let addr = server_addr ps
        port = server_port ps
        protocols = server_protocols ps
    mapM_ (\protocol ->
        case protocol of
            "tcp" -> do
                infoM "RPC_TCP_SERVER" ("listening at " ++ addr ++ ":" ++ show port)
                (AbstractDatabase tdb) <- transDB "tdb" ps
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
            "unix domain socket" -> do
                infoM "RPC_TCP_SERVER" ("listening at " ++ addr)
                (AbstractDatabase tdb) <- transDB "tdb" ps
                let sockHandler sock = forever $ do
                        (clientsock, addr) <- NS.accept sock
                        infoM "RPC_TCP_SERVER" ("client connected from " ++ show addr)
                        handle <- NS.socketToHandle clientsock ReadWriteMode
                        forkIO $ runResourceT $ do
                            (_, conn) <- allocate (dbOpen tdb) (\db -> do
                                        dbClose db
                                        infoM "RPC_TCP_SERVER" ("client disconnected"))
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
            _ ->
                errorM "RPC_TCP_SERVER" ("unsupported protocol " ++ protocol)) protocols
