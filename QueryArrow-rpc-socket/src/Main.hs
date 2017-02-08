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
