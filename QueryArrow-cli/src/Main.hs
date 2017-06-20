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
import qualified Data.Map.Strict as Map
import System.Log.Logger
import QueryArrow.Logging
import Control.Monad.Trans.Either
import QueryArrow.RPC.DB
import Options.Applicative
import Data.Maybe (fromMaybe, isJust, fromJust)
import System.IO
import Control.Exception
import QueryArrow.Serialization
import Network.Socket
import Network
import QueryArrow.RPC.Message
import QueryArrow.FO.Data
import Data.Set (fromList)
import Control.Arrow ((***))
import Data.Monoid ((<>))
import QueryArrow.Client

import QueryArrow.Utils

main::IO()
main = execParser opts >>= mainArgs where
  opts = info (helper <*> input) (fullDesc <> progDesc "QueryArrow description" <> header "QueryArrow header")

data Input = Input {
  query:: String,
  headers::  String,
  configFile:: Maybe String,
  verbose :: Bool,
  showHeaders :: Bool,
  tcpAddr :: Maybe String,
  tcpPort :: Maybe Int,
  udsAddr :: Maybe String,
  params ::  [(String, ConcreteResultValue)]
}
input :: Parser Input
input = Input <$>
            strArgument (metavar "QUERY" <> help "query") <*>
            strArgument (metavar "HEADERS" <> help "headers") <*>
            optional (strOption (long "config" <> short 'c' <> metavar "CONFIG" <> help "config file")) <*>
            switch (long "verbose" <> short 'v' <> help "verbose") <*>
            switch (long "show-headers" <> short 's' <> help "show headers") <*>
            optional (strOption (long "tcpaddr" <> metavar "TCPADDR" <> help "tcp address")) <*>
            optional (option auto (long "tcpport" <> metavar "TCPPORT" <> help "tcp port")) <*>
            optional (strOption (long "udsaddr" <> metavar "UDSADDR" <> help "unix domain socket address")) <*>
            many (option auto (long "params" <> short 'p' <> metavar "PARAMS" <> help "parameters"))

mainArgs :: Input -> IO ()
mainArgs input = do
    setup (if verbose input then INFO else WARNING)
    ps <- getConfig (fromMaybe "/etc/QueryArrow/tdb-plugin-gen-abs.yaml" (configFile input))
    let hdr = words (headers input)
    let qu = query input
    let showhdr = showHeaders input
    let pars = Map.fromList (map (Var *** AbstractResultValue) (params input))
    if isJust (tcpAddr input) && isJust (tcpPort input)
      then
        runTCP (fromJust (tcpAddr input)) (fromJust (tcpPort input)) showhdr hdr qu pars
      else if isJust (udsAddr input)
        then
          runUDS (fromJust (udsAddr input)) showhdr hdr qu pars
        else
          run2 showhdr hdr qu pars ps
