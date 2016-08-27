{-# LANGUAGE MonadComprehensions, ForeignFunctionInterface ,OverloadedStrings #-}
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
import FO
import QueryPlan hiding (Null)
import DB.ResultStream
import FO.Data
import Parser
import ICAT
-- import Plugins
-- import SQL.HDBC.PostgreSQL
-- import Cypher.Neo4j
import FO.Data
import Config
import Utils

import Prelude hiding (lookup)
import Data.Map.Strict (empty, lookup, fromList, singleton, keys, toList, Map, delete, insert, member)
import Text.Parsec (runParser)
import Control.Monad
import Control.Monad.IO.Class (liftIO)
import Control.Monad.Trans.State.Strict
import Control.Monad.Trans.Resource
import Control.Monad.Trans.Class (lift)
import System.Environment
import Data.Tree
import Data.List (intercalate, transpose)
import Control.Applicative ((<$>))
import qualified Control.Applicative as Appl
import qualified Data.ByteString.Char8 as CS
import qualified Data.ByteString.Lazy as B
import System.IO
import System.Environment
import Data.Maybe
import Control.Concurrent
import Control.Concurrent.Async hiding (async)
import Data.Time
import Control.Exception
import Control.Monad.Except
import Serialization
import Data.Aeson
import Data.Aeson.Types
import qualified Data.Text as T
import System.Log.Logger
import Logging
-- import Data.Serialize
import Data.Namespace.Path
import Data.Namespace.Namespace
import qualified Data.Set as Set
import Data.Conduit.Network
import Network.JsonRpc
import Control.Monad.Trans.Reader
import Data.String
import Control.Monad.Logger
import System.Log.FastLogger
import Control.Monad.Logger.HSLogger

main::IO()
main = do
    setup
    args2 <- getArgs
    if null args2
        then do
            print "no arguments"
        else do
            ps <- getConfig (args2 !! 0)
            if (args2 !! 1) == "tcp"
                then
                    runtcpmulti (args2 !! 2) (read (args2 !! 3)) ps
                else
                    run2 (words (args2 !! 2)) (args2 !! 1) ps


runtcpmulti :: String -> Int -> TranslationInfo -> IO ()
runtcpmulti addr port ps = do
    infoM "QA" ("listening at " ++ addr)
    jsonRpcTcpServer
        V2
        True
        (serverSettings port (fromString addr))
        (do
            liftIO $ infoM "QA" ("client connected")
            tdb@(TransDB _ dbs   preds (qr, ir, dr) ) <- liftIO $ transDB "tdb" ps
            liftIO $ mapM_ (debugM "QA" . show) qr
            liftIO $ mapM_ (debugM "QA" . show) ir
            liftIO $ mapM_ (debugM "QA" . show) dr
            liftIO $ dbOpen tdb
            let worker = do
                    t0 <- liftIO $ getCurrentTime
                    req <- receiveRequest
                    case req of
                        Nothing -> return ()
                        Just req -> do
                            liftIO $ infoM "QA" ("received message " ++ show req)
                            let qus = getReqParams req
                            let qs = parse parseJSON qus
                            case qs of
                                Error errmsg ->
                                    sendResponse (ResponseError
                                                      V2
                                                      (ErrorObj
                                                          errmsg
                                                          (-1)
                                                          Null)
                                                      (getReqId req))
                                Success qs -> do
                                    let user = qsuser qs
                                        zone = qszone qs
                                        qu = qsquery qs
                                        hdr = qsheaders qs
                                    ret <- liftIO $ try (liftIO $ run3 hdr qu tdb user zone)
                                    case ret of
                                        Left e ->
                                            sendResponse (ResponseError
                                                              V2
                                                              (ErrorObj
                                                                  (show (e :: SomeException))
                                                                  (-1)
                                                                  Null)
                                                              (getReqId req))
                                        Right (hdr, rep) ->
                                            sendResponse (Response
                                                              V2
                                                              (toJSON (resultSet hdr rep))
                                                              (getReqId req))
                            t1 <- liftIO $ getCurrentTime
                            liftIO $ infoM "QA" (show (diffUTCTime t1 t0))
                            worker
            worker
            liftIO $ dbClose tdb
            liftIO $ infoM "QA" ("client connected")
            )


run2 :: [String] -> String -> TranslationInfo -> IO ()
run2 hdr query ps = do
    tdb@(TransDB _ dbs preds (qr, ir, dr) ) <- transDB "tdb" ps
    mapM_ print qr
    mapM_ print ir
    mapM_ print dr
    dbOpen tdb
    (hdr, pp) <- run3 hdr query tdb "rods" "tempZone"
    putStr (pprint hdr pp)
    dbClose tdb


run3 :: [String] -> String -> TransDB DBAdapterMonad MapResultRow -> String -> String -> IO ([String], [Map String String])
run3 hdr query tdb user zone = do
    let predmap = constructDBPredMap (Database tdb)
    let params = fromList [(Var "client_user_name",StringValue (T.pack user)), (Var "client_zone", StringValue (T.pack zone))]


    r <- runResourceT $ evalStateT (dbCatch $ do
                case runParser progp (mempty, predmap, mempty) "" query of
                            Left err -> error (show err)
                            Right (Left (qu@(Query form), _)) ->
                                case runExcept (checkQuery qu) of
                                    Right _ -> do
                                        rows <- getAllResultsInStream ( doQuery tdb (Set.fromList (map Var hdr)) qu (keys params) (pure params))
                                        return rows
                                    Left e -> error e
                            Right (Right Commit) -> do
                                b <- dbPrepare tdb
                                if b
                                    then do
                                        b <- dbCommit tdb
                                        if b
                                            then return [mempty]
                                            else do
                                                dbRollback tdb
                                                liftIO $ errorM "QA" "prepare succeeded but cannot commit"
                                                error "prepare succeeded but cannot commit"
                                    else do
                                        dbRollback tdb
                                        liftIO $ errorM "QA" "prepare failed, cannot commit"
                                        error "prepare failed, cannot commit") (DBAdapterState Nothing)
    return (case r of
        Right rows ->  (hdr, map (\row -> fromList (map (\(Var v,r) -> (v, show r)) (toList row))) rows)
        Left e ->  (["error"], [singleton "error" (show e)]))
