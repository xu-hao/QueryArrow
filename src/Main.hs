{-# LANGUAGE MonadComprehensions, ForeignFunctionInterface #-}
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
import QueryPlan
import ResultStream
import FO.Data
import Parser
import DBQuery
import ICAT
-- import Plugins
import SQL.HDBC.PostgreSQL
-- import Cypher.Neo4j
import Prover.E
import Prover.Parser
import FO.Data
import Config
import Utils

import Prelude hiding (lookup)
import Data.Map.Strict (empty, lookup, fromList, singleton, keys, toList, Map)
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
import System.ZMQ4.Monadic
import System.IO
import System.Environment
import Data.Maybe
import Data.List hiding (lookup)
import Control.Concurrent
import Control.Concurrent.Async hiding (async)
import Data.Time
import Control.Exception
import Control.Monad.Except
import Serialization
import Data.Aeson
-- import Data.Serialize


main::IO()
main = do
    args2 <- getArgs
    if null args2
        then do
            print "no arguments"
        else do
            ps <- getConfig (args2 !! 1)
            if head args2 == "zmq"
                then
                    runzmq (args2 !! 2) ps
                else
                    run2 (head args2) ps


runzmq :: String -> TranslationInfo -> IO ()
runzmq addr ps = do
    tdb@(TransDB _ dbs   preds (qr, qr2, ir, dr) ) <- transDB "tdb" ps
    mapM_ print qr
    mapM_ print ir
    mapM_ print dr
    putStrLn ("listening at " ++ addr)
    let worker = do
        liftIO $ putStrLn "new worker"
        receiver <- socket Rep
        connect receiver "inproc://workers"
        forever $ do
            qus <- receive receiver
            t0 <- liftIO $ getCurrentTime
            ret <- liftIO $ try (do
                let msg = CS.unpack qus
                liftIO $ putStrLn ("received message " ++ msg)
                liftIO $ hFlush stdout
                let colonx = fromJust (findIndex (== ':') msg)
                    uz = take colonx msg
                    hashx = fromJust (findIndex (== '#') uz)
                    user = take hashx uz
                    zone = drop (hashx + 1) uz
                    qu = drop (colonx + 1) msg
                liftIO $ run3 qu tdb user zone)
            let (vars, rep) = case ret of
                        Left e -> (["error"], [singleton "error" (show (e :: SomeException))])
                        Right pp -> pp
            t1 <- liftIO $ getCurrentTime
            send receiver [] (B.toStrict (encode (resultSet rep vars)))
            liftIO $ putStrLn (show (diffUTCTime t1 t0))
            liftIO $ hFlush stdout
    runZMQ $ do
        server <- socket Router
        bind server addr

        workers <- socket Dealer
        bind workers "inproc://workers"

        replicateM_ 5 (async worker)
        proxy server workers Nothing


run2 :: String -> TranslationInfo -> IO ()
run2 query ps = do
    tdb@(TransDB _ dbs preds (qr, qr2, ir, dr) ) <- transDB "tdb" ps
    mapM_ print qr
    mapM_ print ir
    mapM_ print dr
    pp <- run3 query tdb "rods" "tempZone"
    putStr (pprint pp)

run3 :: String -> TransDB DBAdapterMonad MapResultRow -> String -> String -> IO ([String], [Map String String])
run3 query tdb@(TransDB _ dbs  preds (qr, qr2, ir, dr) ) user zone = do
    let predmap = foldMap (\p@(Pred n _) -> singleton n p) preds
    let params = fromList [(Var "client_user_name",StringValue user), (Var "client_zone", StringValue zone)]
    let pqp qp = do
        liftIO $ putStrLn ("query plan:")
        liftIO $ putStrLn (drawTree (toTree  qp))

    r <- runResourceT $ evalStateT (dbCatch $ do
                case runParser progp predmap "" query of
                            Left err -> error (show err)
                            Right (qu, _) ->
                                case runExcept (checkQuery qu) of
                                    Right _ -> do
                                        liftIO $ putStrLn ("original query: " ++ show qu)
                                        let qu'@(Query vars f') = rewriteQuery (keys params) qr qr2 ir dr qu
                                        liftIO $ putStrLn ("rewritten query: " ++ show qu')
                                        pqp  (queryPlan2 dbs (keys params) vars qu' )
                                        -- liftIO $ print (printFunc qu)
                                        -- liftIO $ print (keys params)
                                        rows <- getAllResultsInStream ( doQuery tdb qu (keys params) (pure params))
                                        return (vars, rows)
                                    Left e -> error e) (DBAdapterState Nothing)
    case r of
        Right (vars, rows) -> return (map unVar vars, map (\row -> fromList (map (\(Var v,r) -> (v, show r)) (toList row))) rows)
        Left e -> return (["error"], [singleton "error" (show e)])
