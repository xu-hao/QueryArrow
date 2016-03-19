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
import FO.E
import FO.Data
import Config
import Utils

import Prelude hiding (lookup)
import Data.Map.Strict (empty, lookup, fromList, singleton, keys)
import Text.Parsec (runParser)
import Control.Monad.IO.Class (liftIO)
import Control.Monad.Trans.State.Strict
import Control.Monad.Trans.Class (lift)
import System.Environment
import Data.Tree
import Data.List (intercalate, transpose)
import Data.Aeson
import Control.Applicative ((<$>))
import qualified Control.Applicative as Appl
import qualified Data.ByteString.Lazy as B
import qualified Data.ByteString.Lazy.Char8 as B8
import FO.Parser

db :: MapDB DBAdapterMonad
db = MapDB "Container" "elem" [(StringValue "ObjA", StringValue "ObjB"),
    (StringValue "ObjB", StringValue "ObjC"), (StringValue "ObjB", StringValue "ObjD"),
    (StringValue "ObjD", StringValue "ObjA"), (StringValue "ObjD", StringValue "ObjP"),
    (StringValue "ObjQ", StringValue "ObjR"), (StringValue "ObjR", StringValue "ObjS")]

db2 :: EqDB DBAdapterMonad
db2 = EqDB "Eq"

main::IO()
main = do
    args2 <- getArgs
    if null args2
        then do
            let prog = "predicate gt(Num, Num)\n\
            \predicate File(Path)\n\
            \predicate Meta(Path, String)\n\
            \gt(x, y)\n\
            \File(z)\n\
            \Meta(z,m)\
            \return x y z m"
            case runParser progp empty "" prog of
                Left err -> print err
                Right res -> print res
            let query0 = "predicate elem(String, String) predicate eq(String, String)\n\
            \elem(x, y) elem(y, z) ~elem(y, \"ObjC\") eq(b,\"test\") eq(a,b) return x y z w b"
            case runParser progp empty "" query0 of
                Left err -> print err
                Right (Q atoms, _) ->
                    evalStateT (dbWithSession [ Database db, Database db2] $ do
                        results <- getAllResults  atoms
                        liftIO $ print results) (DBAdapterState empty Nothing)
            print "test2"
            let query = "elem(x, y) (elem(y, z) | elem(w, y)) \
            \(exists u.elem(y, u)) ~(exists u. exists v.elem(v,u) elem(u,y)) \
            \~elem(y, \"ObjC\") ~eq(x, \"ObjB\") eq(b,\"test\") eq(a,b) return x y z w b"

            case runParser progp (constructPredMap [Database db, Database db2]) "" query of
                Left err -> print err
                Right (Q qu, _) -> do
                    evalStateT (dbWithSession [Database db, Database db2] $ do
                        rs <- getAllResults qu
                        liftIO $ print rs
                        report <- getReportFromDBSession
                        liftIO $ print report) (DBAdapterState empty Nothing)

            --let query3 = "DATA_NAME(a, b) COLL(a, c) COLL_NAME(c, \"/tempZone/home/rods/x\") DATA_SIZE(a, x) le(x,1000) return a b"
            --run2 query3
        else do
            ps <- getConfig (args2 !! 1)
            run2 (head args2) ps

maximumd :: Int -> [Int] -> Int
maximumd d [] = d
maximumd _ l = maximum l

pprint :: [Var] -> [MapResultRow] -> String
pprint vars rows = join  (map unVar vars) ++ "\n" ++ intercalate "\n" rowstrs ++ "\n" where
    join = intercalate " " . map (uncurry pad) . zip collen
    rowstrs = map join m2
    m2 = transpose m
    collen = map (maximumd 0 . map length) m
    m = map f vars
    f var = map (g var) rows
    g::Var->MapResultRow->String
    g var row = case lookup var row of
        Nothing -> "null"
        Just e -> show e
    pad n s
        | length s < n  = s ++ replicate (n - length s) ' '
        | otherwise     = s



defaultInsertMap :: Database m row -> InsertMap
defaultInsertMap (Database db) =
    let preds = getPreds db in
        fromList (zip preds (replicate (length preds) ([0],[0])))

run2 :: String -> TranslationInfo -> IO ()
run2 query ps = do
    tdb@(TransDB _ dbs  _ rules preds (qr, ir, dr) insmap) <- transDB "tdb" ps
    mapM_ print qr
    mapM_ print ir
    mapM_ print dr
    let predmap = foldMap (\p@(Pred n _) -> singleton n p) preds
    let params = fromList [(Var "client_user_name",StringValue "rods"), (Var "client_zone", StringValue "tempZone")]
    (vars, rows) <- evalStateT (dbWithSession [Database tdb] $ do
                case runParser progp predmap "" query of
                            Left err -> error (show err)
                            Right (Q qu, _) -> do
                                liftIO $ putStrLn ("original query: " ++ show qu)
                                let qu'@(Query vars f') = rewriteQuery qr qu
                                liftIO $ putStrLn ("rewritten query: " ++ show qu')
                                liftIO $ putStrLn ("query plan: " ++ drawTree (toTree (queryPlan dbs f')))
                                -- liftIO $ print (printFunc qu)
                                -- liftIO $ print (keys params)
                                rows <- lift $ lift $ getAllResultsInStream ( doQuery tdb qu (keys params) (pure params))
                                return (vars, rows)

                            Right (D atoms cond, _) -> do
                                let qu = transformDeletion [] atoms cond
                                liftIO $ putStrLn ("original query: " ++ show qu)
                                let qu' = rewriteInsert qr ir dr qu
                                liftIO $ putStrLn ("rewritten query: " ++ show qu')
                                liftIO $ putStrLn ("query plan: " ++ drawTree (toTree (insertPlan dbs insmap qu')))
                                rows <- lift $ lift $ getAllResultsInStream (doInsert tdb qu (keys params) (pure params))
                                return ([], rows)

                            Right (I qu, _) -> do
                                liftIO $ putStrLn ("original query: " ++ show qu)
                                let qu' = rewriteInsert qr ir dr qu
                                liftIO $ putStrLn ("rewritten query: " ++ show qu')
                                liftIO $ putStrLn ("query plan: " ++ drawTree (toTree (insertPlan dbs insmap qu')))
                                rows <- lift $ lift $ getAllResultsInStream (doInsert tdb qu (keys params) (pure params))
                                return ([], rows)) (DBAdapterState empty Nothing)

    putStr (pprint vars rows)
