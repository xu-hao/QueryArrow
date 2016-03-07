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
import FO.Data
import Parser
import DBQuery
import ICAT
-- import Plugins
-- import SQL.HDBC.PostgreSQL
import Cypher.Neo4j
import FO.E
import FO.Data
import FO.Config

import Prelude hiding (lookup)
import Data.Map.Strict (empty, lookup)
import Text.Parsec (runParser)
import Control.Monad.IO.Class (liftIO)
import Control.Monad.Trans.State.Strict
import Control.Monad.Trans.Class (lift)
import System.Environment
import Data.List (intercalate, transpose)
import Data.Aeson
import Control.Applicative ((<$>))
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
                        liftIO $ print results) (DBAdapterState empty)
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
                        liftIO $ print report) (DBAdapterState empty)

            --let query3 = "DATA_NAME(a, b) COLL(a, c) COLL_NAME(c, \"/tempZone/home/rods/x\") DATA_SIZE(a, x) le(x,1000) return a b"
            --run2 query3
        else do
            d <- eitherDecode <$> B.readFile (if length args2 <= 2 then "/etc/irods/database_config.json" else args2 !! 2)
            case d of
                Left err -> putStrLn err
                Right ps -> do 
                    d <- eitherDecode <$> B.readFile (if length args2 <= 1 then "test/verifier.json" else args2 !! 1)
                    case d of
                        Left err -> putStrLn err
                        Right ps1 -> do 
                            run2 (head args2) ps ps1

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


insert  db3 a verifier rules  qu@(Insert lits cond) = do
    liftIO $ print (a qu)
    case db3 of
        Database db -> lift $ lift $ do
            let formulas = map (\(_,_,a) -> a) rules
            liftIO $ print "calling verifier"
            case validate qu of
                Just ce -> error ("Deleting a literal not implied by condition " ++ show ce)
                Nothing -> do
                    vres <- liftIO $ validateInsert verifier formulas qu
                    case vres of
                        Just True -> do
                            let vres2 = validate qu
                            case vres2 of
                                Nothing -> do
                                    res <- doInsert db qu :: DBAdapterMonad [Int]
                                    let row = map show res
                                    liftIO $ print row
                                    return ()
                                _ -> error "cannot verify insert statement is correct 2"
                        _ -> error "cannot verify insert statement is correct"

run2 :: String -> ICATDBConnInfo -> VerificationInfo -> IO ()
run2 query ps ps1 = do
            (db3, printFunc, a) <- getDB ps
            let predmap = constructPredMap [db3]
            verifier <- getVerifier ps1
            d0 <- B8.unpack <$> B.readFile (rule_file_path ps1)
            let rules = parseTPTP predmap d0
            evalStateT (dbWithSession [db3] $ do
                        case runParser progp predmap "" query of
                            Left err -> liftIO $ print err
                            Right (Q qu@(Query vars f), _) -> do
                                liftIO $ print (show (execPlan [db3] f))
                                liftIO $ print (printFunc qu)
                                rows <- getAllResults qu
                                report <- getReportFromDBSession
                                liftIO $ print report
                                liftIO $ putStr (pprint vars rows)
                            Right (D atoms cond, _) -> do
                                let qu = transformDeletion [] atoms cond
                                res <- insert  db3 a verifier rules qu
                                liftIO $ print res
                            Right (I qu, _) -> do
                                res <- insert  db3 a verifier rules qu
                                liftIO $ print res) (DBAdapterState empty)

