{-# LANGUAGE MonadComprehensions #-}
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
import Parser
import SQL.SQL
import SQL.HDBC.Sqlite3
import SQL.HDBC.PostgreSQL
import DBQuery
import ICAT

import Prelude hiding (lookup)
import Data.Map.Strict (empty, fromList,lookup)
import Text.Parsec (runParser)
import Data.Functor.Identity
import Control.Monad.Trans.State.Strict (evalStateT)
import System.Environment
import Data.List (intercalate, transpose)
import Data.Aeson
import Control.Applicative ((<$>))
import qualified Data.ByteString.Lazy as B
import System.Plugins.DynamicLoader

pgt :: Pred
pgt = Pred "gt" ["Num", "Num"]

pFile :: Pred
pFile = Pred "File" ["Path"]

pMeta :: Pred
pMeta = Pred "Meta" ["Path", "String"]
 
a1 :: Atom
a1 = Atom pgt [VarExpr "x", IntExpr 1]

a2 :: Atom
a2 = Atom pFile [VarExpr "y"]

a3 :: Atom
a3 = Atom pMeta [VarExpr "y", VarExpr "z"]

db :: MapDB
db = MapDB "Container" "elem" [(StringValue "ObjA", StringValue "ObjB"), 
    (StringValue "ObjB", StringValue "ObjC"), (StringValue "ObjB", StringValue "ObjD"), 
    (StringValue "ObjD", StringValue "ObjA"), (StringValue "ObjD", StringValue "ObjP"), 
    (StringValue "ObjQ", StringValue "ObjR"), (StringValue "ObjR", StringValue "ObjS")]

db2 :: EqDB
db2 = EqDB "Eq"

main::IO()
main = do
    args2 <- getArgs
    if null args2
        then do
            print pgt
            print a1
            print a2
            print a3
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
                Right (atoms, _) -> case  getAllResults [ Database db, Database db2] atoms of 
                    Identity ( results, _) -> print results
            print "test2"
            let query = "elem(x, y) elem(y, z) | elem(w, y) \
            \(exists u.elem(y, u)) ~(exists u.(exists v.elem(v,u) elem(u,y))) \
            \~elem(y, \"ObjC\") ~eq(x, \"ObjB\") eq(b,\"test\") eq(a,b) return x y z w b"
            
            case runParser progp (constructPredMap [Database db, Database db2]) "" query of
                Left err -> print err
                Right (qu, _) -> do 
                    let (Identity  (rs, report)) =  getAllResults [Database db, Database db2] qu
                    print rs
                    print report
        
            let query2 = "predicate filename(String, String) predicate coll(String, String) predicate size(String, Int) predicate le(Int, Int)\
            \ filename(a, \"John\") coll(a, \"X\") size(a, x) le(x,1000) return a"
            conn <- dbConnect (Sqlite3DBConnInfo "./db.sqlite3")
            let db3 = SQLDBAdapter {
                        sqlDBConn = conn,
                        sqlDBName = "test_sqlite3_db",
                        sqlDBPreds = [
                                Pred "filename" ["ObjectId", "String"],
                                Pred "coll" ["ObjectId", "CollId"],
                                Pred "size" ["ObjectId", "BigInt"],
                                Pred "le" ["BigInt", "BigInt"] ],
                        sqlTrans = SQLTrans
                                (fromList [("data", (["id", "filename", "collId", "size"], ["id"]))])
                                (BuiltIn (fromList [("le", \thesign args -> do
                                                return ([], SQLCompCond (case thesign of 
                                                        Pos -> "<"
                                                        Neg -> ">=") (head args) (args !! 1)))]))
                                (fromList [
                                        ("filename", (OneTable "data" "1", [( "1", "id"), ( "1", "filename")])),
                                        ("size", (OneTable "data" "1", [( "1", "id"), ( "1", "size")])),
                                        ("coll", (OneTable "data" "1", [( "1", "id"), ( "1", "collId")]))
                                        ])
                }
            case runParser progp (constructPredMap [Database db3]) "" query2 of
                Left err -> print err
                Right (qu2, _) -> do 
                    let sql = translate db3 qu2
                    print sql
                    (rows, _) <- evalStateT (getAllResults [Database db3] qu2) (DBAdapterState empty)
                    print rows
            let query3 = "DATA_NAME(a, b) COLL(a, c) COLL_NAME(c, \"/tempZone/home/rods/x\") DATA_SIZE(a, x) le(x,1000) return a b"
            run2 query3
        else
            run2 (head args2)
            
pprint :: [Var] -> [MapResultRow] -> String
pprint vars rows = join vars ++ "\n" ++ intercalate "\n" rowstrs ++ "\n" where
    join = intercalate " " . map (uncurry pad) . zip collen
    rowstrs = map join m2
    m2 = transpose m
    collen = map (maximum . map length) m
    m = map f vars
    f var = map (g var) rows
    g::Var->MapResultRow->String
    g var row = case lookup var row of
        Nothing -> "null"
        Just e -> show e
    pad n s
        | length s < n  = s ++ replicate (n - length s) ' '
        | otherwise     = s

    

getDBFromPlugin :: ICATDBConnInfo -> IO (Database DBAdapterMonad MapResultRow, Query -> String)
getDBFromPlugin icatDBConnInfo = do
    let pluginName = "M" ++ catalog_database_type icatDBConnInfo
    print ("load plugin " ++ pluginName)
    modul <- loadModule pluginName Nothing Nothing
    print ("resolve functions")
    resolveFunctions
    print ("load function getDB")
    getDBFunc <- loadFunction modul "getDB"
    print ("return")
    getDBFunc icatDBConnInfo


--                        conn <- dbConnect (Sqlite3DBConnInfo (db_name ps))
--                        let db3 = makeICATSQLDBAdapter conn
--                        return (Database db3, show . translate db3)
                        
run2 :: String -> IO ()
run2 query = do
    d <- eitherDecode <$> B.readFile "/etc/irods/database_config.json"
    case d of
        Left err -> putStrLn err
        Right ps -> do
            (db3, printFunc) <- getDBFromPlugin ps
            let predmap = constructPredMap [db3]
            case runParser progp predmap "" query of
                Left err -> print err
                Right (qu@(Query vars _), _) -> do
                    print (printFunc qu)
                    (rows, report) <- evalStateT (getAllResults [db3] qu) (DBAdapterState empty)
                    print report
                    putStr (pprint vars rows)
