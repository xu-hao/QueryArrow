{-# LANGUAGE MonadComprehensions, FlexibleContexts #-}
module ICAT where

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
import SQL.HDBC
import SQL.HDBC.Sqlite3
import SQL.HDBC.PostgreSQL
import DBQuery

import Data.Map.Strict (empty, fromList)
import Text.Parsec (runParser)
import Data.Functor.Identity
import Control.Monad.Trans.State.Strict (evalStateT)
import Database.HDBC

makeICATSQLDBAdapter :: DBConnection conn stmt SQLQuery SQLExpr => conn -> SQLDBAdapter connInfo conn stmt 
makeICATSQLDBAdapter conn = SQLDBAdapter {
    sqlDBConn = conn,
    sqlDBName = "ICAT",
    sqlDBPreds = [
        Pred "DATA_NAME" ["ObjectId", "String"],
        Pred "COLL_ID" ["ObjectId", "CollId"],
        Pred "DATA_SIZE" ["ObjectId", "BigInt"],
        Pred "COLL_NAME" ["ObjectId", "String"],
        Pred "DATA_COLL_NAME" ["ObjectId", "String"],
        Pred "le" ["BigInt", "BigInt"] ],
    sqlTrans = SQLTrans
        (fromList [("r_data_main", (["data_id", "data_name", "coll_id", "data_size"], ["data_id"])),
                   ("r_coll_main", (["coll_id", "coll_name", "parent_coll_name"], ["coll_id"]))])
        BuiltIn {
            builtInList = ["le"],
            builtInMap = \thesign name args -> do
                return ([], SQLCompCond (case thesign of 
                    Pos -> "<"
                    Neg -> ">=") (head args) (args !! 1))
        }
        (fromList [
            ("DATA_NAME", (OneTable "r_data_main" "1", [( "1", "data_id"), ( "1", "data_name")])),
            ("DATA_SIZE", (OneTable "r_data_main" "1", [( "1", "data_id"), ( "1", "data_size")])),
            ("COLL_NAME", (OneTable "r_coll_main" "1", [( "1", "coll_id"), ( "1", "coll_name")])),
            ("DATA_COLL_NAME", (JoinTables "r_data_main" "1" [
                ("r_coll_main", "2", SQLCompCond "=" (SQLColExpr ("1", "coll_id")) (SQLColExpr ("2", "coll_id")))
            ], [( "1", "data_id"), ( "2", "coll_name")])),
            ("COLL_ID", (OneTable "r_data_main" "1", [( "1", "data_id"), ( "1", "coll_id")]))
            ])
}
