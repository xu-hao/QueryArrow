{-# LANGUAGE FlexibleContexts, FlexibleInstances, StandaloneDeriving, OverloadedStrings #-}

module Main where
import QueryArrow.QueryPlan
import QueryArrow.DB.DB
import QueryArrow.DB.ResultStream
import QueryArrow.FO.Data
import QueryArrow.Parser
import QueryArrow.SQL.SQL
import qualified QueryArrow.ICAT as ICAT
import qualified QueryArrow.BuiltIn as BuiltIn
import qualified QueryArrow.SQL.ICAT as SQL.ICAT
import QueryArrow.DB.GenericDatabase
import QueryArrow.Utils

import Test.QuickCheck
import Control.Applicative ((<$>), (<*>))
import Control.Monad (replicateM)
import Text.Parsec (runParser)
import Data.Functor.Identity (runIdentity, Identity)
import Data.Map.Strict ((!), Map, empty, insert, fromList, singleton, toList)
import Control.Monad.Trans.State.Strict (StateT, evalState, runState, evalStateT, runStateT)
import Debug.Trace (trace)
import Test.Hspec
import Data.Monoid
import Data.Convertible
import Control.Monad.Trans.Except
import qualified Data.Text as T
import Control.Monad.IO.Class
import System.IO.Unsafe
import Data.Namespace.Namespace
import Data.Namespace.Path
import Data.Maybe
import Algebra.Lattice
import qualified Data.Set as Set


standardPreds = (++) <$> (ICAT.loadPreds "../QueryArrow-gen/gen/ICATGen.yaml") <*> pure BuiltIn.standardBuiltInPreds
standardMappings =  (SQL.ICAT.loadMappings "../QueryArrow-gen/gen/SQL/ICATGen.yaml")

sqlStandardTrans ns = SQL.ICAT.sqlStandardTrans ns <$> standardPreds <*> standardMappings <*> pure (Just "nextid")

standardPredMap = ICAT.standardPredMap <$> standardPreds

parseStandardQuery ::  String -> IO Formula
parseStandardQuery query2 = do
    mappings <- standardPredMap
    return (case runParser progp () "" query2 of
            Left err -> let errmsg = "cannot parse " ++ query2 ++ show err in trace errmsg error ""
            Right [Execute qu2] -> qu2)
qParseStandardQuery ::  String -> String -> IO Formula
qParseStandardQuery ns query2 = do
    preds <- ICAT.qStandardPredsMap ns <$> standardPreds
    return (case runParser progp () "" query2 of
            Left err -> let errmsg = "cannot parse " ++ query2 ++ show err in trace errmsg error ""
            Right [Execute qu2] -> qu2)
translateQuery2 :: SQLTrans -> Set.Set Var -> Formula -> SQLQuery
translateQuery2 trans vars qu = translateQuery1 trans vars qu Set.empty

translateQuery1 :: SQLTrans -> Set.Set Var -> Formula -> Set.Set Var -> SQLQuery
translateQuery1 trans vars qu env =
  let (SQLTrans  builtin predtablemap _ ptm) = trans
      env2 = foldl (\map2 key@(Var w)  -> insert key (SQLParamExpr w) map2) empty env in
      fst (runNew (runStateT (translateQueryToSQL (Set.toAscList vars) qu) (TransState {builtin = builtin, predtablemap = predtablemap, repmap = env2, tablemap = empty, nextid=Just (SQL.ICAT.nextidPred ["cat"] "nextid"), ptm = ptm})))

testTranslateSQLInsert :: String -> String -> String -> IO ()
testTranslateSQLInsert ns qus sqls = do
  qu <- qParseStandardQuery "cat" qus
  sql <- translateQuery2 <$> (sqlStandardTrans "cat") <*> pure Set.empty <*> pure qu
  serialize ((\(_,x,_) -> x)sql) `shouldBe` sqls

main :: IO ()
main = hspec $ do
{-    describe "tests" $ do
        it "test0" $ do
            property $ test0
        it "test1" $ do
            property $ test1
        it "test2" $ do
            property $ test2
        it "test3" $ do
            property $ test3
        it "test4" $ do
            property $ test4
        it "test5" $ do
            property $ test5 -}
        it "test that freshSQLVar generates fresh SQL vars" $ do
            (v1, v2) <- return (runNew (evalStateT (do
                v1 <- freshSQLVar "t"
                v2 <- freshSQLVar "t"
                return (v1, v2)) (TransState  (BuiltIn empty) empty empty empty (Just (SQL.ICAT.nextidPred ["cat"] "nextid")) mempty)))
            v1 `shouldBe` SQLVar "t"
            v2 `shouldBe` SQLVar "t0"
        it "test parse query 0" $ do
            formula <- parseStandardQuery "DATA_NAME(x, y, z) return x y z"
            standardPredMap <- standardPredMap
            formula `shouldBe`
              Aggregate (FReturn [Var "x", Var "y", Var "z"])
                (FAtomic (Atom (ObjectPath mempty "DATA_NAME") [VarExpr (Var "x"), VarExpr (Var "y"), VarExpr (Var "z")]))

        it "test parse query 1" $ do
            formula <- parseStandardQuery "let a = count (DATA_NAME(x, y, z)) return a"
            standardPredMap <- standardPredMap
            formula `shouldBe`
              Aggregate (FReturn [Var "a"])
                (Aggregate (Summarize [(Var "a", Count)] []) (FAtomic (Atom (ObjectPath mempty "DATA_NAME") [VarExpr (Var "x"), VarExpr (Var "y"), VarExpr (Var "z")])))

        it "test parse query 1.2" $ do
            formula <- parseStandardQuery "let a = count distinct b (DATA_NAME(x, y, z)) return a"
            standardPredMap <- standardPredMap
            formula `shouldBe`
              Aggregate (FReturn [Var "a"])
                (Aggregate (Summarize [(Var "a", CountDistinct (Var "b"))] []) (FAtomic (Atom (ObjectPath mempty "DATA_NAME") [VarExpr (Var "x"), VarExpr (Var "y"), VarExpr (Var "z")])))

        it "test parse query 1.1" $ do
            formula <- parseStandardQuery "let a = count, b = max x, c = min x (DATA_NAME(x, y, z)) return a"
            standardPredMap <- standardPredMap
            formula `shouldBe`
              Aggregate (FReturn [Var "a"])
                (Aggregate (Summarize [(Var "a", Count), (Var "b", Max (Var "x")), (Var "c", Min (Var "x"))] []) (FAtomic (Atom (ObjectPath mempty "DATA_NAME") [VarExpr (Var "x"), VarExpr (Var "y"), VarExpr (Var "z")])))

        it "test translate sql query 0.1" $ do
            qu <- qParseStandardQuery "cat" "cat.DATA_NAME(x, y, z)"
            sql <- translateQuery2 <$> (sqlStandardTrans "cat") <*> pure (Set.fromList [Var "x", Var "y", Var "z"]) <*> pure qu
            sql `shouldBe` ([Var "x", Var "y", Var "z"], SQLQueryStmt (SQLQuery
                    [
                        (Var "x", SQLColExpr (SQLVar "r_data_main", "data_id")),
                        (Var "y", SQLColExpr (SQLVar "r_data_main", "resc_id")),
                        (Var "z", SQLColExpr (SQLVar "r_data_main", "data_name"))]
                    [OneTable "r_data_main" (SQLVar "r_data_main")]
                    SQLTrueCond
                    []
                    top
                    False
                    []), [])
        it "test translate sql query 0.2" $ do
            qu <- qParseStandardQuery "cat" "let a = count (cat.DATA_NAME(x, y, z))"
            sql <- translateQuery2 <$> (sqlStandardTrans "cat") <*> pure (Set.fromList [Var "a"]) <*> pure qu
            sql `shouldBe` ([Var "a"], SQLQueryStmt (SQLQuery
                    [
                        (Var "a", SQLFuncExpr "count" [(SQLExprText "*")])]
                    [OneTable "r_data_main" (SQLVar "r_data_main")]
                    SQLTrueCond
                    []
                    top
                    False
                    []), [])
        it "test translate sql query 0.2" $ do
            qu <- qParseStandardQuery "cat" "let a = count, b = max x, c = min x (cat.DATA_NAME(x, y, z))"
            sql <- translateQuery2 <$> (sqlStandardTrans "cat") <*> pure (Set.fromList [Var "a", Var "b", Var "c"]) <*> pure qu
            sql `shouldBe` ([Var "a", Var "b", Var "c"], SQLQueryStmt (SQLQuery
                    [
                        (Var "a", SQLFuncExpr "count" [SQLExprText "*"]),
                        (Var "b", SQLFuncExpr "max" [SQLColExpr (SQLVar "r_data_main", "data_id")]),
                        (Var "c", SQLFuncExpr "min" [SQLColExpr (SQLVar "r_data_main", "data_id")])]
                    [OneTable "r_data_main" (SQLVar "r_data_main")]
                    SQLTrueCond
                    []
                    top
                    False
                    []), [])
        {- it "test translate sql query 0" $ do
            let qu = parseStandardQuery "DATA_NAME(x, y, z) return x y"
            let sql = translateQuery2 (sqlStandardTrans "") qu
            sql `shouldBe` ([Var "x", Var "y"], SQLQ (SQL0 [SQLColExpr (SQLVar "r_data_main", "data_id"), SQLColExpr (SQLVar "r_data_main", "data_name")] [OneTable "r_data_main" (SQLVar "r_data_main")] SQLTrueCond), []) -}
        it "test translate sql query with param" $ do
            qu <- qParseStandardQuery "cat" "cat.DATA_NAME(x, y, z)"
            sql <- translateQuery1 <$> (sqlStandardTrans "cat") <*> pure (Set.fromList [Var "x", Var "y", Var "z"]) <*> pure qu <*> pure (Set.fromList [Var "w"])
            sql `shouldBe` ([Var "x", Var "y", Var "z"], SQLQueryStmt (SQLQuery
                    [
                        (Var "x", SQLColExpr (SQLVar "r_data_main", "data_id")),
                        (Var "y", SQLColExpr (SQLVar "r_data_main", "resc_id")),
                        (Var "z", SQLColExpr (SQLVar "r_data_main", "data_name"))]
                    [OneTable "r_data_main" (SQLVar "r_data_main")]
                    SQLTrueCond
                    []
                    top
                    False
                    []), [])
            (params sql) `shouldBe` []
        it "test translate sql query with param 2" $ do
            qu <- qParseStandardQuery "cat" "cat.DATA_NAME(x, y, z)"
            sql <- translateQuery1 <$> (sqlStandardTrans "cat") <*> pure (Set.fromList [Var "x", Var "y"]) <*> pure qu <*> pure (Set.fromList [Var "z"])
            ((\(x,_,_) -> x) sql) `shouldBe` [Var "x", Var "y"]
            serialize ((\(_,x,_) -> x) sql) `shouldBe` "SELECT r_data_main.data_id AS \"x\",r_data_main.resc_id AS \"y\" FROM r_data_main r_data_main WHERE r_data_main.data_name = ?"
            (params sql) `shouldBe` [Var "z"]
        it "test translate sql insert 0" $ do
            testTranslateSQLInsert "cat" "cat.DATA_NAME(x, y, \"foo\") insert cat.DATA_SIZE(x, y, 1000)" "UPDATE r_data_main SET data_size = 1000 WHERE data_name = 'foo'"
        it "test translate sql insert 1" $ do
            testTranslateSQLInsert "cat" "insert cat.DATA_OBJ(1, \"bar\") cat.DATA_NAME(1, \"bar\", \"foo\") cat.DATA_SIZE(1, \"bar\", 1000)" "INSERT INTO r_data_main (data_id,resc_id,data_name,data_size) VALUES (1,'bar','foo',1000)"
        it "test translate sql insert 2" $ do
            testTranslateSQLInsert "cat" "cat.COLL_NAME(a,c) insert cat.DATA_OBJ(1, \"foo\") cat.DATA_NAME(1, \"foo\", c) cat.DATA_SIZE(1, \"foo\", 1000)" "INSERT INTO r_data_main (data_id,resc_id,data_name,data_size) SELECT 1,\'foo\',r_coll_main.coll_name,1000 FROM r_coll_main r_coll_main"
        it "test translate sql insert 3" $ do
            testTranslateSQLInsert "cat" "cat.COLL_NAME(2,c) insert cat.DATA_OBJ(1, \"foo\") cat.DATA_NAME(1, \"foo\", c) cat.DATA_SIZE(1, \"foo\", 1000)" "INSERT INTO r_data_main (data_id,resc_id,data_name,data_size) SELECT 1,\'foo\',r_coll_main.coll_name,1000 FROM r_coll_main r_coll_main WHERE r_coll_main.coll_id = 2"
        it "test translate sql insert 4" $ do
            testTranslateSQLInsert "cat" "insert ~cat.DATA_NAME(1, \"foo\", c)" "UPDATE r_data_main SET data_name = NULL WHERE (data_id = 1 AND resc_id = 'foo')"
        it "test translate sql insert 4.01" $ do
            testTranslateSQLInsert "cat" "cat.DATA_NAME(1, y, c) insert ~cat.DATA_NAME(1, y, c)" "UPDATE r_data_main SET data_name = NULL WHERE data_id = 1"
        it "test translate sql insert 4.1" $ do
            testTranslateSQLInsert "cat" "cat.DATA_NAME(1, \"foo\", c) insert ~cat.DATA_NAME(1, \"foo\", c)" "UPDATE r_data_main SET data_name = NULL WHERE (data_id = 1 AND resc_id = 'foo')"
        it "test translate sql insert 4.2" $ do
            testTranslateSQLInsert "cat" "cat.DATA_PATH(1, \"foo\", c) insert ~cat.DATA_NAME(1, \"foo\", c)" "UPDATE r_data_main SET data_name = NULL WHERE ((data_id = 1 AND resc_id = 'foo') AND data_name = data_path)"
        it "test translate sql insert 5" $ do
            testTranslateSQLInsert "cat" "insert ~cat.DATA_NAME(1, \"bar\", c) cat.DATA_NAME(1, \"bar\", \"foo\")" "UPDATE r_data_main SET data_name = 'foo' WHERE (data_id = 1 AND resc_id = 'bar')"
        it "test translate sql insert 5.1" $ do
            testTranslateSQLInsert "cat" "cat.DATA_NAME(1, \"bar\", c) insert ~cat.DATA_NAME(1, \"bar\", c) cat.DATA_NAME(1, \"bar\", \"foo\")" "UPDATE r_data_main SET data_name = 'foo' WHERE (data_id = 1 AND resc_id = 'bar')"
        it "test translate sql insert 5.2" $ do
            testTranslateSQLInsert "cat" "cat.DATA_PATH(1, \"bar\", c) insert ~cat.DATA_NAME(1, \"bar\", c) cat.DATA_NAME(1, \"bar\", \"foo\")" "UPDATE r_data_main SET data_name = 'foo' WHERE (data_id = 1 AND resc_id = 'bar')"
        it "test translate sql insert 6" $ do
            testTranslateSQLInsert "cat" "cat.DATA_OBJ(x, \"bar\") insert ~cat.DATA_NAME(x, \"bar\", c) cat.DATA_NAME(x, \"bar\", \"foo\")" "UPDATE r_data_main SET data_name = 'foo' WHERE resc_id = 'bar'"
        it "test translate sql insert 7" $ do
            testTranslateSQLInsert "cat" "cat.DATA_NAME(x, \"bar\", \"foo1\") insert ~cat.DATA_NAME(x, \"bar\", \"foo1\") cat.DATA_NAME(x, \"bar\", \"foo\")" "UPDATE r_data_main SET data_name = 'foo' WHERE (resc_id = 'bar' AND data_name = 'foo1')"
        it "test translate sql insert 7.1" $ do
            testTranslateSQLInsert "cat" "cat.DATA_NAME(x, \"bar\", \"foo1\") insert ~cat.DATA_NAME(x, \"bar\", \"foo1\")" "UPDATE r_data_main SET data_name = NULL WHERE (resc_id = 'bar' AND data_name = 'foo1')"
        it "test translate sql insert 21 no insert and delete" $ do
            form <- parseStandardQuery "insert ~cat.DATA_NAME(x, \"bar\", \"foo1\") cat.DATA_SIZE(x, \"bar\", 1000)"
            sql <- supported <$> (GenericDatabase <$> (sqlStandardTrans "cat") <*> pure () <*> pure "cat" <*> standardPreds) <*> pure mempty <*> pure form <*> pure Set.empty
            sql `shouldBe` False
            -- length sql `shouldBe` 2
            -- show (sql !! 0) `shouldBe` "UPDATE r_data_main SET data_size = 1000"
            -- show (sql !! 1) `shouldBe` "UPDATE r_data_main SET data_name = NULL WHERE data_name = 'foo1'"
        it "test translate sql insert 22 no delete conditional and other delete" $ do
            form <- parseStandardQuery "insert ~cat.DATA_NAME(x, \"bar\", \"foo1\") ~cat.DATA_SIZE(x, \"bar\", 1000)"
            sql <- supported <$> (GenericDatabase <$> (sqlStandardTrans "cat") <*> pure () <*> pure "cat" <*> standardPreds) <*> pure mempty <*> pure form <*> pure Set.empty
            sql `shouldBe` False
        it "test translate sql insert 23 delete and other delete" $ do
            form <- parseStandardQuery "insert ~cat.DATA_NAME(x, \"bar\", w) ~cat.DATA_SIZE(x, \"bar\", z)"
            sql <- supported <$> (GenericDatabase <$> (sqlStandardTrans "cat") <*> pure () <*> pure "cat" <*> standardPreds) <*> pure mempty <*> pure form <*> pure Set.empty
            sql `shouldBe` True
        it "test translate sql insert 8" $ do
            testTranslateSQLInsert "cat" "cat.DATA_NAME(x, \"bar1\", \"bar\") insert cat.DATA_NAME(x, \"bar1\", \"foo\")" "UPDATE r_data_main SET data_name = 'foo' WHERE (resc_id = 'bar1' AND data_name = 'bar')"
        it "test translate sql insert 9" $ do
            testTranslateSQLInsert "cat" "insert ~cat.DATA_OBJ(1, \"foo\")" "DELETE FROM r_data_main WHERE (data_id = 1 AND resc_id = 'foo')"
        it "test tranlate sql insert 10" $ (do
                qu <- qParseStandardQuery "cat" "insert ~cat.DATA_OBJ(x, \"foo\")"
                sql <- translateQuery2 <$> (sqlStandardTrans "cat") <*> pure Set.empty <*> pure qu
                print (show ((\(_,x,_) -> x)sql))
            ) `shouldThrow` anyException
        it "test translate sql insert 11" $ do
            testTranslateSQLInsert "cat" "cat.DATA_NAME(x, \"bar\", y) cat.concat(y, \"foo\", z) insert cat.DATA_NAME(x, \"bar\", z)" "UPDATE r_data_main SET data_name = (data_name||'foo') WHERE resc_id = 'bar'"
        it "test translate sql insert 12" $ do
            testTranslateSQLInsert "cat" "cat.DATA_NAME(x, \"bar\", y) cat.substr(y, 0, 1, z) insert cat.DATA_NAME(x, \"bar\", z)" "UPDATE r_data_main SET data_name = substr(data_name,0,1) WHERE resc_id = 'bar'"
        it "test translate sql insert 13" $ do
            testTranslateSQLInsert "cat" "cat.DATA_NAME(x, \"bar\", y) cat.eq(integer y, z) insert cat.DATA_NAME(x, \"bar\", z)" "UPDATE r_data_main SET data_name = cast(data_name as integer) WHERE resc_id = 'bar'"
        it "test translate sql insert 14" $ do
            testTranslateSQLInsert "cat" "cat.DATA_NAME(x, \"bar\", y) cat.strlen(y, l0) cat.add(l0,-1,l) cat.substr(y, l, l0, z) insert cat.DATA_NAME(x, \"bar\", z)" "UPDATE r_data_main SET data_name = substr(data_name,(length(data_name)+-1),length(data_name)) WHERE resc_id = 'bar'"
        it "test translate sql insert 15" $ do
            testTranslateSQLInsert "cat" "cat.DATA_NAME(x, \"bar\", y) cat.add(y, 1, z) insert cat.DATA_NAME(x, \"bar\", z)" "UPDATE r_data_main SET data_name = (data_name+1) WHERE resc_id = 'bar'"
        it "test translate sql insert 16" $ (do
                qu <- qParseStandardQuery "cat" "cat.add(y, 1, z)"
                sql <- translateQuery2 <$> (sqlStandardTrans "cat") <*> pure Set.empty <*> pure qu
                print (show ((\(_,x,_) -> x)sql))
            ) `shouldThrow` anyException
        it "test translate sql 17" $ do
            qu <- qParseStandardQuery "cat" "cat.add(y, 1, z)"
            sql <- translateQuery1 <$> (sqlStandardTrans "cat") <*> pure (Set.singleton (Var "z")) <*> pure qu <*> pure (Set.singleton (Var "y"))
            (serialize ((\(_,x,_) -> x)sql)) `shouldBe` "SELECT (?+1) AS \"z\""
        it "test translate sql insert 18" $ do
                qu <- qParseStandardQuery "cat" "cat.DATA_NAME(x, \"bar\", y) | cat.DATA_NAME(x, \"bar\", y)"
                sql <- supported <$> (GenericDatabase <$> (sqlStandardTrans "cat") <*> pure () <*> pure "cat" <*> standardPreds) <*> pure mempty <*> pure qu <*> pure Set.empty
                sql `shouldBe` False
        it "test translate sql insert 19" $ do
                qu <- qParseStandardQuery "cat" "cat.DATA_NAME(x, \"bar\", y) (cat.DATA_NAME(x, \"bar\", y) | cat.DATA_NAME(x, \"bar\", y))"
                sql <- supported <$> (GenericDatabase <$> (sqlStandardTrans "cat") <*> pure () <*> pure "cat" <*> standardPreds) <*> pure mempty <*> pure qu <*> pure Set.empty
                sql `shouldBe` True
        it "test translate sql insert 20" $ do
                qu <- qParseStandardQuery "cat" "cat.DATA_NAME(x, \"bar\", y) | cat.DATA_NAME(x, \"bar\", y)"
                sql <- supported <$> (GenericDatabase <$> (sqlStandardTrans "cat") <*> pure () <*> pure "cat" <*> standardPreds) <*> pure mempty <*> pure qu <*> pure (Set.fromList [Var "x", Var "y"])
                sql `shouldBe` True
        it "test sql query supported" $ do
                qu <- qParseStandardQuery "cat" "cat.DATA_NAME(x, y, n) cat.DATA_SIZE(x, y, s)"
                sql <- supported <$> (GenericDatabase <$> (sqlStandardTrans "cat") <*> pure () <*> pure "cat" <*> standardPreds) <*> pure mempty <*> pure qu <*> pure Set.empty
                sql `shouldBe` True
