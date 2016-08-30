{-# LANGUAGE FlexibleContexts, FlexibleInstances, StandaloneDeriving, OverloadedStrings #-}

module Main where
import Translation
import QueryPlan
import DB.DB
import DB.ResultStream
import FO.Data
import Parser
import SQL.SQL
import qualified ICAT as ICAT
import qualified SQL.ICAT as SQL.ICAT
import Cypher.ICAT
import Cypher.Cypher
import qualified Cypher.Cypher as Cypher
import DB.GenericDatabase
import InMemory
import Utils

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

{- newtype Char2 = Char2 {unChar2 :: Char}

instance Arbitrary Char2 where
    arbitrary = Char2 <$> choose ('a', 'z')

class ArbitraryN a where
    arbitraryN :: Int -> Gen a

instance ArbitraryN PureFormula where
    arbitraryN 0 = Atomic <$> (Atom <$> arbitrary <*> arbitrary)
    arbitraryN n | n > 0 = oneof [conj <$> arbitraryN (n - 1), disj <$> arbitraryN (n - 1), Not <$> arbitraryN (n-1), Exists <$> (Var <$> string2) <*> arbitraryN (n - 1)]
    arbitraryN n = error (show n)

instance ArbitraryN a => ArbitraryN [a] where
    arbitraryN n = do
        (Positive m) <- arbitrary
        let n' = n `quot` m + 1
        replicateM m (arbitraryN n')

deriving instance Eq QueryPlan
deriving instance Show QueryPlan
deriving instance Eq PureQueryPlan

string2 :: Gen String
string2 = (:) <$> (unChar2 <$> arbitrary) <*> (map unChar2 <$> arbitrary)

text2 :: Gen T.Text
text2 = T.pack <$> string2

instance Arbitrary PredType where
    arbitrary = PredType <$> arbitrary <*> arbitrary

instance Arbitrary PredKind where
    arbitrary = oneof [return ObjectPred, return PropertyPred]

instance Arbitrary ParamType where
    arbitrary = oneof [Key <$> arbitrary, Property <$> arbitrary]

instance Arbitrary Pred where
    arbitrary = Pred <$> (UQPredName <$> string2) <*> arbitrary

instance Arbitrary Expr where
    arbitrary = oneof [VarExpr <$> Var <$> string2, IntExpr <$> arbitrary, StringExpr <$> text2]

instance Arbitrary Sign where
    arbitrary = oneof [return Pos, return Neg]

instance Arbitrary Atom where
    arbitrary = Atom <$> arbitrary <*> arbitrary

instance Arbitrary Lit where
    arbitrary = Lit <$> arbitrary <*> arbitrary

instance Arbitrary ResultValue where
    arbitrary = oneof [IntValue <$> arbitrary, StringValue <$> text2]

instance Arbitrary (MapDB m) where
    arbitrary = do
        (Positive m) <- arbitrary
        let arbitraryList = oneof [replicateM m (IntValue <$> arbitrary), replicateM m (StringValue <$> text2)]
        MapDB <$> string2 <*> string2 <*> (zip <$> arbitraryList <*> arbitraryList)

newtype LimitedMapDB m = LimitedMapDB (MapDB m) deriving Show
instance Arbitrary (LimitedMapDB m) where
    arbitrary = do
        (Positive m) <- arbitrary
        (Positive m1) <- arbitrary
        strList <- replicateM m text2
        let arbitraryList = replicateM m1 (oneof ((return . StringValue) <$> strList))
        LimitedMapDB <$> (MapDB <$> string2 <*> string2 <*> (zip <$> arbitraryList <*> arbitraryList))

runQuery ::  (ResultRow row, Show row) => [Database IO row] -> String -> [row]
runQuery dbs query2 =
    case runParser progp (constructDBPredMap dbs) "" query2 of
            Left err -> let errmsg = "cannot parse " ++ query2 ++ show err in trace errmsg error ""
            Right (qu2, _) -> unsafePerformIO (getAllResults2 dbs  qu2)
-}

standardPreds = unsafePerformIO (ICAT.loadPreds "gen/ICATGen")
standardMappings = unsafePerformIO (SQL.ICAT.loadMappings "gen/SQL/ICATGen")

sqlStandardTrans ns = SQL.ICAT.sqlStandardTrans ns standardPreds standardMappings (Just "nextid")

standardPredMap = ICAT.standardPredMap standardPreds

parseStandardQuery ::  String -> Formula
parseStandardQuery query2 =
    case runParser progp (mempty, standardPredMap, mempty) "" query2 of
            Left err -> let errmsg = "cannot parse " ++ query2 ++ show err in trace errmsg error ""
            Right ([Execute qu2], _) -> qu2
qParseStandardQuery ::  String -> String -> Formula
qParseStandardQuery ns query2 =
    case runParser progp (mempty, ICAT.qStandardPredsMap ns standardPreds, mempty) "" query2 of
            Left err -> let errmsg = "cannot parse " ++ query2 ++ show err in trace errmsg error ""
            Right ([Execute qu2], _) -> qu2
{-
parseStandardInsert :: String -> Query
parseStandardInsert  = parseStandardQuery

to1 :: Ord k => [k] -> [Map k t] -> [[t]]
to1 vars  = map (\res -> [res ! var | var <- vars])

to2 :: [(a,a)] -> [[a]]
to2 = map ((a,b) -> [a, b])

db :: MapDB DBAdapterMonad
db = MapDB "Container" "elem" [(StringValue "ObjA", StringValue "ObjB"),
    (StringValue "ObjB", StringValue "ObjC"), (StringValue "ObjB", StringValue "ObjD"),
    (StringValue "ObjD", StringValue "ObjA"), (StringValue "ObjD", StringValue "ObjP"),
    (StringValue "ObjQ", StringValue "ObjR"), (StringValue "ObjR", StringValue "ObjS")]

db2 :: EqDB DBAdapterMonad
db2 = EqDB "Eq"

-- test that simple query of the form p(x,y) returns all rows
test0 :: MapDB IO -> Bool
test0 db = case db of
    MapDB name predName2 rows ->
        let query2 = name ++ "." ++ predName2 ++ "(x,y) return x y"
            results = runQuery [Database db] query2 in
            to1 [Var "x", Var "y"] results == to2 rows

-- test that simple query of the form p(x,y) p(y,z)
test1 :: LimitedMapDB IO -> Bool
test1 (LimitedMapDB db) = case db of
    MapDB name predName2 rows ->
        let query2 = name ++ "." ++ predName2 ++ "(x,y)" ++ name ++ "." ++ predName2 ++ "(y,z) return x y z"
            results = to1 [Var "x",Var "y",Var "z"] (runQuery [Database db] query2)
            rr = to2 rows in
            results ==  [ [a1, a2, b2] | [a1,a2] <- rr, [b1,b2] <- rr, a2 == b1]

-- test that simple query of the form p(x,y) ~exists p(y,z)
test2 :: LimitedMapDB IO -> Bool
test2 (LimitedMapDB db) = case db of
    MapDB name predName2 rows ->
        let query2 = name ++ "." ++ predName2 ++ "(x,y)[~(exists z." ++ name ++ "." ++ predName2 ++ "(y,z))] return x y"
            results = to1 [Var "x",Var "y"] (runQuery [Database db] query2)
            rr = to2 rows in
            results == [ [a,b] | [a,b] <- rr, null [ [a1,b1] | [a1,b1] <- rr, b==a1]]

-- test that simple query of the form p(x,y) exists p(y,z)
test3 :: LimitedMapDB IO -> Bool
test3 (LimitedMapDB db) = case db of
    MapDB name predName2 rows ->
        let query2 = name ++ "." ++ predName2 ++ "(x,y)[exists z." ++ name ++ "." ++ predName2 ++ "(y,z)] return x y"
            results = to1 [Var "x", Var "y"] (runQuery [Database db] query2)
            rr = to2 rows in
            -- trace ("rr: " ++show rr++"\nresults: "++show results)
                results == [ [a,b] | [a,b] <- rr, not (null [ [a1,b1] | [a1,b1] <- rr, b==a1])]

-- test that simple query of the form p(x,y) p(y,z) ~p(x,z)
test4 :: LimitedMapDB IO -> Bool
test4 (LimitedMapDB db) = case db of
    MapDB name predName2 rows ->
        let query2 = name ++ "." ++ predName2 ++ "(x,y)" ++ name ++ "." ++ predName2 ++ "(y,z)[~"++name ++ "." ++ predName2++"(x,z)] return x y z"
            results = to1 [Var "x", Var "y", Var "z"] (runQuery [Database db] query2)
            rr = to2 rows in
            -- trace ("rr: " ++concatMap (\x-> show x++"\n") rr ++"\nresults: "++concatMap (\x-> show x++"\n") results)
                results == [ [a,b,c] | [a,b,c] <- [ [a1, a2, b2] | [a1,a2] <- rr, [b1,b2] <- rr, a2 == b1], [a, c] `notElem` rr]

-- test that simple query of the form p(x,y) | p(y,x)
test5 :: LimitedMapDB IO -> Bool
test5 (LimitedMapDB db) = case db of
    MapDB name predName2 rows ->
        let query2 = name ++ "." ++ predName2 ++ "(x,y)|"++name ++ "." ++ predName2 ++ "(y,x) return x y"
            results = to1 [Var "x", Var "y"] (runQuery [Database db, Database (EqDB "")] query2)
            rr = to2 rows in
            -- trace ("rr: " ++concatMap (\x-> show x++"\n") rr ++"\nresults: "++concatMap (\x-> show x++"\n") results)
                results == [ [a,b] | [a,b] <- rr] ++ [[b,a]|[ a,b]<-rr ]

testsFunc ::(Show a, Arbitrary a)=>(a-> Bool)->IO ()
testsFunc = quickCheckWith stdArgs {maxSize = 5}
 -}
translateQuery2 :: SQLTrans -> Set.Set Var -> Formula -> SQLQuery
translateQuery2 trans vars qu = translateQuery1 trans vars qu Set.empty

translateQuery1 :: SQLTrans -> Set.Set Var -> Formula -> Set.Set Var -> SQLQuery
translateQuery1 trans vars qu env =
  let (SQLTrans  builtin predtablemap _) = trans
      env2 = foldl (\map2 key@(Var w)  -> insert key (SQLParamExpr w) map2) empty env in
      fst (runNew (runStateT (translateQueryToSQL (Set.toAscList vars) qu) (TransState {builtin = builtin, predtablemap = predtablemap, repmap = env2, tablemap = empty, nextid="nextid"})))

testTranslateSQLInsert :: String -> String -> String -> IO ()
testTranslateSQLInsert ns qus sqls = do
  let qu = qParseStandardQuery "cat" qus
  let sql = translateQuery2 (sqlStandardTrans "cat") Set.empty qu
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
                return (v1, v2)) (TransState  (BuiltIn empty) empty empty empty "nextid")))
            v1 `shouldBe` SQLVar "t"
            v2 `shouldBe` SQLVar "t0"
        it "test parse query 0" $ do
            let formula = parseStandardQuery "DATA_NAME(x, y, z) return x y z"
            formula `shouldBe`
              FSequencing
                (FAtomic (Atom (fromMaybe (error "no such predicate") (lookupObject (ObjectPath mempty "DATA_NAME") standardPredMap) ) [VarExpr (Var "x"), VarExpr (Var "y"), VarExpr (Var "z")]))
                (FReturn [Var "x", Var "y", Var "z"])

        it "test parse query 1" $ do
            let formula = parseStandardQuery "let a = count (DATA_NAME(x, y, z)) return a"
            formula `shouldBe`
              FSequencing
                (Aggregate (Summarize [(Var "a", Count)]) (FAtomic (Atom (fromMaybe (error "no such predicate") (lookupObject (ObjectPath mempty "DATA_NAME") standardPredMap) ) [VarExpr (Var "x"), VarExpr (Var "y"), VarExpr (Var "z")])))
                (FReturn [Var "a"])

        it "test parse query 1.1" $ do
            let formula = parseStandardQuery "let a = count, b = max x, c = min x (DATA_NAME(x, y, z)) return a"
            formula `shouldBe`
              FSequencing
                (Aggregate (Summarize [(Var "a", Count), (Var "b", Max (Var "x")), (Var "c", Min (Var "x"))]) (FAtomic (Atom (fromMaybe (error "no such predicate") (lookupObject (ObjectPath mempty "DATA_NAME") standardPredMap) ) [VarExpr (Var "x"), VarExpr (Var "y"), VarExpr (Var "z")])))
                (FReturn [Var "a"])

        it "test translate sql query 0.1" $ do
            let qu = qParseStandardQuery "cat" "cat.DATA_NAME(x, y, z)"
            let sql = translateQuery2 (sqlStandardTrans "cat") (Set.fromList [Var "x", Var "y", Var "z"]) qu
            sql `shouldBe` ([Var "x", Var "y", Var "z"], SQLQueryStmt (SQLQuery
                    [
                        (Var "x", SQLColExpr (SQLVar "r_data_main", "data_id")),
                        (Var "y", SQLColExpr (SQLVar "r_data_main", "resc_name")),
                        (Var "z", SQLColExpr (SQLVar "r_data_main", "data_name"))]
                    [OneTable "r_data_main" (SQLVar "r_data_main")]
                    SQLTrueCond
                    []
                    top), [])
        it "test translate sql query 0.2" $ do
            let qu = qParseStandardQuery "cat" "let a = count (cat.DATA_NAME(x, y, z))"
            let sql = translateQuery2 (sqlStandardTrans "cat") (Set.fromList [Var "a"]) qu
            sql `shouldBe` ([Var "a"], SQLQueryStmt (SQLQuery
                    [
                        (Var "a", SQLFuncExpr "count" [(SQLExprText "*")])]
                    [OneTable "r_data_main" (SQLVar "r_data_main")]
                    SQLTrueCond
                    []
                    top), [])
        it "test translate sql query 0.2" $ do
            let qu = qParseStandardQuery "cat" "let a = count, b = max x, c = min x (cat.DATA_NAME(x, y, z))"
            let sql = translateQuery2 (sqlStandardTrans "cat") (Set.fromList [Var "a", Var "b", Var "c"]) qu
            sql `shouldBe` ([Var "a", Var "b", Var "c"], SQLQueryStmt (SQLQuery
                    [
                        (Var "a", SQLFuncExpr "count" [SQLExprText "*"]),
                        (Var "b", SQLFuncExpr "max" [SQLColExpr (SQLVar "r_data_main", "data_id")]),
                        (Var "c", SQLFuncExpr "min" [SQLColExpr (SQLVar "r_data_main", "data_id")])]
                    [OneTable "r_data_main" (SQLVar "r_data_main")]
                    SQLTrueCond
                    []
                    top), [])
        {- it "test translate sql query 0" $ do
            let qu = parseStandardQuery "DATA_NAME(x, y, z) return x y"
            let sql = translateQuery2 (sqlStandardTrans "") qu
            sql `shouldBe` ([Var "x", Var "y"], SQLQ (SQL0 [SQLColExpr (SQLVar "r_data_main", "data_id"), SQLColExpr (SQLVar "r_data_main", "data_name")] [OneTable "r_data_main" (SQLVar "r_data_main")] SQLTrueCond), []) -}
        it "test translate sql query with param" $ do
            let qu = qParseStandardQuery "cat" "cat.DATA_NAME(x, y, z)"
            let sql = translateQuery1 (sqlStandardTrans "cat") (Set.fromList [Var "x", Var "y", Var "z"]) qu (Set.fromList [Var "w"])
            sql `shouldBe` ([Var "x", Var "y", Var "z"], SQLQueryStmt (SQLQuery
                    [
                        (Var "x", SQLColExpr (SQLVar "r_data_main", "data_id")),
                        (Var "y", SQLColExpr (SQLVar "r_data_main", "resc_name")),
                        (Var "z", SQLColExpr (SQLVar "r_data_main", "data_name"))]
                    [OneTable "r_data_main" (SQLVar "r_data_main")]
                    SQLTrueCond
                    []
                    top), [])
            (params sql) `shouldBe` []
        it "test translate sql query with param 2" $ do
            let qu = qParseStandardQuery "cat" "cat.DATA_NAME(x, y, z)"
            let sql = translateQuery1 (sqlStandardTrans "cat") (Set.fromList [Var "x", Var "y"]) qu (Set.fromList [Var "z"])
            ((\(x,_,_) -> x) sql) `shouldBe` [Var "x", Var "y"]
            serialize ((\(_,x,_) -> x) sql) `shouldBe` "SELECT r_data_main.data_id AS x,r_data_main.resc_name AS y FROM r_data_main r_data_main WHERE r_data_main.data_name = ?"
            (params sql) `shouldBe` [Var "z"]
        it "test translate sql insert 0" $ do
            testTranslateSQLInsert "cat" "cat.DATA_NAME(x, y, \"foo\") insert cat.DATA_SIZE(x, y, 1000)" "UPDATE r_data_main SET data_size = 1000 WHERE data_name = 'foo'"
        it "test translate sql insert 1" $ do
            testTranslateSQLInsert "cat" "insert cat.DATA_OBJ(1, \"bar\") cat.DATA_NAME(1, \"bar\", \"foo\") cat.DATA_SIZE(1, \"bar\", 1000)" "INSERT INTO r_data_main (data_id,resc_name,data_name,data_size) VALUES (1,'bar','foo',1000)"
        it "test translate sql insert 2" $ do
            testTranslateSQLInsert "cat" "cat.COLL_NAME(a,c) insert cat.DATA_OBJ(1, \"foo\") cat.DATA_NAME(1, \"foo\", c) cat.DATA_SIZE(1, \"foo\", 1000)" "INSERT INTO r_data_main (data_id,resc_name,data_name,data_size) SELECT 1,\'foo\',r_coll_main.coll_name,1000 FROM r_coll_main r_coll_main"
        it "test translate sql insert 3" $ do
            testTranslateSQLInsert "cat" "cat.COLL_NAME(2,c) insert cat.DATA_OBJ(1, \"foo\") cat.DATA_NAME(1, \"foo\", c) cat.DATA_SIZE(1, \"foo\", 1000)" "INSERT INTO r_data_main (data_id,resc_name,data_name,data_size) SELECT 1,\'foo\',r_coll_main.coll_name,1000 FROM r_coll_main r_coll_main WHERE r_coll_main.coll_id = 2"
        it "test translate sql insert 4" $ do
            testTranslateSQLInsert "cat" "insert ~cat.DATA_NAME(1, \"foo\", c)" "UPDATE r_data_main SET data_name = NULL WHERE (data_id = 1 AND resc_name = 'foo')"
        it "test translate sql insert 4.01" $ do
            testTranslateSQLInsert "cat" "cat.DATA_NAME(1, y, c) insert ~cat.DATA_NAME(1, y, c)" "UPDATE r_data_main SET data_name = NULL WHERE data_id = 1"
        it "test translate sql insert 4.1" $ do
            testTranslateSQLInsert "cat" "cat.DATA_NAME(1, \"foo\", c) insert ~cat.DATA_NAME(1, \"foo\", c)" "UPDATE r_data_main SET data_name = NULL WHERE (data_id = 1 AND resc_name = 'foo')"
        it "test translate sql insert 4.2" $ do
            testTranslateSQLInsert "cat" "cat.DATA_PATH(1, \"foo\", c) insert ~cat.DATA_NAME(1, \"foo\", c)" "UPDATE r_data_main SET data_name = NULL WHERE ((data_id = 1 AND resc_name = 'foo') AND data_name = data_path)"
        it "test translate sql insert 5" $ do
            testTranslateSQLInsert "cat" "insert ~cat.DATA_NAME(1, \"bar\", c) cat.DATA_NAME(1, \"bar\", \"foo\")" "UPDATE r_data_main SET data_name = 'foo' WHERE (data_id = 1 AND resc_name = 'bar')"
        it "test translate sql insert 5.1" $ do
            testTranslateSQLInsert "cat" "cat.DATA_NAME(1, \"bar\", c) insert ~cat.DATA_NAME(1, \"bar\", c) cat.DATA_NAME(1, \"bar\", \"foo\")" "UPDATE r_data_main SET data_name = 'foo' WHERE (data_id = 1 AND resc_name = 'bar')"
        it "test translate sql insert 5.2" $ do
            testTranslateSQLInsert "cat" "cat.DATA_PATH(1, \"bar\", c) insert ~cat.DATA_NAME(1, \"bar\", c) cat.DATA_NAME(1, \"bar\", \"foo\")" "UPDATE r_data_main SET data_name = 'foo' WHERE (data_id = 1 AND resc_name = 'bar')"
        it "test translate sql insert 6" $ do
            testTranslateSQLInsert "cat" "cat.DATA_OBJ(x, \"bar\") insert ~cat.DATA_NAME(x, \"bar\", c) cat.DATA_NAME(x, \"bar\", \"foo\")" "UPDATE r_data_main SET data_name = 'foo' WHERE resc_name = 'bar'"
        it "test translate sql insert 7" $ do
            testTranslateSQLInsert "cat" "cat.DATA_NAME(x, \"bar\", \"foo1\") insert ~cat.DATA_NAME(x, \"bar\", \"foo1\") cat.DATA_NAME(x, \"bar\", \"foo\")" "UPDATE r_data_main SET data_name = 'foo' WHERE (resc_name = 'bar' AND data_name = 'foo1')"
        it "test translate sql insert 7.1" $ do
            testTranslateSQLInsert "cat" "cat.DATA_NAME(x, \"bar\", \"foo1\") insert ~cat.DATA_NAME(x, \"bar\", \"foo1\")" "UPDATE r_data_main SET data_name = NULL WHERE (resc_name = 'bar' AND data_name = 'foo1')"
        it "test translate sql insert 7.2" $ do
            let form = parseStandardQuery "insert ~DATA_NAME(x, \"bar\", \"foo1\") DATA_SIZE(x, \"bar\", 1000)"
            let sql = supported (GenericDatabase (sqlStandardTrans "") () "cat" standardPreds) form Set.empty
            sql `shouldBe` False
            -- length sql `shouldBe` 2
            -- show (sql !! 0) `shouldBe` "UPDATE r_data_main SET data_size = 1000"
            -- show (sql !! 1) `shouldBe` "UPDATE r_data_main SET data_name = NULL WHERE data_name = 'foo1'"
        it "test translate sql insert 8" $ do
            testTranslateSQLInsert "cat" "cat.DATA_NAME(x, \"bar1\", \"bar\") insert cat.DATA_NAME(x, \"bar1\", \"foo\")" "UPDATE r_data_main SET data_name = 'foo' WHERE (resc_name = 'bar1' AND data_name = 'bar')"
        it "test translate sql insert 9" $ do
            testTranslateSQLInsert "cat" "insert ~cat.DATA_OBJ(1, \"foo\")" "DELETE FROM r_data_main WHERE (data_id = 1 AND resc_name = 'foo')"
        it "test tranlate sql insert 10" $ (
            let qu = qParseStandardQuery "cat" "insert ~cat.DATA_OBJ(x, \"foo\")"
                sql = translateQuery2 (sqlStandardTrans "cat") Set.empty qu in
                print (show ((\(_,x,_) -> x)sql))
            ) `shouldThrow` anyException
        {- it "test translate cypher query 0" $ do
            let qu = parseStandardQuery "DATA_NAME(x, y, z) return x y"
            let (_, sql) = translateQuery2 (cypherTrans "") qu
            print sql
            show sql `shouldBe` "MATCH (var:DataObject) RETURN var.object_id,var.data_name"
        it "test translate cypher insert 0" $ do
            let qu = parseStandardInsert "DATA_NAME(x, \"foo\") insert DATA_SIZE(x, 1000)"
            let sql = translateInsert (cypherTrans "") qu
            show (snd sql) `shouldBe` "MATCH (var:DataObject{data_name:'foo'}) SET var.data_size = 1000"
        it "test translate cypher insert 1" $ do
            let qu = parseStandardInsert "insert DATA_OBJ(1) DATA_NAME(1, \"foo\") DATA_SIZE(1, 1000)"
            let sql = translateInsert (cypherTrans "") qu
            show (snd sql) `shouldBe` "CREATE (var:DataObject{object_id:1,data_name:'foo',data_size:1000})"
        it "test translate cypher insert 2" $ do
            let qu = parseStandardInsert "COLL_NAME(a,c) insert DATA_OBJ(1) DATA_NAME(1, c) DATA_SIZE(1, 1000)"
            let sql = translateInsert (cypherTrans "") qu
            show (snd sql) `shouldBe` "MATCH (var:Collection) CREATE (var2:DataObject{object_id:1,data_name:var.coll_name,data_size:1000})"
        it "test translate cypher insert 3" $ do
            let qu = parseStandardInsert "COLL_NAME(2,c) insert DATA_OBJ(1) DATA_NAME(1, c) DATA_SIZE(1, 1000)"
            let sql = translateInsert (cypherTrans "") qu
            show (snd sql) `shouldBe` "MATCH (var:Collection{object_id:2}) CREATE (var2:DataObject{object_id:1,data_name:var.coll_name,data_size:1000})"
        it "test translate cypher insert 4" $ do
            let qu = parseStandardInsert "insert ~DATA_NAME(1, c)"
            let sql = translateInsert (cypherTrans "") qu
            show (snd sql) `shouldBe` "MATCH (var:DataObject{object_id:1}) SET var.data_name = NULL"
        it "test translate cypher insert 5" $ do
            let qu = parseStandardInsert "insert ~DATA_NAME(1, c) DATA_NAME(1, \"foo\")"
            let sql = translateInsert (cypherTrans "") qu
            show (snd sql) `shouldBe` "MATCH (var:DataObject{object_id:1}) SET var.data_name = 'foo'"
        it "test translate cypher insert 6" $ do
            let qu = parseStandardInsert "insert ~DATA_NAME(x, c) DATA_NAME(x, \"foo\")"
            let sql = translateInsert (cypherTrans "") qu
            show (snd sql) `shouldBe` "MATCH (var:DataObject) SET var.data_name = 'foo'"
        {- it "test translate cypher insert 7 E" $ do
            let qu = parseStandardInsert "insert ~DATA_NAME(x, \"foo1\") DATA_NAME(x, \"foo\")"
            val <- validate verifier2 qu
            val `shouldNotBe` Nothing
            -- let sql = translateInsert (cypherTrans "") qu
            -- show sql `shouldBe` "MATCH (var:DataObject) SET var.data_name = 'foo'" -}
        it "test translate cypher insert 7.1" $ do
            let qu = parseStandardInsert "insert ~DATA_NAME(x, \"foo1\")"
            let sql = translateInsert (cypherTrans "") qu
            show (snd sql) `shouldBe` "MATCH (var:DataObject{data_name:'foo1'}) SET var.data_name = NULL"
        {- it "test translate cypher insert 7.2 E" $ do
            let qu = parseStandardInsert "insert ~DATA_NAME(x, \"foo1\") DATA_SIZE(x, 1000)"
            val <- validate verifier2 qu
            val `shouldNotBe` Nothing
            -- let sql = translateInsert cypherTrans qu
            -- show sql `shouldBe` "MATCH (var:DataObject) SET var.data_size = 1000 WITH (var:DataObject{data_name:'foo1'}) SET var.data_name = NULL" -}
        it "test translate cypher insert 8" $ do
            let qu = parseStandardInsert "DATA_NAME(x, \"bar\") insert DATA_NAME(x, \"foo\")"
            let sql = translateInsert (cypherTrans "") qu
            show (snd sql) `shouldBe` "MATCH (var:DataObject{data_name:'bar'}) SET var.data_name = 'foo'"
        it "test translate cypher insert 9" $ do
            let qu = parseStandardInsert "insert ~DATA_OBJ(1)"
            let sql = translateInsert (cypherTrans "") qu
            show (snd sql) `shouldBe` "MATCH (var:DataObject{object_id:1}) DELETE var"
        it "test tranlate cypher insert 10" $ do
            let qu = parseStandardInsert "insert ~DATA_OBJ(x)"
            let sql = translateInsert (cypherTrans "") qu
            show (snd sql) `shouldBe` "MATCH (var:DataObject) DELETE var"
        let a = (var "a")
        let b = (var "b")
        let c = (var "c")
        let d = (var "d")
        let e = (var "e")
        let f = (var "f")
        it "monoid <> graph 0" $ do
            GraphPattern [nodevlp "0" "l" [(PropertyKey "p",a)]] <> GraphPattern [nodevlp "0" "l" [(PropertyKey "q", b)]] `shouldBe`
                GraphPattern [nodevlp "0" "l" [(PropertyKey "p",a), (PropertyKey "q", b)]]
        it "monoid <> graph 1" $ do
            GraphPattern [nodevlp "0" "l" [(PropertyKey "p",a)]] <> GraphPattern [nodevlp "0" "l" [(PropertyKey "p", b)]] `shouldBe`
                GraphPattern [nodevlp "0" "l" [(PropertyKey "p",a)], nodevlp "0" "l" [(PropertyKey "p", b)]]
        it "monoid <> graph 2" $ do
            GraphPattern [nodevlp "0" "l" [(PropertyKey "p",a), (PropertyKey "q", b)]] <> GraphPattern [nodevlp "0" "l" [(PropertyKey "q", b)]] `shouldBe`
                GraphPattern [nodevlp "0" "l" [(PropertyKey "p",a), (PropertyKey "q", b)]]
        it "unifyOne 0" $ do
            unifyOne ("a" `dot` "l") ["b" `dot` "l"] `shouldBe`
                Just (["b" `dot` "l"], cypherVarExprMap (CypherVar "a") b)
        it "unifyOne 1" $ do
            unifyOne ("a" `dot` "l") ["b" `dot` "x"] `shouldBe`
                Nothing
        it "unifyAll 0" $ do
            let rep = CypherVar "X"
            let exprs = [
                  "a" `dot` "l",
                  "b" `dot` "l",
                  "c" `dot` "m",
                  "d" `dot` "m",
                  "e" `dot` "m",
                  "f" `dot` "z"
                  ]
            unifyAll rep exprs [] `shouldBe`
                ((("b" `dot` "l") Cypher..=. ("e" `dot` "m")) Cypher..&&. (("b" `dot` "l") Cypher..=. ("f" `dot` "z")),
                    CypherVarExprMap (fromList [(CypherVar "X", "b" `dot` "l"), (CypherVar "a", b), (CypherVar "c", e), (CypherVar "d", e)]))


        {- it "test eprover 0" $ do
            let (Query _ rule) = parseStandardQuery "~DATA_NAME(x, y, z) | DATA_OBJ(x) return x y"
            let i = parseStandardInsert "DATA_OBJ(x) insert DATA_NAME(x, \"foo\")"
            r <- validateInsert (verifier2)  [rule] i
            r `shouldBe` Just True

        it "test eprover 1" $ do
            let (Query _ rule) = parseStandardQuery "~DATA_NAME(x, y, z) | DATA_OBJ(x) return x y"
            let i = parseStandardInsert "insert DATA_NAME(x, \"foo\")"
            r <- validateInsert (verifier2)  [rule] i
            r `shouldNotBe` Just True

        it "test eprover 2" $ do
            let (Query _ rule1) = parseStandardQuery "~DATA_NAME(x, y, z) | DATA_OBJ(x) return x y"
            let (Query _ rule2) = parseStandardQuery "~DATA_SIZE(x, y) | DATA_OBJ(x) return x y"
            let i = parseStandardInsert "DATA_SIZE(x, 1000) insert DATA_NAME(x, \"foo\")"
            r <- validateInsert (verifier2)  [rule1, rule2] i
            r `shouldBe` Just True
        it "test eprover 3" $ do
            let (Query _ rule1) = parseStandardQuery "~DATA_NAME(x, y, z) | DATA_OBJ(x) return x y"
            let (Query _ rule2) = parseStandardQuery "~DATA_SIZE(x, y) | DATA_OBJ(x) return x y"
            let i = parseStandardInsert "DATA_NAME(x, \"baz\") insert DATA_NAME(x, \"foo\")"
            r <- validateInsert (verifier2)  [rule1, rule2] i
            r `shouldBe` Just True
        it "test eprover 4" $ do
            let (Query _ rule1) = parseStandardQuery "~DATA_NAME(x, y, z) | DATA_OBJ(x) return x y"
            let (Query _ rule2) = parseStandardQuery "~DATA_SIZE(x, y) | DATA_OBJ(x) return x y"
            let i = parseStandardInsert "insert ~DATA_NAME(x, \"foo\")"
            r <- validateInsert (verifier2)  [rule1, rule2] i
            r `shouldBe` Just True
        it "test eprover 5" $ do
            let (Query _ rule1) = parseStandardQuery "~DATA_NAME(x, y, z) | DATA_OBJ(x) return x y"
            let (Query _ rule2) = parseStandardQuery "~DATA_SIZE(x, y) | DATA_OBJ(x) return x y"
            let i = parseStandardInsert "insert ~DATA_NAME(x, y, z) ~DATA_SIZE(x, 1000) "
            r <- validateInsert (verifier2)  [rule1, rule2] i
            r `shouldBe` Just True
        it "test eprover 6" $ do
            let (Query _ rule1) = parseStandardQuery "~DATA_NAME(x, y, z) | DATA_OBJ(x) return x y"
            let (Query _ rule2) = parseStandardQuery "~DATA_SIZE(x, y) | DATA_OBJ(x) return x y"
            let i = parseStandardInsert "insert ~DATA_NAME(x, y, z) ~DATA_OBJ(x)"
            r <- validateInsert (verifier2)  [rule1, rule2] i
            r `shouldNotBe` Just True
        it "test eprover 7" $ do
            let (Query _ rule1) = parseStandardQuery "~DATA_NAME(x, y, z) | DATA_OBJ(x) return x y"
            let (Query _ rule2) = parseStandardQuery "~DATA_SIZE(x, y) | DATA_OBJ(x) return x y"
            let i = parseStandardInsert "insert ~DATA_NAME(x, y, z) ~DATA_SIZE(x, 1000) ~DATA_OBJ(x)"
            r <- validateInsert (verifier2)  [rule1, rule2] i
            r `shouldNotBe` Just True

        it "test cvc4 0" $ do
            let (Query _ rule) = parseStandardQuery "~DATA_NAME(x, y, z) | DATA_OBJ(x) return x y"
            let i = parseStandardInsert "DATA_OBJ(x) insert DATA_NAME(x, \"foo\")"
            r <- validateInsert (verifier)  [rule] i
            r `shouldBe` Just True

        it "test cvc4 1" $ do
            let (Query _ rule) = parseStandardQuery "~DATA_NAME(x, y, z) | DATA_OBJ(x) return x y"
            let i = parseStandardInsert "insert DATA_NAME(x, \"foo\")"
            r <- validateInsert (verifier)  [rule] i
            r `shouldNotBe` Just True

        it "test cvc4 2" $ do
            let (Query _ rule1) = parseStandardQuery "~DATA_NAME(x, y, z) | DATA_OBJ(x) return x y"
            let (Query _ rule2) = parseStandardQuery "~DATA_SIZE(x, y) | DATA_OBJ(x) return x y"
            let i = parseStandardInsert "DATA_SIZE(x, 1000) insert DATA_NAME(x, \"foo\")"
            r <- validateInsert (verifier)  [rule1, rule2] i
            r `shouldBe` Just True
        it "test cvc4 3" $ do
            let (Query _ rule1) = parseStandardQuery "~DATA_NAME(x, y, z) | DATA_OBJ(x) return x y"
            let (Query _ rule2) = parseStandardQuery "~DATA_SIZE(x, y) | DATA_OBJ(x) return x y"
            let i@(Query _ (FSequencing [cond ,FInsert lits])) = parseStandardInsert "DATA_NAME(x, \"baz\") insert DATA_NAME(x, \"foo\")"
            r <- validateInsert (verifier)  [rule1, rule2] i
            r `shouldBe` Just True
        it "test cvc4 4" $ do
            let (Query _ rule1) = parseStandardQuery "~DATA_NAME(x, y, z) | DATA_OBJ(x) return x y"
            let (Query _ rule2) = parseStandardQuery "~DATA_SIZE(x, y) | DATA_OBJ(x) return x y"
            let i@(Query _ (FInsert lits)) = parseStandardInsert "insert ~DATA_NAME(x, \"foo\")"
            r <- validateInsert (verifier)  [rule1, rule2] i
            r `shouldBe` Just True
        it "test cvc4 5" $ do
            let (Query _ rule1) = parseStandardQuery "~DATA_NAME(x, y, z) | DATA_OBJ(x) return x y"
            let (Query _ rule2) = parseStandardQuery "~DATA_SIZE(x, y) | DATA_OBJ(x) return x y"
            let i@(Query _ (FInsert lits)) = parseStandardInsert "insert ~DATA_NAME(x, y, z) ~DATA_SIZE(x, 1000) "
            r <- validateInsert (verifier)  [rule1, rule2] i
            r `shouldBe` Just True
        it "test cvc4 6" $ do
            let (Query _ rule1) = parseStandardQuery "~DATA_NAME(x, y, z) | DATA_OBJ(x) return x y"
            let (Query _ rule2) = parseStandardQuery "~DATA_SIZE(x, y) | DATA_OBJ(x) return x y"
            let i@(Query _ (FInsert lits)) = parseStandardInsert "insert ~DATA_NAME(x, y, z) ~DATA_OBJ(x)"
            r <- validateInsert (verifier)  [rule1, rule2] i
            r `shouldNotBe` Just True
        it "test cvc4 7" $ do
            let (Query _ rule1) = parseStandardQuery "~DATA_NAME(x, y, z) | DATA_OBJ(x) return x y"
            let (Query _ rule2) = parseStandardQuery "~DATA_SIZE(x, y) | DATA_OBJ(x) return x y"
            let i@(Query _ (FInsert  lits)) = parseStandardInsert "insert ~DATA_NAME(x, y, z) ~DATA_SIZE(x, 1000) ~DATA_OBJ(x)"
            r <- validateInsert (verifier)  [rule1, rule2] i
            r `shouldNotBe` Just True -}

        {- it "test validate insert 0 E" $ do
            let qu = parseStandardInsert "insert ~DATA_NAME(x, \"foo1\") DATA_NAME(x, \"foo\")"
            val <- validate verifier2 qu
            val `shouldNotBe` Nothing
        -- it "test validate insert 1" $ do
        --    let (Query _ rule) = parseStandardQuery "~DATA_NAME(x, y, z) | DATA_OBJ(x) return x y"
        --    let qu = parseStandardInsert "DATA_NAME(x, \"foo\") insert ~DATA_OBJ(x)"
        --    let val = validate rule qu
        --    val `shouldBe` Nothing
        it "test validate insert 2 E" $ do
            let qu = parseStandardInsert "DATA_NAME(x, \"foo\") insert DATA_SIZE(x, 1000)"
            val <- validate verifier2 qu
            val `shouldBe` Nothing
        it "test validate insert 3 E" $ do
            let qu = parseStandardInsert "DATA_NAME(y, \"foo\") insert ~DATA_SIZE(x, 1000)"
            val <- validate verifier2 qu
            val `shouldBe` Nothing -}
        let at p args = Atom (Pred (UQPredName p) (PredType ObjectPred (map (const (Key "String")) args))) args
        let atom p args = Atomic (at p args)
        let v = VarExpr . Var
        let i = IntExpr
        let s = StringExpr
        let a1 = atom "p" [v "X", v "Y"]
        let a2 = atom "q" [v "X", v "Y"]
        let a3 = atom "p" [i 1, i 2]
        let a4 = atom "q" [i 1, i 2]
















        {- it "sat solver" $ do
            let rule = (a1 --> a2) & a3
            let goal = a4
            let x = valid (rule --> goal)
            case x of Just ce -> print ce; _ -> return ()
            x `shouldBe` Nothing
        it "sat solver" $ do
            let rule = (a1 --> a2) & a1
            let goal = a2
            let x = valid (rule --> goal)
            case x of Just ce -> print ce; _ -> return ()
            x `shouldBe` Nothing
        it "sat solver" $ do
            let rule = (a1 --> a2) & a3
            let goal = Not a4
            let x = valid (rule --> goal)
            case x of Just ce -> print ce; _ -> return ()
            x `shouldNotBe` Nothing
        it "sat solver" $ do
            let rule = (a1 --> a2) & a1
            let goal = Not a2
            let x = valid (rule --> goal)
            case x of Just ce -> print ce; _ -> return ()
            x `shouldNotBe` Nothing -}



        it "queryplan1" $ do
            let db = MapDB "mapdb" "p" [(StringValue "a", StringValue "b")]
            let db2 = MapDB "mapdb2" "p" [(StringValue "c", StringValue "c")]
            let query2 = "p(x,y) return x y"
            let results = to1 [Var "x",Var "y"] (runQuery [Database db, Database db2] query2)
            results `shouldBe`  [ [StringValue "a" , StringValue "b"]]

        it "queryplan2" $ do
            let db = MapDB "mapdb" "p" [(StringValue "a", StringValue "b")]
            let db2 = EqDB "eqdb"
            let db3 = EqDB "eqdb2"
            let query2 = "p(x,y) eq(x,\"a\") return x y"
            let results = to1 [Var "x",Var "y"] (runQuery [Database db, Database db2, Database db3] query2)
            results `shouldBe`  [ [StringValue "a" , StringValue "b"]]

        it "queryplan3" $ do
            let db = MapDB "mapdb" "p" [(StringValue "a", StringValue "b")]:: MapDB Identity
            let db2 = (EqDB "eqdb"  :: EqDB Identity)
            let db3 = EqDB "eqdb2":: EqDB Identity
            let query2 = "p(x,y) p(y,z) eq(x,\"a\") return x y"
            let dbs = [Database db, Database db2, Database db3]
            let [p] = getPreds db
            let [eq] = getPreds db3
            let [eq2] = getPreds db2
            eq `shouldBe` eq2
            case runParser progp (constructDBPredMap dbs) "" query2 of
                 Left _ -> error ("cannot parse query: " ++ show query2)
                 Right (qu, _) -> do
                    let qp = queryPlan dbs qu
                    qp `shouldBe` QPSequencing
                        (QPSequencing
                            (Exec (FAtomic (Atom p [VarExpr (Var "x"),VarExpr (Var "y")])) [0])
                            (Exec (FAtomic (Atom p [VarExpr (Var "y"), VarExpr (Var "z")])) [0]))
                        (Exec (FAtomic (Atom eq [VarExpr (Var "x"), StringExpr "a"])) [1,2])
        it "queryplan insert eq" $ do
            let db = MapDB "mapdb" "p" [(StringValue "a", StringValue "b")]
            let db2 = (EqDB "eqdb"  :: EqDB Identity)
            let dbs = [Database db, Database db2]
            let query2 = "insert eq(1,1)"
            let [eqp] = getPreds db2
            let insmap = empty
            case runParser progp (constructDBPredMap dbs) "" query2 of
                 Left _ -> error ("cannot parse query: " ++ show query2)
                 Right (ins, _) -> do
                    let qp = queryPlan dbs  ins
                    qp `shouldBe` (Exec (FInsert (Lit Pos (Atom eqp [IntExpr 1,IntExpr 1]))) [])
                    let qp2 = runIdentity (runExceptT (checkQueryPlan dbs (calculateVars [] [] qp)))
                    show qp2 `shouldBe` "Left (\"no database\",(insert eq(1,1)),fromList [])"
        it "queryplan insert map" $ do
            let db = StateMapDB "mapdb" "p" :: StateMapDB Identity
            let db2 = (EqDB "eqdb" :: EqDB (StateT (Map String [(ResultValue, ResultValue)]) Identity))
            let dbs = [Database db, Database db2]
            let query2 = "insert p(1,1)"
            let [eqp] = getPreds db2
            let [p] = getPreds db
            let insmap = empty
            case runParser progp (constructDBPredMap dbs) "" query2 of
                 Left _ -> error ("cannot parse query: " ++ show query2)
                 Right (ins, _) -> do
                    let qp = queryPlan dbs  ins
                    qp `shouldBe` (Exec (FInsert (Lit Pos (Atom p [IntExpr 1,IntExpr 1]))) [0])
                    let qp2 = runIdentity (evalStateT (runExceptT (checkQueryPlan dbs (calculateVars [] [] qp))) (singleton "mapdb" [(StringValue "a", StringValue "b")]))
                    show qp2 `shouldBe` "Right (fromList [])"
        it "queryplan insert insmap" $ do
            let db = MapDB "mapdb" "p" [(StringValue "a", StringValue "b")]:: MapDB Identity
            let db2 = (EqDB "eqdb"  :: EqDB Identity)
            let dbs = [Database db, Database db2]
            let query2 = "insert p(1,1)"
            let [eqp] = getPreds db2
            let [p] = getPreds db
            let insmap = singleton p ([1], [1])
            case runParser progp (constructDBPredMap dbs) "" query2 of
                 Left _ -> error ("cannot parse query: " ++ show query2)
                 Right (ins, _) -> do
                    let qp = queryPlan dbs  ins
                    -- qp `shouldBe` (Exec (FInsert (Lit Pos (Atom p [IntExpr 1,IntExpr 1]))) [])
                    let qp2 = runIdentity (runExceptT (checkQueryPlan dbs (calculateVars [] [] qp)))
                    show qp2 `shouldBe` "Left (\"no database\",(insert p(1,1)),fromList [])"

        it "schema 0" $ do
            let CypherTrans _ _ mappings = cypherTrans ""
            show (mappings ! Pred (QPredName "" "DATA_NAME") (PredType PropertyPred [Key "Number",Property "Text"] )) `shouldBe` "([1,2],GraphPattern [(0:DataObject{object_id:1})],GraphPattern [(0:DataObject{data_name:2})],[(0,[1])])"
        it "schema 1" $ do
            let CypherTrans _ _ mappings = cypherTrans ""
            show (mappings ! Pred (QPredName "" "DATA_COLL_ID") (PredType PropertyPred [Key "Number",Property "Number"])) `shouldBe` "([1,2],GraphPattern [(d{object_id:1}),(c{object_id:2})],GraphPattern [(d)-[e:DATA_COLL_ID]->(c)],[(d,[1]),(e,[1]),(c,[2])])"
        it "schema 2" $ do
            let CypherTrans _ _ mappings = cypherTrans ""
            show (mappings ! Pred (QPredName "" "USER_GROUP_OBJ") (PredType PropertyPred [Key "Number", Key "Number"])) `shouldBe` "([1,2],GraphPattern [(d{group_user_id:1}),(c{user_id:2})],GraphPattern [(d)-[e:USER_GROUP_OBJ]->(c)],[(d,[1]),(e,[1,2]),(c,[2])])"
        it "schema 3" $ do
            let CypherTrans _ _ mappings = cypherTrans ""
            show (mappings ! Pred (QPredName "" "USER_GROUP_CREATE_TS") (PredType PropertyPred [Key "Number", Key "Number", Property "Text"])) `shouldBe` "([1,2,3],GraphPattern [(d{group_user_id:1}),(c{user_id:2}),(d)-[e:USER_GROUP_CREATE_TS]->(c)],GraphPattern [(e{create_ts:3})],[(d,[1]),(e,[1,2]),(c,[2])])"
-}
