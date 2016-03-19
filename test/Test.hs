{-# LANGUAGE FlexibleContexts, FlexibleInstances #-}

import FO hiding (validateInsert, validate)
import qualified FO
import QueryPlan
import ResultStream
import FO.FunSat
import FO.Data
import FO.Parser
import Parser
import SQL.SQL
import ICAT
import SQL.ICAT
import Cypher.ICAT
import Cypher.Cypher
import qualified Cypher.Cypher as Cypher
import DBQuery
import FO.E
import FO.CVC4

import Test.QuickCheck
import Control.Applicative ((<$>), (<*>))
import Control.Monad (replicateM)
import Text.Parsec (runParser)
import Data.Functor.Identity (runIdentity, Identity)
import Data.Map.Strict ((!), Map, empty, insert, fromList, singleton)
import Control.Monad.Trans.State.Strict (evalState, runState, evalStateT, runStateT)
import Debug.Trace (trace)
import Test.Hspec
import Data.Monoid
import Data.Convertible
import Control.Monad.Trans.Except

validateInsert :: TheoremProver_ p => p -> [Formula] -> Insert -> IO (Maybe Bool)
validateInsert = FO.validateInsert . TheoremProver

validate :: TheoremProver_ p => p -> Insert -> IO (Maybe Lit)
validate = FO.validate . TheoremProver

newtype Char2 = Char2 {unChar2 :: Char}

instance Arbitrary Char2 where
    arbitrary = Char2 <$> choose ('a', 'z')

class ArbitraryN a where
    arbitraryN :: Int -> Gen a

instance ArbitraryN Formula where
    arbitraryN 0 = Atomic <$> (Atom <$> arbitrary <*> arbitrary)
    arbitraryN n | n > 0 = oneof [Conjunction <$> arbitraryN (n - 1), Disjunction <$> arbitraryN (n - 1), Not <$> arbitraryN (n-1), Exists <$> (Var <$> string2) <*> arbitraryN (n - 1)]
    arbitraryN n = error (show n)

instance ArbitraryN a => ArbitraryN [a] where
    arbitraryN n = do
        (Positive m) <- arbitrary
        let n' = n `quot` m + 1
        replicateM m (arbitraryN n')


string2 :: Gen String
string2 = (:) <$> (unChar2 <$> arbitrary) <*> (map unChar2 <$> arbitrary)

instance Arbitrary PredType where
    arbitrary = PredType <$> arbitrary <*> arbitrary

instance Arbitrary PredKind where
    arbitrary = oneof [return ObjectPred, return PropertyPred]

instance Arbitrary ParamType where
    arbitrary = oneof [Key <$> arbitrary, Property <$> arbitrary]

instance Arbitrary Pred where
    arbitrary = Pred <$> string2 <*> arbitrary

instance Arbitrary Expr where
    arbitrary = oneof [VarExpr <$> Var <$> string2, IntExpr <$> arbitrary, StringExpr <$> string2]

instance Arbitrary Sign where
    arbitrary = oneof [return Pos, return Neg]

instance Arbitrary Atom where
    arbitrary = Atom <$> arbitrary <*> arbitrary

instance Arbitrary Lit where
    arbitrary = Lit <$> arbitrary <*> arbitrary

instance Arbitrary ResultValue where
    arbitrary = oneof [IntValue <$> arbitrary, StringValue <$> string2]

instance Arbitrary (MapDB m) where
    arbitrary = do
        (Positive m) <- arbitrary
        let arbitraryList = oneof [replicateM m (IntValue <$> arbitrary), replicateM m (StringValue <$> string2)]
        MapDB <$> string2 <*> string2 <*> (zip <$> arbitraryList <*> arbitraryList)

newtype LimitedMapDB m = LimitedMapDB (MapDB m) deriving Show
instance Arbitrary (LimitedMapDB m) where
    arbitrary = do
        (Positive m) <- arbitrary
        (Positive m1) <- arbitrary
        strList <- replicateM m string2
        let arbitraryList = replicateM m1 (oneof ((return . StringValue) <$> strList))
        LimitedMapDB <$> (MapDB <$> string2 <*> string2 <*> (zip <$> arbitraryList <*> arbitraryList))

runQuery ::  (ResultRow row, Show row) => [Database Identity row] -> String -> [row]
runQuery dbs query2 =
    case runParser progp (constructPredMap dbs) "" query2 of
            Left err -> let errmsg = "cannot parse " ++ query2 ++ show err in trace errmsg error ""
            Right (Q qu2, _) -> runIdentity (dbWithSession dbs $ getAllResults qu2)

parseStandardQuery ::  String -> Query
parseStandardQuery query2 =
    case runParser progp standardPredMap "" query2 of
            Left err -> let errmsg = "cannot parse " ++ query2 ++ show err in trace errmsg error ""
            Right (Q qu2, _) -> qu2

parseStandardInsert :: String -> Insert
parseStandardInsert query2 =
    case runParser progp standardPredMap "" query2 of
            Left err -> let errmsg = "cannot parse " ++ query2 ++ show err in trace errmsg error ""
            Right (I qu2, _) -> qu2

to1 :: Ord k => [k] -> [Map k t] -> [[t]]
to1 vars  = map (\res -> [res ! var | var <- vars])

to2 :: [(a,a)] -> [[a]]
to2 = map (\(a,b) -> [a, b])

-- test that simple query of the form p(x,y) returns all rows
test0 :: MapDB Identity -> Bool
test0 db = case db of
    MapDB _ predName2 rows ->
        let query2 = predName2 ++ "(x,y) return x y"
            results = runQuery [Database db] query2 in
            to1 [Var "x", Var "y"] results == to2 rows

-- test that simple query of the form p(x,y) p(y,z)
test1 :: LimitedMapDB Identity -> Bool
test1 (LimitedMapDB db) = case db of
    MapDB _ predName2 rows ->
        let query2 = predName2 ++ "(x,y)" ++ predName2 ++ "(y,z) return x y z"
            results = to1 [Var "x",Var "y",Var "z"] (runQuery [Database db] query2)
            rr = to2 rows in
            results ==  [ [a1, a2, b2] | [a1,a2] <- rr, [b1,b2] <- rr, a2 == b1]

-- test that simple query of the form p(x,y) ~exists p(y,z)
test2 :: LimitedMapDB Identity -> Bool
test2 (LimitedMapDB db) = case db of
    MapDB _ predName2 rows ->
        let query2 = predName2 ++ "(x,y)~(exists z." ++ predName2 ++ "(y,z)) return x y"
            results = to1 [Var "x",Var "y"] (runQuery [Database db] query2)
            rr = to2 rows in
            results == [ [a,b] | [a,b] <- rr, null [ [a1,b1] | [a1,b1] <- rr, b==a1]]

-- test that simple query of the form p(x,y) exists p(y,z)
test3 :: LimitedMapDB Identity -> Bool
test3 (LimitedMapDB db) = case db of
    MapDB _ predName2 rows ->
        let query2 = predName2 ++ "(x,y)(exists z." ++ predName2 ++ "(y,z)) return x y"
            results = to1 [Var "x", Var "y"] (runQuery [Database db] query2)
            rr = to2 rows in
            -- trace ("rr: " ++show rr++"\nresults: "++show results)
                results == [ [a,b] | [a,b] <- rr, not (null [ [a1,b1] | [a1,b1] <- rr, b==a1])]

-- test that simple query of the form p(x,y) p(y,z) ~p(x,z)
test4 :: LimitedMapDB Identity -> Bool
test4 (LimitedMapDB db) = case db of
    MapDB _ predName2 rows ->
        let query2 = predName2 ++ "(x,y)" ++ predName2 ++ "(y,z)~"++predName2++"(x,z) return x y "
            results = to1 [Var "x", Var "y", Var "z"] (runQuery [Database db] query2)
            rr = to2 rows in
            -- trace ("rr: " ++concatMap (\x-> show x++"\n") rr ++"\nresults: "++concatMap (\x-> show x++"\n") results)
                results == [ [a,b,c] | [a,b,c] <- [ [a1, a2, b2] | [a1,a2] <- rr, [b1,b2] <- rr, a2 == b1], [a, c] `notElem` rr]

-- test that simple query of the form p(x,y) | p(y,x)
test5 :: LimitedMapDB Identity -> Bool
test5 (LimitedMapDB db) = case db of
    MapDB _ predName2 rows ->
        let query2 = predName2 ++ "(x,y)|"++predName2 ++ "(y,x) return x y"
            results = to1 [Var "x", Var "y"] (runQuery [Database db, Database (EqDB "")] query2)
            rr = to2 rows in
            -- trace ("rr: " ++concatMap (\x-> show x++"\n") rr ++"\nresults: "++concatMap (\x-> show x++"\n") results)
                results == [ [a,b] | [a,b] <- rr] ++ [[b,a]|[ a,b]<-rr ]

testsFunc ::(Show a, Arbitrary a)=>(a-> Bool)->IO ()
testsFunc = quickCheckWith stdArgs {maxSize = 5}

loadStandardICATRule :: String -> [Input]
loadStandardICATRule s = parseTPTP standardPredMap s


verifier = CVC4 "external/CVC4/cvc4-1.4-x86_64-linux-opt" 1000 1000 32
verifier2 = E "external/E/PROVER/eprover" 1 32

translateQuery trans qu = fst (translateQueryWithParams trans qu mempty)
translateInsert trans qu = fst (translateInsertWithParams trans qu mempty)
main :: IO ()
main = hspec $ do
    describe "tests" $ do
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
            property $ test5
        it "test that freshSQLVar generates fresh SQL vars" $ do
            (v1, v2) <- return (runNew (evalStateT (do
                v1 <- freshSQLVar "t"
                v2 <- freshSQLVar "t"
                return (v1, v2)) (empty, BuiltIn empty, empty, empty, empty, [])))
            v1 `shouldBe` SQLVar "t"
            v2 `shouldBe` SQLVar "t0"
        it "test parse query 0" $ do
            let (Query vars formula) = parseStandardQuery "DATA_NAME(x, y) return x y"
            vars `shouldBe` [Var "x", Var "y"]
            formula `shouldBe` Atomic (Atom (standardPredMap ! "DATA_NAME") [VarExpr (Var "x"), VarExpr (Var "y")])

        it "test translate sql query 0" $ do
            let qu = parseStandardQuery "DATA_NAME(x, y) return x y"
            let sql = translateQuery sqlStandardTrans qu
            sql `shouldBe` (([Var "x", Var "y"], SQL [(SQLVar "r_data_main", "data_id"), (SQLVar "r_data_main", "data_name")] [OneTable "r_data_main" (SQLVar "r_data_main")] strue))
        it "test translate sql query with param" $ do
            let qu = parseStandardQuery "DATA_NAME(x, y) return x y"
            let sql = translateQueryWithParams sqlStandardTrans qu (singleton (Var "w") (IntValue 0))
            (fst sql) `shouldBe` (([Var "x", Var "y"], SQL [(SQLVar "r_data_main", "data_id"), (SQLVar "r_data_main", "data_name")] [OneTable "r_data_main" (SQLVar "r_data_main")] strue))
            (snd sql) `shouldBe` []
        it "test translate sql query with param 2" $ do
            let qu = parseStandardQuery "DATA_NAME(x, y) return x"
            let sql = translateQueryWithParams sqlStandardTrans qu (singleton (Var "y") (StringValue ""))
            (fst (fst sql)) `shouldBe` [Var "x"]
            show (snd (fst sql)) `shouldBe` "SELECT r_data_main.data_id FROM r_data_main r_data_main WHERE r_data_main.data_name = ?"
            (snd sql) `shouldBe` [Var "y"]
        it "test translate sql insert 0" $ do
            let qu = parseStandardInsert "DATA_NAME(x, \"foo\") insert DATA_SIZE(x, 1000)"
            let sql = translateInsert sqlStandardTrans qu
            show (sql) `shouldBe` "UPDATE r_data_main SET data_size = 1000 WHERE data_name = 'foo'"
        it "test translate sql insert 1" $ do
            let qu = parseStandardInsert "insert DATA_OBJ(1) DATA_NAME(1, \"foo\") DATA_SIZE(1, 1000)"
            let sql = translateInsert sqlStandardTrans qu
            show (sql) `shouldBe` "INSERT INTO r_data_main (data_id,data_name,data_size) VALUES (1,'foo',1000)"
        it "test translate sql insert 2" $ do
            let qu = parseStandardInsert "COLL_NAME(a,c) insert DATA_OBJ(1) DATA_NAME(1, c) DATA_SIZE(1, 1000)"
            let sql = translateInsert sqlStandardTrans qu
            show (sql) `shouldBe` "INSERT INTO r_data_main (data_id,data_name,data_size) SELECT 1,r_coll_main.coll_name,1000 FROM r_coll_main r_coll_main"
        it "test translate sql insert 3" $ do
            let qu = parseStandardInsert "COLL_NAME(2,c) insert DATA_OBJ(1) DATA_NAME(1, c) DATA_SIZE(1, 1000)"
            let sql = translateInsert sqlStandardTrans qu
            show (sql) `shouldBe` "INSERT INTO r_data_main (data_id,data_name,data_size) SELECT 1,r_coll_main.coll_name,1000 FROM r_coll_main r_coll_main WHERE r_coll_main.coll_id = 2"
        it "test translate sql insert 4" $ do
            let qu = parseStandardInsert "insert ~DATA_NAME(1, c)"
            let sql = translateInsert sqlStandardTrans qu
            show (sql) `shouldBe` "UPDATE r_data_main SET data_name = NULL"
        it "test translate sql insert 5" $ do
            let qu = parseStandardInsert "insert ~DATA_NAME(1, c) DATA_NAME(1, \"foo\")"
            let sql = translateInsert sqlStandardTrans qu
            show (sql) `shouldBe` "UPDATE r_data_main SET data_name = 'foo'"
        it "test translate sql insert 6" $ do
            let qu = parseStandardInsert "insert ~DATA_NAME(x, c) DATA_NAME(x, \"foo\")"
            let sql = translateInsert sqlStandardTrans qu
            show (sql) `shouldBe` "UPDATE r_data_main SET data_name = 'foo'"
        it "test translate sql insert 7" $ do
            let qu = parseStandardInsert "insert ~DATA_NAME(x, \"foo1\") DATA_NAME(x, \"foo\")"
            let sql = translateInsert sqlStandardTrans qu
            show (sql) `shouldBe` "UPDATE r_data_main SET data_name = 'foo'"
        it "test translate sql insert 7.1" $ do
            let qu = parseStandardInsert "insert ~DATA_NAME(x, \"foo1\")"
            let sql = translateInsert sqlStandardTrans qu
            show (sql) `shouldBe` "UPDATE r_data_main SET data_name = NULL WHERE data_name = 'foo1'"
        it "test translate sql insert 7.2" $ do
            let (Insert lits formula) = parseStandardInsert "insert ~DATA_NAME(x, \"foo1\") DATA_SIZE(x, 1000)"
            let sql = translateableInsert sqlStandardTrans formula lits
            sql `shouldBe` False
            -- length sql `shouldBe` 2
            -- show (sql !! 0) `shouldBe` "UPDATE r_data_main SET data_size = 1000"
            -- show (sql !! 1) `shouldBe` "UPDATE r_data_main SET data_name = NULL WHERE data_name = 'foo1'"
        it "test translate sql insert 8" $ do
            let qu = parseStandardInsert "DATA_NAME(x, \"bar\") insert DATA_NAME(x, \"foo\")"
            let sql = translateInsert sqlStandardTrans qu
            show (sql) `shouldBe` "UPDATE r_data_main SET data_name = 'foo' WHERE data_name = 'bar'"
        it "test translate sql insert 9" $ do
            let qu = parseStandardInsert "insert ~DATA_OBJ(1)"
            let sql = translateInsert sqlStandardTrans qu
            show (sql) `shouldBe` "DELETE FROM r_data_main WHERE data_id = 1"
        it "test tranlate sql insert 10" $ do
            let qu = parseStandardInsert "insert ~DATA_OBJ(x)"
            let sql = translateInsert sqlStandardTrans qu
            show (sql) `shouldBe` "DELETE FROM r_data_main"
        it "test translate cypher query 0" $ do
            let qu = parseStandardQuery "DATA_NAME(x, y) return x y"
            let (_, sql) = translateQuery cypherTrans qu
            print sql
            show sql `shouldBe` "MATCH (var:DataObject) RETURN var.obj_id,var.data_name"
        it "test translate cypher insert 0" $ do
            let qu = parseStandardInsert "DATA_NAME(x, \"foo\") insert DATA_SIZE(x, 1000)"
            let sql = translateInsert cypherTrans qu
            show sql `shouldBe` "MATCH (var:DataObject{data_name:'foo'}) SET var.data_size = 1000"
        it "test translate cypher insert 1" $ do
            let qu = parseStandardInsert "insert DATA_OBJ(1) DATA_NAME(1, \"foo\") DATA_SIZE(1, 1000)"
            let sql = translateInsert cypherTrans qu
            show sql `shouldBe` "CREATE (var:DataObject{obj_id:1,data_name:'foo',data_size:1000})"
        it "test translate cypher insert 2" $ do
            let qu = parseStandardInsert "COLL_NAME(a,c) insert DATA_OBJ(1) DATA_NAME(1, c) DATA_SIZE(1, 1000)"
            let sql = translateInsert cypherTrans qu
            show sql `shouldBe` "MATCH (var:Collection) CREATE (var2:DataObject{obj_id:1,data_name:var.coll_name,data_size:1000})"
        it "test translate cypher insert 3" $ do
            let qu = parseStandardInsert "COLL_NAME(2,c) insert DATA_OBJ(1) DATA_NAME(1, c) DATA_SIZE(1, 1000)"
            let sql = translateInsert cypherTrans qu
            show sql `shouldBe` "MATCH (var:Collection{obj_id:2}) CREATE (var2:DataObject{obj_id:1,data_name:var.coll_name,data_size:1000})"
        it "test translate cypher insert 4" $ do
            let qu = parseStandardInsert "insert ~DATA_NAME(1, c)"
            let sql = translateInsert cypherTrans qu
            show sql `shouldBe` "MATCH (var:DataObject{obj_id:1}) SET var.data_name = NULL"
        it "test translate cypher insert 5" $ do
            let qu = parseStandardInsert "insert ~DATA_NAME(1, c) DATA_NAME(1, \"foo\")"
            let sql = translateInsert cypherTrans qu
            show sql `shouldBe` "MATCH (var:DataObject{obj_id:1}) SET var.data_name = 'foo'"
        it "test translate cypher insert 6" $ do
            let qu = parseStandardInsert "insert ~DATA_NAME(x, c) DATA_NAME(x, \"foo\")"
            let sql = translateInsert cypherTrans qu
            show sql `shouldBe` "MATCH (var:DataObject) SET var.data_name = 'foo'"
        it "test translate cypher insert 7 E" $ do
            let qu = parseStandardInsert "insert ~DATA_NAME(x, \"foo1\") DATA_NAME(x, \"foo\")"
            val <- validate verifier2 qu
            val `shouldNotBe` Nothing
            -- let sql = translateInsert cypherTrans qu
            -- show sql `shouldBe` "MATCH (var:DataObject) SET var.data_name = 'foo'"
        it "test translate cypher insert 7.1" $ do
            let qu = parseStandardInsert "insert ~DATA_NAME(x, \"foo1\")"
            let sql = translateInsert cypherTrans qu
            show sql `shouldBe` "MATCH (var:DataObject{data_name:'foo1'}) SET var.data_name = NULL"
        it "test translate cypher insert 7.2 E" $ do
            let qu = parseStandardInsert "insert ~DATA_NAME(x, \"foo1\") DATA_SIZE(x, 1000)"
            val <- validate verifier2 qu
            val `shouldNotBe` Nothing
            -- let sql = translateInsert cypherTrans qu
            -- show sql `shouldBe` "MATCH (var:DataObject) SET var.data_size = 1000 WITH (var:DataObject{data_name:'foo1'}) SET var.data_name = NULL"
        it "test translate cypher insert 8" $ do
            let qu = parseStandardInsert "DATA_NAME(x, \"bar\") insert DATA_NAME(x, \"foo\")"
            let sql = translateInsert cypherTrans qu
            show sql `shouldBe` "MATCH (var:DataObject{data_name:'bar'}) SET var.data_name = 'foo'"
        it "test translate cypher insert 9" $ do
            let qu = parseStandardInsert "insert ~DATA_OBJ(1)"
            let sql = translateInsert cypherTrans qu
            show sql `shouldBe` "MATCH (var:DataObject{obj_id:1}) DELETE var"
        it "test tranlate cypher insert 10" $ do
            let qu = parseStandardInsert "insert ~DATA_OBJ(x)"
            let sql = translateInsert cypherTrans qu
            show sql `shouldBe` "MATCH (var:DataObject) DELETE var"
        let a = (var "a")
        let b = (var "b")
        let c = (var "c")
        let d = (var "d")
        let e = (var "e")
        let f = (var "f")
        it "monoid <> graph 0" $ do
            GraphPattern [nodevlp "0" "l" [(PropertyKey "p",a)]] [] <> GraphPattern [nodevlp "0" "l" [(PropertyKey "q", b)]] [] `shouldBe`
                GraphPattern [nodevlp "0" "l" [(PropertyKey "p",a), (PropertyKey "q", b)]] []
        it "monoid <> graph 1" $ do
            GraphPattern [nodevlp "0" "l" [(PropertyKey "p",a)]] [] <> GraphPattern [nodevlp "0" "l" [(PropertyKey "p", b)]] [] `shouldBe`
                GraphPattern [nodevlp "0" "l" [(PropertyKey "p",a)], nodevlp "0" "l" [(PropertyKey "p", b)]] []
        it "monoid <> graph 2" $ do
            GraphPattern [nodevlp "0" "l" [(PropertyKey "p",a), (PropertyKey "q", b)]] [] <> GraphPattern [nodevlp "0" "l" [(PropertyKey "q", b)]] [] `shouldBe`
                GraphPattern [nodevlp "0" "l" [(PropertyKey "p",a), (PropertyKey "q", b)]] []
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


        it "test eprover 0" $ do
            let (Query _ rule) = parseStandardQuery "~DATA_NAME(x, y) | DATA_OBJ(x) return x y"
            let i@(Insert lits cond) = parseStandardInsert "DATA_OBJ(x) insert DATA_NAME(x, \"foo\")"
            r <- validateInsert (verifier2)  [rule] i
            r `shouldBe` Just True

        it "test eprover 1" $ do
            let (Query _ rule) = parseStandardQuery "~DATA_NAME(x, y) | DATA_OBJ(x) return x y"
            let i@(Insert lits cond) = parseStandardInsert "insert DATA_NAME(x, \"foo\")"
            r <- validateInsert (verifier2)  [rule] i
            r `shouldNotBe` Just True

        it "test eprover 2" $ do
            let (Query _ rule1) = parseStandardQuery "~DATA_NAME(x, y) | DATA_OBJ(x) return x y"
            let (Query _ rule2) = parseStandardQuery "~DATA_SIZE(x, y) | DATA_OBJ(x) return x y"
            let i@(Insert lits cond) = parseStandardInsert "DATA_SIZE(x, 1000) insert DATA_NAME(x, \"foo\")"
            r <- validateInsert (verifier2)  [rule1, rule2] i
            r `shouldBe` Just True
        it "test eprover 3" $ do
            let (Query _ rule1) = parseStandardQuery "~DATA_NAME(x, y) | DATA_OBJ(x) return x y"
            let (Query _ rule2) = parseStandardQuery "~DATA_SIZE(x, y) | DATA_OBJ(x) return x y"
            let i@(Insert lits cond) = parseStandardInsert "DATA_NAME(x, \"baz\") insert DATA_NAME(x, \"foo\")"
            r <- validateInsert (verifier2)  [rule1, rule2] i
            r `shouldBe` Just True
        it "test eprover 4" $ do
            let (Query _ rule1) = parseStandardQuery "~DATA_NAME(x, y) | DATA_OBJ(x) return x y"
            let (Query _ rule2) = parseStandardQuery "~DATA_SIZE(x, y) | DATA_OBJ(x) return x y"
            let i@(Insert lits cond) = parseStandardInsert "insert ~DATA_NAME(x, \"foo\")"
            r <- validateInsert (verifier2)  [rule1, rule2] i
            r `shouldBe` Just True
        it "test eprover 5" $ do
            let (Query _ rule1) = parseStandardQuery "~DATA_NAME(x, y) | DATA_OBJ(x) return x y"
            let (Query _ rule2) = parseStandardQuery "~DATA_SIZE(x, y) | DATA_OBJ(x) return x y"
            let i@(Insert lits cond) = parseStandardInsert "insert ~DATA_NAME(x, y) ~DATA_SIZE(x, 1000) "
            r <- validateInsert (verifier2)  [rule1, rule2] i
            r `shouldBe` Just True
        it "test eprover 6" $ do
            let (Query _ rule1) = parseStandardQuery "~DATA_NAME(x, y) | DATA_OBJ(x) return x y"
            let (Query _ rule2) = parseStandardQuery "~DATA_SIZE(x, y) | DATA_OBJ(x) return x y"
            let i@(Insert lits cond) = parseStandardInsert "insert ~DATA_NAME(x, y) ~DATA_OBJ(x)"
            r <- validateInsert (verifier2)  [rule1, rule2] i
            r `shouldNotBe` Just True
        it "test eprover 7" $ do
            let (Query _ rule1) = parseStandardQuery "~DATA_NAME(x, y) | DATA_OBJ(x) return x y"
            let (Query _ rule2) = parseStandardQuery "~DATA_SIZE(x, y) | DATA_OBJ(x) return x y"
            let i@(Insert lits cond) = parseStandardInsert "insert ~DATA_NAME(x, y) ~DATA_SIZE(x, 1000) ~DATA_OBJ(x)"
            r <- validateInsert (verifier2)  [rule1, rule2] i
            r `shouldNotBe` Just True

        it "test cvc4 0" $ do
            let (Query _ rule) = parseStandardQuery "~DATA_NAME(x, y) | DATA_OBJ(x) return x y"
            let i@(Insert lits cond) = parseStandardInsert "DATA_OBJ(x) insert DATA_NAME(x, \"foo\")"
            r <- validateInsert (verifier)  [rule] i
            r `shouldBe` Just True

        it "test cvc4 1" $ do
            let (Query _ rule) = parseStandardQuery "~DATA_NAME(x, y) | DATA_OBJ(x) return x y"
            let i@(Insert lits cond) = parseStandardInsert "insert DATA_NAME(x, \"foo\")"
            r <- validateInsert (verifier)  [rule] i
            r `shouldNotBe` Just True

        it "test cvc4 2" $ do
            let (Query _ rule1) = parseStandardQuery "~DATA_NAME(x, y) | DATA_OBJ(x) return x y"
            let (Query _ rule2) = parseStandardQuery "~DATA_SIZE(x, y) | DATA_OBJ(x) return x y"
            let i@(Insert lits cond) = parseStandardInsert "DATA_SIZE(x, 1000) insert DATA_NAME(x, \"foo\")"
            r <- validateInsert (verifier)  [rule1, rule2] i
            r `shouldBe` Just True
        it "test cvc4 3" $ do
            let (Query _ rule1) = parseStandardQuery "~DATA_NAME(x, y) | DATA_OBJ(x) return x y"
            let (Query _ rule2) = parseStandardQuery "~DATA_SIZE(x, y) | DATA_OBJ(x) return x y"
            let i@(Insert lits cond) = parseStandardInsert "DATA_NAME(x, \"baz\") insert DATA_NAME(x, \"foo\")"
            r <- validateInsert (verifier)  [rule1, rule2] i
            r `shouldBe` Just True
        it "test cvc4 4" $ do
            let (Query _ rule1) = parseStandardQuery "~DATA_NAME(x, y) | DATA_OBJ(x) return x y"
            let (Query _ rule2) = parseStandardQuery "~DATA_SIZE(x, y) | DATA_OBJ(x) return x y"
            let i@(Insert lits cond) = parseStandardInsert "insert ~DATA_NAME(x, \"foo\")"
            r <- validateInsert (verifier)  [rule1, rule2] i
            r `shouldBe` Just True
        it "test cvc4 5" $ do
            let (Query _ rule1) = parseStandardQuery "~DATA_NAME(x, y) | DATA_OBJ(x) return x y"
            let (Query _ rule2) = parseStandardQuery "~DATA_SIZE(x, y) | DATA_OBJ(x) return x y"
            let i@(Insert lits cond) = parseStandardInsert "insert ~DATA_NAME(x, y) ~DATA_SIZE(x, 1000) "
            r <- validateInsert (verifier)  [rule1, rule2] i
            r `shouldBe` Just True
        it "test cvc4 6" $ do
            let (Query _ rule1) = parseStandardQuery "~DATA_NAME(x, y) | DATA_OBJ(x) return x y"
            let (Query _ rule2) = parseStandardQuery "~DATA_SIZE(x, y) | DATA_OBJ(x) return x y"
            let i@(Insert lits cond) = parseStandardInsert "insert ~DATA_NAME(x, y) ~DATA_OBJ(x)"
            r <- validateInsert (verifier)  [rule1, rule2] i
            r `shouldNotBe` Just True
        it "test cvc4 7" $ do
            let (Query _ rule1) = parseStandardQuery "~DATA_NAME(x, y) | DATA_OBJ(x) return x y"
            let (Query _ rule2) = parseStandardQuery "~DATA_SIZE(x, y) | DATA_OBJ(x) return x y"
            let i@(Insert lits cond) = parseStandardInsert "insert ~DATA_NAME(x, y) ~DATA_SIZE(x, 1000) ~DATA_OBJ(x)"
            r <- validateInsert (verifier)  [rule1, rule2] i
            r `shouldNotBe` Just True

        it "test validate insert 0 E" $ do
            let qu = parseStandardInsert "insert ~DATA_NAME(x, \"foo1\") DATA_NAME(x, \"foo\")"
            val <- validate verifier2 qu
            val `shouldNotBe` Nothing
        -- it "test validate insert 1" $ do
        --    let (Query _ rule) = parseStandardQuery "~DATA_NAME(x, y) | DATA_OBJ(x) return x y"
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
            val `shouldBe` Nothing
        let at p args = Atom (Pred p (PredType ObjectPred (map (const (Key "String")) args))) args
        let atom p args = Atomic (at p args)
        let v = VarExpr . Var
        let i = IntExpr
        let s = StringExpr
        let a1 = atom "p" [v "X", v "Y"]
        let a2 = atom "q" [v "X", v "Y"]
        let a3 = atom "p" [i 1, i 2]
        let a4 = atom "q" [i 1, i 2]
















        it "atom to int" $ do
            let at1 = at "p" [s "a", s "b"]
            let (i, (n, m)) = runState (convert at1) (1 :: Int, empty)
            i `shouldBe` (1 :: Int)
            n `shouldBe` 2
            m `shouldBe` insert at1 (1 :: Int) empty


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
        it "standard rules" $ do
            let formulas = loadStandardICATRule "fof(test, axiom, ('DATA_NAME'(X, Y) => 'DATA_OBJ'(X)))."
            let f = ("test", "axiom", (standardPredMap ! "DATA_NAME") @@ [v "X", v "Y"] --> ((standardPredMap ! "DATA_OBJ") @@ [v "X"]))
            formulas `shouldBe` [f]
            
            
            
        it "queryplan1" $ do
            let db = MapDB "mapdb" "p" [(StringValue "a", StringValue "b")]
            let db2 = MapDB "mapdb2" "p" [(StringValue "c", StringValue "c")]
            let query2 = "p(x,y) return x y"
            let results = to1 [Var "x",Var "y"] (runQuery [Database db, Database db2] query2)
            results `shouldBe`  [ [StringValue "a" , StringValue "b"], [StringValue "c", StringValue "c"]]

        it "queryplan2" $ do
            let db = MapDB "mapdb" "p" [(StringValue "a", StringValue "b")]
            let db2 = EqDB "eqdb" 
            let db3 = EqDB "eqdb2"
            let query2 = "p(x,y) eq(x,\"a\") return x y"
            let results = to1 [Var "x",Var "y"] (runQuery [Database db, Database db2, Database db3] query2)
            results `shouldBe`  [ [StringValue "a" , StringValue "b"]]

        it "queryplan3" $ do
            let db = MapDB "mapdb" "p" [(StringValue "a", StringValue "b")]
            let db2 = (EqDB "eqdb"  :: EqDB Identity)
            let db3 = EqDB "eqdb2"
            let query2 = "p(x,y) p(y,z) eq(x,\"a\") return x y"
            let dbs = [Database db, Database db2, Database db3]
            case runParser progp (constructPredMap dbs) "" query2 of
                 Left _ -> error ("cannot parse query: " ++ show query2)
                 Right (Q (Query _ form), _) -> do
                    let qp = queryPlan dbs form
                    show qp `shouldBe` "(exec p(x,y) at [0] filter by (exec p(y,z) at [0] filter by exec eq(x,\"a\") at [1,2]))"
        it "queryplan insert eq" $ do
            let db = MapDB "mapdb" "p" [(StringValue "a", StringValue "b")]
            let db2 = (EqDB "eqdb"  :: EqDB Identity)
            let dbs = [Database db, Database db2]
            let query2 = "insert eq(1,1)"
            let [eqp] = getPreds db2
            let insmap = singleton eqp ([1], [1])
            case runParser progp (constructPredMap dbs) "" query2 of
                 Left _ -> error ("cannot parse query: " ++ show query2)
                 Right (I ins, _) -> do
                    let qp = insertPlan dbs insmap ins 
                    show qp `shouldBe` "exec [insert [eq(1,1)] where () at []]"
                    let qp2 = runIdentity (runExceptT (checkQueryPlan' dbs qp))
                    show qp2 `shouldBe` "Left (eq(1,1),fromList [])"
        it "queryplan insert map" $ do
            let db = StateMapDB "mapdb" "p" :: StateMapDB Identity
            let db2 = (EqDB "eqdb")
            let dbs = [Database db, Database db2]
            let query2 = "insert p(1,1)"
            let [eqp] = getPreds db
            let insmap = singleton eqp ([0], [0])
            case runParser progp (constructPredMap dbs) "" query2 of
                 Left _ -> error ("cannot parse query: " ++ show query2)
                 Right (I ins, _) -> do
                    let qp = insertPlan dbs insmap ins 
                    show qp `shouldBe` "exec [insert [p(1,1)] where () at [0]]"
                    let qp2 = runIdentity (evalStateT (runExceptT (checkQueryPlan' dbs qp)) [(StringValue "a", StringValue "b")])
                    show qp2 `shouldBe` "Right (fromList [])"
        it "queryplan insert insmap" $ do
            let db = MapDB "mapdb" "p" [(StringValue "a", StringValue "b")]:: MapDB Identity
            let db2 = (EqDB "eqdb"  :: EqDB Identity)
            let dbs = [Database db, Database db2]
            let query2 = "insert p(1,1)"
            let [eqp] = getPreds db
            let insmap = singleton eqp ([1], [1])
            case runParser progp (constructPredMap dbs) "" query2 of
                 Left _ -> error ("cannot parse query: " ++ show query2)
                 Right (I ins, _) -> do
                    let qp = insertPlan dbs insmap ins 
                    show qp `shouldBe` "exec [insert [p(1,1)] where () at []]"
                    let qp2 = runIdentity (runExceptT (checkQueryPlan' dbs qp))
                    show qp2 `shouldBe` "Left (p(1,1),fromList [])"
