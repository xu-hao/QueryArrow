{-# LANGUAGE OverloadedStrings, FlexibleContexts #-}
import Test.Hspec
import QueryArrow.SQL.Parser
import QueryArrow.SQL.Translate
import QueryArrow.SQL.SQL
import Text.Parsec
import Data.Namespace.Path
import QueryArrow.Syntax.Term
import QueryArrow.Syntax.Serialize

mapping :: [SQLMapping]
mapping = [
    SQLMapping "p1" "t1" ["c1_1","c1_2","c1_3"],
    SQLMapping "p2" "t2" ["c2_1","c2_2","c2_3"]
    ]

namespacePath :: NamespacePath String
namespacePath = NamespacePath ["db1"]

immutable :: Immutable
immutable = Immutable 
    (createSQLTableIndex namespacePath mapping)
    (createSQLPredIndex namespacePath mapping)
    (createSQLColIndex mapping)
    (createSQLSQLOperToPredIndex namespacePath)

predName1 :: PredName
predName1 = ObjectPath namespacePath "p1"

predName2 :: PredName
predName2 = ObjectPath namespacePath "p2"

predNameEq :: PredName
predNameEq = ObjectPath namespacePath "eq"

shouldBe2 :: (Monoid b, Serialize a, Serialize b, Show a, Show b, Eq a, Eq b) => Formula2 a b -> Formula2 a b -> IO ()
shouldBe2 a b = do
    let anorm = normalize a
        bnorm = normalize b
    serialize anorm `shouldBe` serialize bnorm
    anorm `shouldBe` bnorm

parseSQL :: String -> IO SQL
parseSQL s = case parse sqlP "" s of
    Left err -> fail (show err)
    Right sql -> return sql

parseTrans :: String -> IO Formula
parseTrans s = do
    sql <- parseSQL s
    return (runTrans (translateSQLToQuery sql) immutable)

    
main :: IO ()
main = hspec $ do
    describe "SQL" $ do
        it "select 1" $ do
            form <- parseTrans "select c1_1 from t1"
            form `shouldBe2` Aggregate (FReturn [Var "c1_1"]) (predName1 @@@ [var "c1_1", var "c1_2", var "c1_3"])
        it "select where 1" $ do
            form <- parseTrans "select c1_1 from t1 where c1_1 = 1"
            form `shouldBe2` Aggregate (FReturn [Var "c1_1"]) (predName1 @@@ [var "c1_1", var "c1_2", var "c1_3"] .*. predNameEq @@@ [var "c1_1", IntExpr 1])
        it "select where 2" $ do
            form <- parseTrans "select c1_1 from t1 where c1_1 = 's'"
            form `shouldBe2` Aggregate (FReturn [Var "c1_1"]) (predName1 @@@ [var "c1_1", var "c1_2", var "c1_3"] .*. predNameEq @@@ [var "c1_1", StringExpr "s"])
        it "select where 3" $ do
            form <- parseTrans "select c1_1 from t1 where c1_2 = 1"
            form `shouldBe2` Aggregate (FReturn [Var "c1_1"]) (predName1 @@@ [var "c1_1", var "c1_2", var "c1_3"] .*. predNameEq @@@ [var "c1_2", IntExpr 1])
        it "select where 4" $ do
            form <- parseTrans "select c1_1 from t1 where c1_2 = 's'"
            form `shouldBe2` Aggregate (FReturn [Var "c1_1"]) (predName1 @@@ [var "c1_1", var "c1_2", var "c1_3"] .*. predNameEq @@@ [var "c1_2", StringExpr "s"])
        it "select where 5" $ do
            form <- parseTrans "select c1_1 from t1 where c1_1 = 1 and c1_2 = 's'"
            form `shouldBe2` Aggregate (FReturn [Var "c1_1"]) (predName1 @@@ [var "c1_1", var "c1_2", var "c1_3"] .*. predNameEq @@@ [var "c1_1", IntExpr 1] .*. predNameEq @@@ [var "c1_2", StringExpr "s"])
        it "select where 6" $ do
            form <- parseTrans "select c1_1 from t1 where c1_2 = 1 and c1_3 = 's'"
            form `shouldBe2` Aggregate (FReturn [Var "c1_1"]) (predName1 @@@ [var "c1_1", var "c1_2", var "c1_3"] .*. predNameEq @@@ [var "c1_2", IntExpr 1] .*. predNameEq @@@ [var "c1_3", StringExpr "s"])
        it "select multitable 1" $ do
            form <- parseTrans "select c1_1, c2_1 from t1, t2"
            form `shouldBe2` Aggregate (FReturn [Var "c1_1", Var "c2_1"]) (predName1 @@@ [var "c1_1", var "c1_2", var "c1_3"] .*. predName2 @@@ [var "c2_1", var "c2_2", var "c2_3"])
        it "select multitable where 1" $ do
            form <- parseTrans "select c1_1, c2_1 from t1, t2 where c1_1 = c2_1"
            form `shouldBe2` Aggregate (FReturn [Var "c1_1", Var "c2_1"]) (predName1 @@@ [var "c1_1", var "c1_2", var "c1_3"] .*. predName2 @@@ [var "c2_1", var "c2_2", var "c2_3"] .*. predNameEq @@@ [var "c1_1", var "c2_1"])
        it "select multitable where 1" $ do
            form <- parseTrans "select c1_1, c2_1 from t1, t2 where c1_2 = c2_2"
            form `shouldBe2` Aggregate (FReturn [Var "c1_1", Var "c2_1"]) (predName1 @@@ [var "c1_1", var "c1_2", var "c1_3"] .*. predName2 @@@ [var "c2_1", var "c2_2", var "c2_3"] .*. predNameEq @@@ [var "c1_2", var "c2_2"])
                                                