{-# LANGUAGE OverloadedStrings, TypeSynonymInstances, FlexibleInstances #-}
import Test.Hspec
import Control.Exception
import Control.DeepSeq
import Data.Namespace.Path
import QueryArrow.SQL.Parser
import QueryArrow.SQL.Translate
import QueryArrow.SQL.SQL
import QueryArrow.Syntax.Term
import QueryArrow.Syntax.Type
import QueryArrow.Syntax.Serialize
import QueryArrow.SQL.Tools

mapping :: [SQLMapping]
mapping = [
    SQLMapping "p1" "t1" ["c1_1","c1_2","c1_3"],
    SQLMapping "p2" "t2" ["c2_1","c2_2","c2_3"]
    ]

namespacePath :: NamespacePath String
namespacePath = NamespacePath ["db1"]

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

parseTrans :: String -> IO Formula
parseTrans = parseSQLTrans (immutable namespacePath mapping)

instance NFData (NamespacePath String) where
    rnf (NamespacePath ks) = rnf ks `seq` ()

instance NFData (ObjectPath String) where
    rnf (ObjectPath nsp k) = rnf nsp `seq` rnf k `seq` ()

instance NFData Var
instance NFData Sign
instance NFData Aggregator
instance NFData Summary
instance NFData Bind
instance NFData Lit
instance NFData CastType
instance NFData (ExprF Expr)
instance NFData Expr
instance NFData Atom
instance NFData (FormulaF () Formula)
instance NFData Formula
    
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
        it "select group by 1" $ do
            form <- parseTrans "select max(c1_1) from t1 group by c1_2"
            form `shouldBe2` Aggregate (FReturn [Var "c1_10"]) (Aggregate (Summarize [Bind (Var "c1_10") (Max (Var "c1_1"))] [Var "c1_2"]) (predName1 @@@ [var "c1_1", var "c1_2", var "c1_3"]))
        it "select group by alias 1" $ do
            form <- parseTrans "select max(c1_1) as a1 from t1 group by c1_2"
            form `shouldBe2` Aggregate (FReturn [Var "a1"]) (Aggregate (Summarize [Bind (Var "a1") (Max (Var "c1_1"))] [Var "c1_2"]) (predName1 @@@ [var "c1_1", var "c1_2", var "c1_3"]))
        it "select group by alias 2" $
            (do
                form <- parseTrans "select max(c1_1) as c1_1 from t1 group by c1_2"
                evaluate (force form)
                ) `shouldThrow` errorCall "addSelectToColAliasMap: reference to alias \"c1_1\" in select expression"
        it "select order by 1" $ do
            form <- parseTrans "select c1_1 from t1 order by c1_1"
            form `shouldBe2` Aggregate (OrderByAsc (Var "c1_1")) (Aggregate (FReturn [Var "c1_1"]) (predName1 @@@ [var "c1_1", var "c1_2", var "c1_3"]))
    
                                                