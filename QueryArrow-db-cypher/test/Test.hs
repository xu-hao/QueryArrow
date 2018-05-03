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
import QueryArrow.Cypher.ICAT
import QueryArrow.Cypher.Cypher
import qualified QueryArrow.Cypher.Cypher as Cypher
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

translateCypherQuery2 :: CypherTrans -> Set.Set Var -> Formula -> CypherQuery
translateCypherQuery2 trans vars qu = translateCypherQuery1 trans vars qu Set.empty

translateCypherQuery1 :: CypherTrans -> Set.Set Var -> Formula -> Set.Set Var -> CypherQuery
translateCypherQuery1 trans vars qu env =
  let (CypherTrans  builtin predtablemap ptm) = trans
      env2 = foldl (\map2 key@(Var w)  -> insert key (SQLParamExpr w) map2) empty env in
      fst (runNew (runStateT (translateQueryToCypher qu) (builtin, predtablemap, mempty, Set.toAscList vars, Set.toAscList env, ptm)))

testTranslateSQLInsert :: String -> String -> String -> IO ()
testTranslateSQLInsert ns qus sqls = do
  qu <- qParseStandardQuery "cat" qus
  sql <- translateQuery2 <$> (sqlStandardTrans "cat") <*> pure Set.empty <*> pure qu
  serialize ((\(_,x,_) -> x)sql) `shouldBe` sqls

main :: IO ()
main = hspec $ do
        it "test translate cypher query 0" $ do
            qu <- qParseStandardQuery "cat" "cat.DATA_NAME(x, y, z) return x y"
            cyphertrans <- cypherTrans "cat" <$> standardPreds <*> standardMappings
            let cypher = translateCypherQuery2 cyphertrans (Set.fromList [Var "x", Var "y"]) qu
            serialize cypher `shouldBe` "MATCH (var:DataObject)-[var1:resc_id]->(var0) WITH var.data_id AS x,var0.resc_id AS y RETURN x,y"
        it "test translate cypher query 1" $ do
            qu <- qParseStandardQuery "cat" "cat.DATA_NAME(x, y, z) return x y z"
            cyphertrans <- cypherTrans "cat" <$> standardPreds <*> standardMappings
            let cypher = translateCypherQuery2 cyphertrans (Set.fromList [Var "x", Var "y", Var "z"]) qu
            serialize cypher `shouldBe` "MATCH (var:DataObject)-[var1:resc_id]->(var0) WITH var.data_id AS x,var0.resc_id AS y,var.data_name AS z RETURN x,y,z"
        it "test translate cypher query 2" $ do
            qu <- qParseStandardQuery "cat" "cat.DATA_NAME(x, y, z) return z"
            cyphertrans <- cypherTrans "cat" <$> standardPreds <*> standardMappings
            let cypher = translateCypherQuery1  cyphertrans (Set.fromList [Var "z"]) qu (Set.fromList [Var "x", Var "y"])
            serialize cypher `shouldBe` "WITH {x} AS x,{y} AS y MATCH (var:DataObject)-[var1:resc_id]->(var0) WHERE (var0.resc_id = y AND var.data_id = x) WITH var.data_name AS z RETURN z"
        it "test translate cypher query 3" $ do
            qu <- qParseStandardQuery "cat" "cat.COLL_NAME(x, y) return x y"
            cyphertrans <- cypherTrans "cat" <$> standardPreds <*> standardMappings
            let cypher = translateCypherQuery2 cyphertrans (Set.fromList [Var "x", Var "y"]) qu
            serialize cypher `shouldBe` "MATCH (var:Collection) WITH var.coll_id AS x,var.coll_name AS y RETURN x,y"
        it "test translate cypher query 4" $ do
            qu <- qParseStandardQuery "cat" "cat.COLL_NAME(x, y) return y"
            cyphertrans <- cypherTrans "cat" <$> standardPreds <*> standardMappings
            let cypher = translateCypherQuery1  cyphertrans (Set.fromList [Var "y"]) qu (Set.fromList [Var "x"])
            serialize cypher `shouldBe` "WITH {x} AS x MATCH (var:Collection) WHERE var.coll_id = x WITH var.coll_name AS y RETURN y"
        it "test translate cypher query 5" $ do
            qu <- qParseStandardQuery "cat" "cat.DATA_OBJ(x, y) return x y"
            cyphertrans <- cypherTrans "cat" <$> standardPreds <*> standardMappings
            let cypher = translateCypherQuery1  cyphertrans (Set.fromList []) qu (Set.fromList [Var "x", Var "y"])
            serialize cypher `shouldBe` "WITH {x} AS x,{y} AS y MATCH (var0:DataObject)-[var1:resc_id]->(var) WHERE (var0.data_id = x AND var.resc_id = y) WITH 1 AS dummy RETURN 1"
        it "test translate cypher query 6" $ do
            qu <- qParseStandardQuery "cat" "cat.DATA_OBJ(x, y) return y"
            cyphertrans <- cypherTrans "cat" <$> standardPreds <*> standardMappings
            let cypher = translateCypherQuery1  cyphertrans (Set.fromList [Var "y"]) qu (Set.fromList [Var "x"])
            serialize cypher `shouldBe` "WITH {x} AS x MATCH (var0:DataObject)-[var1:resc_id]->(var) WHERE var0.data_id = x WITH var.resc_id AS y RETURN y"
        it "test translate cypher query 7" $ do
            qu <- qParseStandardQuery "cat" "cat.DATA_OBJ(x, y) return x y"
            cyphertrans <- cypherTrans "cat" <$> standardPreds <*> standardMappings
            let cypher = translateCypherQuery1  cyphertrans (Set.fromList [Var "x", Var "y"]) qu (Set.fromList [])
            serialize cypher `shouldBe` "MATCH (var0:DataObject)-[var1:resc_id]->(var) WITH var0.data_id AS x,var.resc_id AS y RETURN x,y"
        it "test translate cypher insert 0" $ do
            qu <- qParseStandardQuery "cat" "insert cat.DATA_NAME(x, y, z)"
            cyphertrans <- cypherTrans "cat" <$> standardPreds <*> standardMappings
            let cypher = translateCypherQuery1 cyphertrans (Set.fromList []) qu (Set.fromList [Var "x", Var "y",Var "z"])
            serialize cypher `shouldBe` "WITH {x} AS x,{y} AS y,{z} AS z MATCH (var:DataObject)-[var1:resc_id]->(var0) WHERE (var0.resc_id = y AND var.data_id = x) SET var.data_name = z WITH 1 AS dummy RETURN 1"
        it "test translate cypher insert 1" $ do
            qu <- qParseStandardQuery "cat" "insert cat.DATA_OBJ(1, 2) cat.DATA_NAME(1, 2, \"foo\") cat.DATA_SIZE(1, 2, 1000)"
            cyphertrans <- cypherTrans "cat" <$> standardPreds <*> standardMappings
            let cypher = translateCypherQuery1 cyphertrans (Set.fromList []) qu (Set.fromList [Var "x", Var "y",Var "z"])
            serialize cypher `shouldBe` "WITH {x} AS x,{y} AS y,{z} AS z MATCH (var{resc_id:2}) CREATE (var0:DataObject{data_id:1}),(var0)-[var1:resc_id]->(var) WITH 1 AS dummy MATCH (var2:DataObject{data_id:1})-[var4:resc_id]->(var3{resc_id:2}) SET var2.data_name = 'foo' WITH 1 AS dummy0 MATCH (var5:DataObject{data_id:1})-[var7:resc_id]->(var6{resc_id:2}) SET var5.data_size = 1000 WITH 1 AS dummy1 RETURN 1"
        {- it "test translate cypher insert 2" $ do
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
-}
