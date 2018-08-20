{-# LANGUAGE FlexibleContexts, FlexibleInstances, OverloadedStrings #-}

module Main where
import QueryArrow.QueryPlan
import QueryArrow.DB.DB
import QueryArrow.DB.ResultStream
import QueryArrow.Syntax.Term
import QueryArrow.Parser
import qualified QueryArrow.Mapping as ICAT
import qualified QueryArrow.BuiltIn as BuiltIn
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

parseStandardQuery ::  String -> IO Formula
parseStandardQuery query2 =
    return (case runParser progp () "" query2 of
            Left err -> let errmsg = "cannot parse " ++ query2 ++ show err in trace errmsg error ""
            Right [Execute qu2] -> qu2)
qParseStandardQuery ::  String -> String -> IO Formula
qParseStandardQuery ns query2 =
    return (case runParser progp () "" query2 of
            Left err -> let errmsg = "cannot parse " ++ query2 ++ show err in trace errmsg error ""
            Right [Execute qu2] -> qu2)

main :: IO ()
main = hspec $ do
        it "test parse query 0" $ do
            formula <- parseStandardQuery "DATA_NAME(x, y, z) return x y z"
            formula `shouldBe`
              Aggregate (FReturn [Var "x", Var "y", Var "z"])
                (FAtomic (Atom (ObjectPath mempty "DATA_NAME") [VarExpr (Var "x"), VarExpr (Var "y"), VarExpr (Var "z")]))

        it "test parse query 1" $ do
            formula <- parseStandardQuery "let a = count (DATA_NAME(x, y, z)) return a"
            formula `shouldBe`
              Aggregate (FReturn [Var "a"])
                (Aggregate (Summarize [(Var "a", Count)] []) (FAtomic (Atom (ObjectPath mempty "DATA_NAME") [VarExpr (Var "x"), VarExpr (Var "y"), VarExpr (Var "z")])))

        it "test parse query 1.2" $ do
            formula <- parseStandardQuery "let a = count distinct b (DATA_NAME(x, y, z)) return a"
            formula `shouldBe`
              Aggregate (FReturn [Var "a"])
                (Aggregate (Summarize [(Var "a", CountDistinct (Var "b"))] []) (FAtomic (Atom (ObjectPath mempty "DATA_NAME") [VarExpr (Var "x"), VarExpr (Var "y"), VarExpr (Var "z")])))

        it "test parse query 1.1" $ do
            formula <- parseStandardQuery "let a = count, b = max x, c = min x (DATA_NAME(x, y, z)) return a"
            formula `shouldBe`
              Aggregate (FReturn [Var "a"])
                (Aggregate (Summarize [(Var "a", Count), (Var "b", Max (Var "x")), (Var "c", Min (Var "x"))] []) (FAtomic (Atom (ObjectPath mempty "DATA_NAME") [VarExpr (Var "x"), VarExpr (Var "y"), VarExpr (Var "z")])))
