{-# LANGUAGE TypeFamilies, TypeSynonymInstances, FlexibleInstances, MultiParamTypeClasses, FlexibleContexts, RankNTypes, GADTs, OverloadedStrings #-}
module QueryArrow.Cypher.BuiltIn where

import QueryArrow.FO.Data
import QueryArrow.Cypher.Cypher

import Data.Map.Strict (fromList)
import QueryArrow.ListUtils (subset)
import Control.Monad.Trans.State.Strict (get, put)
import Data.Convertible (convert)
import Data.Monoid ((<>))


cypherBuiltIn :: (String -> PredName) -> CypherBuiltIn
cypherBuiltIn lookupPred =
        CypherBuiltIn ( fromList [
            (lookupPred "le",  \ [arg1, arg2] ->
                return (cwhere (CypherCompCond "<=" arg1 arg2 Pos))),
            (lookupPred "lt",  \ [arg1, arg2] ->
                return (cwhere (CypherCompCond "<" arg1 arg2 Pos))),
            (lookupPred "eq", \  [arg1, arg2] -> do
                (a, b, repmap, rvars0, env0, ptm) <- get
                let rvars = map convert rvars0
                let env = map convert env0
                let fv2 = fv arg2
                if fv2 `subset` env
                  then
                    case arg1 of
                        CypherVarExpr v ->
                            if v `elem` env
                                then return (cwhere (CypherCompCond "=" arg1 arg2 Pos))
                                else if v `elem` rvars
                                    then do
                                        put (a, b, repmap <> cypherVarExprMap v arg2, rvars0, env0, ptm)
                                        return mempty
                                    else return mempty
                        _ -> do
                            let fv1 = fv arg1
                            if fv1 `subset` env
                                then return (cwhere (CypherCompCond "=" arg1 arg2 Pos))
                                else error "eq: first argument is not a variable and contains free variables"
                  else error "eq: second argument contains free variables"
                ),
            (lookupPred "like_regex",  \ [arg1, arg2] ->
                return (cwhere (CypherCompCond "=~" arg1 arg2 Pos))),
            (lookupPred "like",  \[arg1, arg2] ->
                return (cwhere (CypherCompCond "=~" arg1 (wildcardToRegex arg2) Pos))),
            (lookupPred "regex_replace",  \[arg1, arg2, arg3, CypherVarExpr v] -> do
                (a, b, repmap, rvars, env, ptm) <- get
                put (a,b, repmap <> cypherVarExprMap v (CypherAppExpr "replace" [arg1, arg2, arg3]), rvars, env, ptm) -- currently only do non regex replace
                return mempty),
            (lookupPred "substr",  \[arg1, arg2, arg3, CypherVarExpr v] -> do
                (a, b, repmap, rvars, env, ptm) <- get
                put (a,b, repmap <> cypherVarExprMap v (CypherAppExpr "substr" [arg1, arg2, arg3]), rvars, env, ptm)
                return mempty),
            (lookupPred "replace",  \[arg1, arg2, arg3, CypherVarExpr v] -> do
                (a, b, repmap, rvars, env, ptm) <- get
                put (a,b, repmap <> cypherVarExprMap v (CypherAppExpr "replace" [arg1, arg2, arg3]), rvars, env, ptm)
                return mempty),
            (lookupPred "concat",  \[arg1, arg2, CypherVarExpr v] -> do
                (a, b, repmap, rvars, env, ptm) <- get
                put (a,b, repmap <> cypherVarExprMap v (CypherInfixExpr "+" arg1 arg2), rvars, env, ptm)
                return mempty),
            (lookupPred "add",  \[arg1, arg2, CypherVarExpr v] -> do
                (a, b, repmap, rvars, env, ptm) <- get
                put (a,b, repmap <> cypherVarExprMap v (CypherInfixExpr "+" arg1 arg2), rvars, env, ptm)
                return mempty),
            (lookupPred "sub",  \[arg1, arg2, CypherVarExpr v] -> do
                (a, b, repmap, rvars, env, ptm) <- get
                put (a,b, repmap <> cypherVarExprMap v (CypherInfixExpr "-" arg1 arg2), rvars, env, ptm)
                return mempty),
            (lookupPred "mul",  \[arg1, arg2, CypherVarExpr v] -> do
                (a, b, repmap, rvars, env, ptm) <- get
                put (a,b, repmap <> cypherVarExprMap v (CypherInfixExpr "*" arg1 arg2), rvars, env, ptm)
                return mempty),
            (lookupPred "div",  \[arg1, arg2, CypherVarExpr v] -> do
                (a, b, repmap, rvars, env, ptm) <- get
                put (a,b, repmap <> cypherVarExprMap v (CypherInfixExpr "/" arg1 arg2), rvars, env, ptm)
                return mempty),
            (lookupPred "strlen",  \[arg1, CypherVarExpr v] -> do
                (a, b, repmap, rvars, env, ptm) <- get
                put (a,b, repmap <> cypherVarExprMap v (CypherAppExpr "length" [arg1]), rvars, env, ptm)
                return mempty),
            (lookupPred "in",  \ [arg1, arg2] ->
                return (cwhere (CypherCompCond "in" arg1 arg2 Pos))),
            (lookupPred "ge",  \ [arg1, arg2] ->
                return (cwhere (CypherCompCond ">=" arg1 arg2 Pos))),
            (lookupPred "gt",  \ [arg1, arg2] ->
                return (cwhere (CypherCompCond ">" arg1 arg2 Pos))),
            (lookupPred "ne",  \ [arg1, arg2] ->
                return (cwhere (CypherCompCond "<>" arg1 arg2 Pos))),
            (lookupPred "not_like_regex",  \ [arg1, arg2] ->
                return (cwhere (CypherCompCond "=~" arg1 arg2 Neg)))
        ])

wildcardToRegex :: CypherExpr -> CypherExpr
wildcardToRegex a = foldl (\a (x, y) -> CypherAppExpr "replace" [a, CypherStringConstExpr x, CypherStringConstExpr y]) a [("\\", "\\\\"), (".", "\\."), ("*", "\\*"), ("%", ".*"), ("_", ".")] -- incomplete
