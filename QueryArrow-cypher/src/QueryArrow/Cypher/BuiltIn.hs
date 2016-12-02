{-# LANGUAGE TypeFamilies, TypeSynonymInstances, FlexibleInstances, MultiParamTypeClasses, FlexibleContexts, RankNTypes, GADTs #-}
module QueryArrow.Cypher.BuiltIn where

import QueryArrow.FO.Data
import QueryArrow.Cypher.Cypher

import Data.Map.Strict (fromList)
import QueryArrow.ListUtils (subset)
import Control.Monad.Trans.State.Strict (get, put)
import Data.Convertible (convert)
import Data.Monoid ((<>))


cypherBuiltIn :: (String -> Pred) -> CypherBuiltIn
cypherBuiltIn lookupPred =
        CypherBuiltIn ( fromList [
            (lookupPred "le",  \ [arg1, arg2] ->
                return (cwhere (CypherCompCond "<=" arg1 arg2 Pos))),
            (lookupPred "lt",  \ [arg1, arg2] ->
                return (cwhere (CypherCompCond "<" arg1 arg2 Pos))),
            (lookupPred "eq", \  [arg1, arg2] -> do
                (a, b, repmap, rvars0, env0) <- get
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
                                        put (a, b, repmap <> cypherVarExprMap v arg2, rvars0, env0)
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
            (lookupPred "ge",  \ [arg1, arg2] ->
                return (cwhere (CypherCompCond ">=" arg1 arg2 Pos))),
            (lookupPred "gt",  \ [arg1, arg2] ->
                return (cwhere (CypherCompCond ">" arg1 arg2 Pos))),
            (lookupPred "ne",  \ [arg1, arg2] ->
                return (cwhere (CypherCompCond "<>" arg1 arg2 Pos))),
            (lookupPred "not_like_regex",  \ [arg1, arg2] ->
                return (cwhere (CypherCompCond "=~" arg1 arg2 Neg)))
        ])
