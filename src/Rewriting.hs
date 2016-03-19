{-# LANGUAGE RankNTypes, MultiParamTypeClasses #-}

module Rewriting where

import FO.Data

import Control.Lens.Setter
import Data.Map.Strict (empty, singleton, union)
import Control.Monad
import Debug.Trace


type Pattern = Atom

data QueryRewritingRule = QueryRewritingRule Pattern Formula deriving Show
data InsertRewritingRule = InsertRewritingRule Pattern [Atom] Formula deriving Show

class Match a b where
    match :: a -> b -> Maybe Substitution

instance Match Expr Expr where
    match (VarExpr v) e = Just (singleton v e)
    match (IntExpr i) (IntExpr i2)  | i == i2   = Just empty
                                    | otherwise = Nothing
    match (StringExpr s) (StringExpr s2)    | s == s2   = Just empty
                                            | otherwise = Nothing
    match (PatternExpr p) (PatternExpr p2)  | p == p2   = Just empty
                                            | otherwise = Nothing
    match _ _ = Nothing

instance Match Atom Atom where
    match (Atom p args) (Atom p2 args2) | p == p2   = foldM (\sub (e, e2) -> do
                                                            sub2 <- match (subst sub e) e2
                                                            return (union sub sub2)) empty (zip args args2)
                                        | otherwise = Nothing

-- doesn't satisfy lens laws
atoms :: ASetter Formula Formula Atom Formula
atoms = sets (\subform form0 -> case form0 of
                (Atomic a2) ->
                    subform a2
                (Disjunction disjs) ->
                    (Disjunction (over (mapped . atoms) (subform) disjs))
                (Conjunction conjs) ->
                    (Conjunction (over (mapped . atoms) ( subform) conjs))
                (Exists v form) ->
                    (Exists v (over (atoms) ( subform) form))
                (Not form) ->
                    (Not (over (atoms) (subform) form))
                (Forall v form) ->
                    (Forall v (over (atoms) ( subform) form)))

matches :: Pattern -> ASetter Atom Formula Substitution Formula
matches p  = sets (\subform a -> case match p a of
                        Nothing -> Atomic a
                        Just sub -> subform sub)

rewrite :: Formula -> QueryRewritingRule -> Formula
rewrite form2 (QueryRewritingRule p form)  =
    over (atoms . matches p) (\sub -> subst sub form) form2

rewrites    :: Int -> Formula -> [QueryRewritingRule] -> Formula
rewrites n form rules   | n < 0     = error "maximum number of rewrites reached"
                        | otherwise = let form' = foldl rewrite form rules in
                                        if form == form' then form else rewrites (n - 1) form' rules

rewrites1 :: Atom -> [InsertRewritingRule] -> Maybe ([Atom], Formula)
rewrites1 _ [] = Nothing
rewrites1 a (InsertRewritingRule p as form : rs) =
    case match p a of
        Nothing -> rewrites1 a rs
        Just sub -> Just (subst sub as, subst sub form)
