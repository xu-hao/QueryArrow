{-# LANGUAGE RankNTypes, MultiParamTypeClasses #-}

module Rewriting where

import FO.Data

import Data.Map.Strict (empty, singleton, union, fromList)
import Control.Monad
import Data.List ((\\))
-- import Debug.Trace


type Pattern = Atom

data QueryRewritingRule = QueryRewritingRule Pattern PureFormula deriving Show
data InsertRewritingRule = InsertRewritingRule Pattern Formula deriving Show

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




rewriteAtomic1' :: [Var] -> Atom -> [QueryRewritingRule] -> NewEnv (Maybe PureFormula)
rewriteAtomic1' _ _ [] = return Nothing
rewriteAtomic1' ext a2 ((QueryRewritingRule p form) : rs) =
    case match p a2 of
        Nothing -> rewriteAtomic1' ext a2 rs
        Just sub -> do
            let fvp = freeVars p
                fvform = freeVars form
                fvold = (fvform \\ fvp) \\ ext
            fvnew <- new fvold
            let sub2 = fromList (zip fvold (map VarExpr fvnew))
            return (Just (subst (sub `union` sub2) form))

rewriteAtomic1 :: [Var] -> Atom -> [InsertRewritingRule] -> NewEnv (Maybe Formula)
rewriteAtomic1 _ _ [] = return Nothing
rewriteAtomic1 ext a2 ((InsertRewritingRule p form) : rs) =
    case match p a2 of
        Nothing -> rewriteAtomic1 ext a2 rs
        Just sub -> do
            let fvp = freeVars p
                fvform = freeVars form
                fvold = (fvform \\ fvp) \\ ext
            fvnew <- new fvold
            let sub2 = fromList (zip fvold (map VarExpr fvnew))
            return (Just (subst (sub `union` sub2) form))

rewrite1 :: [Var] -> [QueryRewritingRule] -> [InsertRewritingRule] -> [InsertRewritingRule] -> [InsertRewritingRule] -> Formula -> NewEnv Formula
rewrite1 ext  qr qr2 ir dr form0 =
    case form0 of
        (FAtomic a2) -> do
            res <- rewriteAtomic1 ext a2 qr2
            return (case res of
                Nothing -> form0
                Just form -> form)
        (FChoice disj1 disj2) ->
            (FChoice <$> rewrite1 ext qr qr2 ir dr disj1 <*> rewrite1 ext qr qr2 ir dr disj2)
        (FSequencing conj1 conj2) ->
            (FSequencing <$> rewrite1 ext qr qr2 ir dr conj1 <*> rewrite1 ext qr qr2 ir dr conj2)
        (FOne) ->
            return FOne
        (FZero) ->
            return FZero
        (FInsert lit@(Lit s a)) -> do
            res <- rewriteAtomic1 ext a (case s of
                            Pos -> ir
                            Neg -> dr)
            return (case res of
                        Nothing -> FInsert lit
                        Just form -> form)

        (FClassical form) ->
            (FClassical <$> rewrite1' ext  qr  form)
        (FTransaction form) ->
            FTransaction <$> rewrite1 ext qr qr2 ir dr form

rewrite1' :: [Var] -> [QueryRewritingRule] -> PureFormula -> NewEnv PureFormula
rewrite1' ext  qr form0 =
    case form0 of
        (Atomic a2) -> do
            res <- rewriteAtomic1' ext a2 qr
            return (case res of
                Nothing -> form0
                Just form -> form)
        (Disjunction disj1 disj2) ->
            (Disjunction <$> rewrite1' ext qr disj1 <*> rewrite1' ext qr disj2)
        (Conjunction conj1 conj2) ->
            (Conjunction <$> rewrite1' ext qr conj1 <*> rewrite1' ext qr conj2)
        CTrue -> return CTrue
        CFalse -> return CFalse
        (Exists v form) ->
            (Exists v <$> rewrite1' ext  qr form)
        (Not form) ->
            (Not <$> rewrite1' ext  qr form)
        (Forall v form) ->
            (Forall v <$> rewrite1' ext  qr form)

rewrites    :: Int -> [Var] -> [QueryRewritingRule] -> [InsertRewritingRule] -> [InsertRewritingRule] -> [InsertRewritingRule] -> Formula -> NewEnv Formula
rewrites n ext  rules rules2 ir dr form | n < 0     = error "maximum number of rewrites reached"
                        | otherwise = do
                            form' <-  rewrite1 ext rules rules2 ir dr form
                            if form == form'
                                then return form
                                else rewrites (n - 1) ext rules rules2 ir dr form'
