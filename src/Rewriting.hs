{-# LANGUAGE RankNTypes, MultiParamTypeClasses #-}

module Rewriting where

import FO.Data

import Data.Map.Strict (empty, singleton, union, fromList)
import Control.Monad
import Data.List ((\\))
import Data.Set (Set, toAscList)
import Algebra.Lattice
import Algebra.SemiBoundedLattice
-- import Debug.Trace


type Pattern = Atom

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




rewriteAtomic1 :: Set Var -> Atom -> [InsertRewritingRule] -> NewEnv (Maybe Formula)
rewriteAtomic1 _ _ [] = return Nothing
rewriteAtomic1 ext a2 ((InsertRewritingRule p form) : rs) =
    case match p a2 of
        Nothing -> rewriteAtomic1 ext a2 rs
        Just sub -> do
            let fvp = freeVars p
                fvform = freeVars form
                fvold = toAscList ((fvform \\\ fvp) \\\ ext)
            fvnew <- new fvold
            let sub2 = fromList (zip fvold (map VarExpr fvnew))
            return (Just (subst (sub `union` sub2) form))

rewrite1 :: Set Var ->[InsertRewritingRule] -> [InsertRewritingRule] -> [InsertRewritingRule] -> Formula -> NewEnv Formula
rewrite1 ext  qr  ir dr form0 =
    case form0 of
        (FReturn _) -> return form0
        (FAtomic a2) -> do
            res <- rewriteAtomic1 ext a2 qr
            return (case res of
                Nothing -> form0
                Just form -> form)
        (FChoice disj1 disj2) ->
            (FChoice <$> rewrite1 ext qr  ir dr disj1 <*> rewrite1 ext qr  ir dr disj2)
        (FPar disj1 disj2) ->
            (FPar <$> rewrite1 ext qr  ir dr disj1 <*> rewrite1 ext qr  ir dr disj2)
        (FSequencing conj1 conj2) ->
            (FSequencing <$> rewrite1 ext qr  ir dr conj1 <*> rewrite1 ext qr  ir dr conj2)
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
        (FTransaction ) ->
            return FTransaction
        (Exists v form) ->
            (Exists v <$> rewrite1 ext  qr ir dr form)
        (Not form) ->
            (Not <$> rewrite1 ext  qr ir dr form)

rewrites    :: Int -> Set Var -> [InsertRewritingRule] -> [InsertRewritingRule] -> [InsertRewritingRule] -> Formula -> NewEnv Formula
rewrites n ext  rules  ir dr form | n < 0     = error "maximum number of rewrites reached"
                        | otherwise = do
                            form' <-  rewrite1 ext rules  ir dr form
                            if form == form'
                                then return form
                                else rewrites (n - 1) ext rules  ir dr form'
