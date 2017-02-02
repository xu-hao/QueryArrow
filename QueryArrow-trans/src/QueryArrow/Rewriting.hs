{-# LANGUAGE RankNTypes, MultiParamTypeClasses #-}

module QueryArrow.Rewriting where

import Prelude hiding (lookup)
import QueryArrow.FO.Data
import QueryArrow.FO.Types
import QueryArrow.FO.Domain

import Data.Map.Strict (empty, singleton, union, fromList, lookup)
import Control.Monad
import Control.Monad.Trans.Reader
import Control.Monad.Trans.State
import Control.Monad.Trans.Class
import Control.Monad.Except
import Data.List ((\\), nub)
import Data.Set (Set, toAscList)
import qualified Data.Set as Set
import Algebra.Lattice
import Algebra.SemiBoundedLattice
-- import Debug.Trace


type Pattern = Atom

data InsertRewritingRule = InsertRewritingRule Pattern Formula

instance Show InsertRewritingRule where
  show (InsertRewritingRule pat form) = serialize pat ++ " -> " ++ serialize form

instance Typecheck InsertRewritingRule where
  typecheck r@(InsertRewritingRule pat@(Atom pn args) form) =
    if all isVar args
      then if length (nub args) < length args
        then
          throwError ("typecheck: rule pattern is not linear in vars " ++ show r)
        else do
          ptm <- lift . lift $ ask
          case lookup pn ptm of
            Nothing ->
              throwError ("typecheck: cannot find predicate " ++ show pn ++ " rule " ++ show r)
            Just pt -> do
              pt'@(PredType _ pts) <- instantiatePredType pt
              unless (length args == length pts) $ throwError ("typecheck: parameter count mismatch " ++ show r)
              initTCMonad (fromList (zip (map extractVar args) pts))
              typecheck form
              (vtm, _) <- lift get
              let ovars = outputComponents pt' args
              let ub = unbounded vtm (Set.fromList (map extractVar ovars))
              unless (null ub) $ throwError ("typecheck: unbounded vars " ++ show ub ++ " rule " ++ show r ++ " bounded " ++ show vtm)
      else
        throwError ("typecheck: rule pattern are not all vars " ++ show r)


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
        (Aggregate agg form) ->
            (Aggregate agg <$> rewrite1 ext  qr ir dr form)

rewrites    :: Int -> Set Var -> [InsertRewritingRule] -> [InsertRewritingRule] -> [InsertRewritingRule] -> Formula -> NewEnv Formula
rewrites n ext  rules  ir dr form | n < 0     = error "maximum number of rewrites reached"
                        | otherwise = do
                            form' <-  rewrite1 ext rules  ir dr form
                            if form == form'
                                then return form
                                else rewrites (n - 1) ext rules  ir dr form'
