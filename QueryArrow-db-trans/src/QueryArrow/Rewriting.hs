{-# LANGUAGE RankNTypes, MultiParamTypeClasses, TypeSynonymInstances, FlexibleInstances #-}

module QueryArrow.Rewriting where

import Prelude hiding (lookup)
import QueryArrow.Syntax.Term
import QueryArrow.Syntax.Serialize
import QueryArrow.Syntax.Type
import QueryArrow.Semantics.TypeChecker
import QueryArrow.Semantics.Domain

import Data.Map.Strict (empty, singleton, union, fromList, lookup)
import Control.Monad
import Control.Monad.Trans.Reader
import Control.Monad.Trans.State
import Control.Monad.Trans.Class
import Control.Monad.Except
import Data.List (nub)
import Data.Set (Set, toAscList)
import qualified Data.Set as Set
import Algebra.SemiBoundedLattice
import Debug.Trace

type Pattern = Atom

data InsertRewritingRule = InsertRewritingRule Pattern Formula

data InsertRewritingRuleT = InsertRewritingRuleT Pattern FormulaT

instance Show InsertRewritingRule where
  show (InsertRewritingRule pat form) = serialize pat ++ " -> " ++ serialize form

instance Typecheck InsertRewritingRule InsertRewritingRuleT where
  typecheck r@(InsertRewritingRule atom0@(Atom pn args) form) =
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
              form' <- typecheck form
              (vtm, _) <- lift get
              let ovars = outputComponents pt' args
              let ub = unbounded vtm (Set.fromList (map extractVar ovars))
              unless (null ub) $ throwError ("typecheck: unbounded vars " ++ show ub ++ " rule " ++ show r ++ " bounded " ++ show vtm)
              return (InsertRewritingRuleT atom0 form')
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
    match (AppExpr a b) (AppExpr a2 b2) = do
           match a a2
           match b b2
    match _ _ = Nothing

instance Match Atom Atom where
    match (Atom p args) (Atom p2 args2) | p == p2   = foldM (\sub (e, e2) -> do
                                                            sub2 <- match (subst sub e) e2
                                                            return (union sub sub2)) empty (zip args args2)
                                        | otherwise = Nothing




rewriteAtomic1 :: Set Var -> Atom -> [InsertRewritingRuleT] -> NewEnv (Maybe FormulaT)
rewriteAtomic1 _ _ [] = return Nothing
rewriteAtomic1 ext a2 (InsertRewritingRuleT p form : rs) =
    case match p a2 of
        Nothing -> rewriteAtomic1 ext a2 rs
        Just sub -> do
            let fvp = freeVars p
                fvform = freeVars form
                fvold = toAscList ((fvform \\\ fvp) \\\ ext)
            fvnew <- new fvold
            let sub2 = fromList (zip fvold (map VarExpr fvnew))
            return (Just (subst (sub `union` sub2) form))

rewrite1 :: Set Var ->[InsertRewritingRuleT] -> [InsertRewritingRuleT] -> [InsertRewritingRuleT] -> FormulaT -> NewEnv FormulaT
rewrite1 ext  qr  ir dr form0 =
    case form0 of
        FAtomicA _ a2 -> do
            res <- rewriteAtomic1 ext a2 qr
            return (case res of
                Nothing -> form0
                Just form -> form)
        FChoiceA d disj1 disj2 ->
            FChoiceA d <$> rewrite1 ext qr  ir dr disj1 <*> rewrite1 ext qr  ir dr disj2
        FParA d disj1 disj2 ->
            FParA d <$> rewrite1 ext qr  ir dr disj1 <*> rewrite1 ext qr  ir dr disj2
        FSequencingA d conj1 conj2 ->
            FSequencingA d <$> rewrite1 ext qr  ir dr conj1 <*> rewrite1 ext qr  ir dr conj2
        FOneA _ ->
            return form0
        FZeroA _ ->
            return form0
        FInsertA d lit@(Lit s a) -> do
            res <- rewriteAtomic1 ext a (case s of
                            Pos -> ir
                            Neg -> dr)
            return (case res of
                        Nothing -> form0
                        Just form -> form)
        AggregateA d agg form ->
            AggregateA d agg <$> rewrite1 ext  qr ir dr form

rewrites    :: Int -> Set Var -> [InsertRewritingRuleT] -> [InsertRewritingRuleT] -> [InsertRewritingRuleT] -> FormulaT -> NewEnv FormulaT
rewrites n ext  rules  ir dr form | n < 0     = error "maximum number of rewrites reached"
                        | otherwise = do
                            form' <-  rewrite1 ext rules  ir dr form
                            if form == form'
                                then return form
                                else rewrites (n - 1) ext rules  ir dr form'
