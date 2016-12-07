{-# LANGUAGE FlexibleContexts, MultiParamTypeClasses, FlexibleInstances, GADTs, RankNTypes #-}

module QueryArrow.Data.Abstract where

import QueryArrow.FO.Data

import Prelude hiding (lookup)
import Control.Monad.Reader
import Control.Applicative (liftA2)

type AbstractFormula a = a -> Formula
type AbstractPred a = a -> PredName

(.*.) :: AbstractFormula a -> AbstractFormula a -> AbstractFormula a
a .*. b = liftA2 FSequencing a b

(.+.) :: AbstractFormula a -> AbstractFormula a -> AbstractFormula a
a .+. b = liftA2 FChoice a b


formula :: a -> AbstractFormula a -> Formula
formula p r = r p

infixl 5 @@
infixl 5 @@+
infixl 5 @@-
infixl 4 .*.
infixl 3 .+.

(@@) :: AbstractPred a -> [Expr] -> AbstractFormula a
label @@ args = do
    p <- ask
    return (FAtomic (Atom ( label p) args))

(@@+) :: AbstractPred a -> [Expr] -> AbstractFormula a
label @@+ args = do
    p <- ask
    return (FInsert (Lit Pos (Atom ( label p) args)))

(@@-) :: AbstractPred a -> [Expr] -> AbstractFormula a
label @@- args = do
    p <- ask
    return (FInsert (Lit Neg (Atom ( label p) args)))

cre :: AbstractPred a -> [Expr] -> AbstractFormula a
cre label args = do
    p <- ask
    return (FInsert (Lit Pos (Atom ( label p) args)))

del :: AbstractPred a -> [Expr] -> AbstractFormula a
del label args = do
    p <- ask
    return (FInsert (Lit Neg (Atom ( label p) args)))

notE :: AbstractFormula a -> AbstractFormula a
notE a =
    Aggregate Not <$> a

existsE :: AbstractFormula a -> AbstractFormula a
existsE a =
    Aggregate Exists <$> a

var :: String -> Expr
var = VarExpr . Var

aggregate :: Aggregator -> AbstractFormula a -> AbstractFormula a
aggregate s a = Aggregate s <$>  a
