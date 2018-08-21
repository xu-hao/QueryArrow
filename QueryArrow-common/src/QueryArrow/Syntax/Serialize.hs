{-# LANGUAGE TypeFamilies, MultiParamTypeClasses, ExistentialQuantification, FlexibleInstances, StandaloneDeriving, DeriveFunctor, UndecidableInstances, DeriveGeneric,
   RankNTypes, FlexibleContexts, GADTs, PatternSynonyms, ScopedTypeVariables #-}

module QueryArrow.Syntax.Serialize where

import Prelude hiding (lookup)
import Data.Map.Strict (Map, empty, insert, alter, lookup, fromList, foldrWithKey)
import Data.List (intercalate, union, unwords)
import Control.Monad.Trans.State.Strict (evalState,get, put, State)
import Data.Set (Set, singleton)
import qualified Data.Set as Set
import Data.Maybe
import qualified Data.Map as Map
import qualified Data.Text as T
import Data.Namespace.Path
import Data.Namespace.Namespace
import Algebra.Lattice
import Control.Monad (foldM)
import Data.Monoid ((<>))
import Control.Comonad
import Control.Comonad.Cofree
import QueryArrow.Syntax.Type
import QueryArrow.Syntax.Term
import Debug.Trace


class Serialize a where
    serialize :: a -> String
    
instance Serialize () where
    serialize () = "()"
    
instance (Serialize a) => Serialize (Atom1 a) where
    serialize (Atom name args) = predNameToString name ++ "(" ++ intercalate "," (map serialize args) ++ ")"

instance (Serialize a) => Serialize (Expr1 a) where
    serialize e1 = "[" ++ serialize (extract e1) ++ "]" ++ case unwrap e1 of
      VarExprF var0 -> serialize var0
      ConsExprF var0 -> var0
      IntExprF i -> show i
      StringExprF s -> show s
      AppExprF a b -> "(" ++ serialize a ++ " " ++ serialize b ++ ")"
      NullExprF -> "null"
      CastExprF t v -> serialize v ++ " " ++ serialize t

instance Serialize CastType where
    serialize (TypeCons ty) = "(cons " ++ ty ++ ")"
    serialize (TypeApp ty1 ty2) = "(" ++ serialize ty1 ++ " " ++ serialize ty2 ++ ")"
    serialize (TypeUniv tv ty) = "(forall " ++ tv ++ "." ++ serialize ty ++ ")"
    serialize (TypeVar v) = "(var " ++ v ++ ")"

instance Serialize Var where
    serialize (Var s) = s

instance (Serialize a) => Serialize (Lit1 a) where
    serialize (Lit thesign theatom) = case thesign of
        Pos -> serialize theatom
        Neg -> "¬" ++ serialize theatom

instance Serialize Summary where
    serialize (Max v) = "max " ++ serialize v
    serialize (Min v) = "min " ++ serialize v
    serialize Count = "count"
    serialize (Sum v) = "sum " ++ serialize v
    serialize (Average v) = "average " ++ serialize v
    serialize (CountDistinct v) = "count distinct(" ++ serialize v ++ ")"
    serialize (Random v) = "randomw " ++ serialize v

instance Serialize Bind where
    serialize (Bind var1 func1) = serialize var1 ++ " = " ++ serialize func1
    
instance (Serialize a, Serialize b) => Serialize (Formula2 a b) where
    serialize f1 = "[" ++ serialize (extract f1) ++ "]" ++ case unwrap f1 of
      FAtomicF a -> serialize a
      FInsertF lits -> "(insert " ++ serialize lits ++ ")"
      FSequencingF _ _ -> "(" ++ intercalate " ⊗ " (map serialize (getFsequencings' f1)) ++ ")"
      FChoiceF _ _ -> "(" ++ intercalate " ⊕ " (map serialize (getFchoices' f1)) ++ ")"
      FParF _ _ -> "(" ++ intercalate " ‖ " (map serialize (getFpars' f1)) ++ ")"
      FOneF -> "𝟏"
      FZeroF -> "𝟎"
      AggregateF (FReturn vars) form -> "(" ++ serialize form ++ " return " ++ unwords (map serialize vars) ++ ")"
      AggregateF Exists form -> "∃" ++ serialize form
      AggregateF Not form -> "¬" ++ serialize form
      AggregateF Distinct form -> "(distinct " ++ serialize form ++ ")"
      AggregateF (Summarize funcs groupby) form -> "(let " ++ unwords (map serialize funcs) ++ " " ++ (if null groupby then "" else "group by " ++ unwords (map serialize groupby) ++ " ") ++ serialize form ++ ")"
      AggregateF (Limit n) form -> "(limit " ++ show n ++ " " ++ serialize form ++ ")"
      AggregateF (OrderByAsc var1) form -> "(order by " ++ serialize var1 ++ " asc " ++ serialize form ++ ")"
      AggregateF (OrderByDesc var1) form -> "(order by " ++ serialize var1 ++ " desc " ++ serialize form ++ ")"

instance (Serialize a, Serialize b) => Serialize (Map a b) where
  serialize m = foldrWithKey (\ a b s -> serialize a ++ "=" ++ serialize b ++ if null s then s else "," ++ s ) "" m

instance Serialize PredName where
    serialize = predNameToString
  
instance Serialize ParamType where
    serialize (ParamType key input output isRef t) = (if key then "key " else ""  ) ++ (if input then "input " else "") ++ (if output then "output " else "") ++ (if isRef then "ref " else "") ++ serialize t
  
  