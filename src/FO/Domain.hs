module FO.Domain where

import FO.Data

import Data.Map.Strict (lookup, empty, delete, intersectionWith, unionWith, unionsWith, insert, Map)
import Prelude hiding (lookup)

-- Int must be nonnegative
data DomainSize = Unbounded
                | Bounded Int deriving Show

dmin :: DomainSize -> DomainSize -> DomainSize
dmin Unbounded b = b
dmin a Unbounded = a
dmin (Bounded a) (Bounded b) = Bounded (min a b)

dmax :: DomainSize -> DomainSize -> DomainSize
dmax Unbounded _ = Unbounded
dmax _ Unbounded = Unbounded
dmax (Bounded a) (Bounded b) = Bounded (max a b)

-- dmul :: DomainSize -> DomainSize -> DomainSize
-- dmul (Infinite a) (Infinite b) = liftM2 (*)

dmaxs :: [DomainSize] -> DomainSize
dmaxs = foldl dmax (Bounded 0)

exprDomainSizeMap :: DomainSizeMap -> DomainSize -> Expr -> DomainSizeMap
exprDomainSizeMap varDomainSize maxval expr = case expr of
    VarExpr var -> insert var (dmin (lookupDomainSize var varDomainSize) maxval) empty
    _ -> empty

-- estimate domain size (upper bound)
type DomainSizeMap = Map Var DomainSize

lookupDomainSize :: Var -> DomainSizeMap -> DomainSize
lookupDomainSize var map1 = case lookup var map1 of
    Nothing -> Unbounded
    Just a -> a

-- anything DomainSize value is less than or equal to Unbounded so we can use union and intersection operations on DomainSizeMaps
mmins :: [DomainSizeMap] -> DomainSizeMap
mmins = unionsWith dmin

mmin :: DomainSizeMap -> DomainSizeMap -> DomainSizeMap
mmin = unionWith dmin
mmaxs :: [DomainSizeMap] -> DomainSizeMap
mmaxs = foldl1 (intersectionWith dmax) -- must have at least one

type DomainSizeFunction m a = Sign -> a -> m DomainSizeMap

class DeterminedVars a where
    determinedVars :: Monad m => DomainSizeFunction m Atom -> Sign -> a -> m DomainSizeMap

instance DeterminedVars Atom where
    determinedVars dsp = dsp

instance DeterminedVars Formula where
    determinedVars dsp sign (Atomic atom) = determinedVars dsp sign atom
    determinedVars dsp Pos (Conjunction formulas) = do
        maps <- mapM (determinedVars dsp Pos) formulas
        return (mmins maps)
    determinedVars dsp Neg (Conjunction formulas) = do
        maps <- mapM (determinedVars dsp Neg) formulas
        return (mmaxs maps)
    determinedVars dsp Pos (Disjunction formulas) = do
        maps <- mapM (determinedVars dsp Pos) formulas
        return (mmins maps)
    determinedVars dsp Neg (Disjunction formulas) = do
        maps <- mapM (determinedVars dsp Neg) formulas
        return (mmaxs maps)
    determinedVars dsp Pos (Not formula) =
        determinedVars dsp Neg formula
    determinedVars dsp Neg (Not formula) =
        determinedVars dsp Pos formula
    determinedVars dsp Pos (Exists var formula) = do
        map1 <- determinedVars dsp Pos formula
        return (delete var map1)
    determinedVars dsp Neg (Exists var formula) = do
        return empty -- this may be an underestimation, need look into this more
