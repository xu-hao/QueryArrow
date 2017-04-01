{-# LANGUAGE MultiParamTypeClasses, FlexibleInstances, RankNTypes, FlexibleContexts, UndecidableInstances #-}

module QueryArrow.Syntax.Domain where

import QueryArrow.Syntax.Data

import Data.Map.Strict (lookup)
import Prelude hiding (lookup)
import Data.Set (Set, fromList)
import Control.Monad.Except (throwError)
import Algebra.Lattice

-- -- Int must be nonnegative
-- data DomainSize = Unbounded
--                 | Bounded deriving (Eq, Show)
--
-- instance Num DomainSize where
--     Unbounded + _ = Unbounded
--     _ + Unbounded = Unbounded
--     Bounded + Bounded = Bounded
--     Unbounded * _ = Unbounded
--     _ * Unbounded = Unbounded
--     Bounded * Bounded = Bounded
--     abs a = a
--     signum _ = Bounded
--     negate _ = error "negate: DomainSize cannot be negated"
--     fromInteger i = if i < 0 then error ("DomainSize::fromInteger:" ++ show i) else Bounded
--
-- instance Ord DomainSize where
--     Unbounded <= Unbounded = True
--     Bounded <= Unbounded = True
--     Unbounded <= Bounded = False
--     Bounded <= Bounded = True
--
--
-- -- dmul :: DomainSize -> DomainSize -> DomainSize
-- -- dmul (Infinite a) (Infinite b) = liftM2 (*)
--
-- dmaxs :: [DomainSize] -> DomainSize
-- dmaxs = maximum . (Bounded : )
--
-- -- estimate domain size (upper bound)
-- type DomainSizeMap = Map Var DomainSize
--
-- lookupDomainSize :: Var -> DomainSizeMap -> DomainSize
-- lookupDomainSize var map1 = case lookup var map1 of
--     Nothing -> Unbounded
--     Just a -> a
--
-- -- anything DomainSize value is less than or equal to Unbounded so we can use union and intersection operations on DomainSizeMaps
-- mmins :: [DomainSizeMap] -> DomainSizeMap
-- mmins = unionsWith min
--
-- mmin :: DomainSizeMap -> DomainSizeMap -> DomainSizeMap
-- mmin = unionWith min
-- mmaxs :: [DomainSizeMap] -> DomainSizeMap
-- mmaxs = foldl1 (intersectionWith max) -- must have at least one

type DomainSizeFunction a = Set Var -> a -> Set Var

isVar :: Expr -> Bool
isVar (VarExpr _) = True
isVar _ = False

toDSP :: PredTypeMap -> DomainSizeFunction Atom
toDSP ptm vars (Atom p args) =
  (case lookup p ptm of
    Nothing -> error ("toDSP: cannot find predicate " ++ show p)
    Just pt -> fromList (map extractVar (filter isVar (outputComponents pt args)))) \/ vars

class DeterminedVars a f where
  {-|
    This function must return all determined vars including those in the input set
  -}
    determinedVars :: DomainSizeFunction (Atom1 f) -> DomainSizeFunction a

instance DeterminedVars (Atom1 f) f where
    determinedVars dsp vars a = dsp vars a \/ vars

instance Unannotate f (Formula0 a) => DeterminedVars (Formula1 a f) a where
    determinedVars dsp vars f1 = case unannotate f1 of
      (FAtomic0 atom0) -> determinedVars dsp vars atom0
      (Aggregate0 (FReturn vars2) form) ->
        let vars1 = determinedVars dsp vars form in
        fromList vars2 /\ vars1
      (FInsert0 _) -> vars
      (FSequencing0 form1 form2) ->
        let map1 = determinedVars dsp vars form1 in
            determinedVars dsp map1 form2
      (FChoice0 form1 form2) ->
        let map1 = determinedVars dsp vars form1
            map2 = determinedVars dsp vars form2 in
            (map1 /\ map2)
      (FPar0 form1 form2) ->
        let map1 = determinedVars dsp vars form1
            map2 = determinedVars dsp vars form2 in
            (map1 /\ map2)
      (Aggregate0 Not _) -> vars
      (Aggregate0 Distinct form) -> determinedVars dsp vars form
      (Aggregate0 Exists _) -> vars
      (Aggregate0 (Summarize funcs groupby) _) -> (fromList (fst (unzip funcs))) \/ vars
      (Aggregate0 (Limit _) form) -> determinedVars dsp vars form
      (Aggregate0 (OrderByAsc _) form) -> determinedVars dsp vars form
      (Aggregate0 (OrderByDesc _) form) -> determinedVars dsp vars form
      FOne0 -> vars
      FZero0 -> vars


toOutputVarsFunction :: PredTypeMap -> OutputVarsFunction Atom
toOutputVarsFunction ptm vars (Atom p args) =
  case lookup p ptm of
    Nothing -> error ("toOutputVarsFunction: cannot find predicate " ++ show p)
    Just pt ->
        let outputOnly = fromList (map extractVar (filter isVar (outputOnlyComponents pt args))) in
            if not (null (outputOnly /\ vars))
                then
                    throwError (outputOnly /\ vars)
                else
                    return (fromList (map extractVar (filter isVar (outputComponents pt args))) \/ vars)

type OutputVarsFunction a = Set Var -> a -> Either (Set Var) (Set Var)

class OutputVars a where
  {-|
    This function must return all determined vars including those in the input set
  -}
    outputVars :: OutputVarsFunction Atom -> OutputVarsFunction a

instance OutputVars Atom where
    outputVars dsp vars a = (\/ vars) <$> dsp vars a

instance OutputVars Formula where
    outputVars dsp vars (FAtomic atom0) = outputVars dsp vars atom0
    outputVars _ vars (FInsert _) = return vars
    outputVars dsp vars (FSequencing form1 form2) = do
        map1 <- outputVars dsp vars form1
        outputVars dsp map1 form2
    outputVars dsp vars (FChoice form1 form2) = do
        map1 <- outputVars dsp vars form1
        map2 <- outputVars dsp vars form2
        return (map1 \/ map2)
    outputVars dsp vars (FPar form1 form2) = do
        map1 <- outputVars dsp vars form1
        map2 <- outputVars dsp vars form2
        return (map1 \/ map2)
    outputVars dsp vars (Aggregate (FReturn vars2) form) =  do
        vars1 <- outputVars dsp vars form
        return (fromList vars2 /\ vars1)
    outputVars _ vars (Aggregate Not _) = return vars
    outputVars dsp vars (Aggregate Distinct form) = outputVars dsp vars form
    outputVars _ vars (Aggregate Exists _) = return vars
    outputVars _ vars (Aggregate (Summarize funcs groupby) _) = return (fromList (fst (unzip funcs)) \/ vars)
    outputVars dsp vars (Aggregate (Limit _) form) = outputVars dsp vars form
    outputVars dsp vars (Aggregate (OrderByAsc _) form) = outputVars dsp vars form
    outputVars dsp vars (Aggregate (OrderByDesc _) form) = outputVars dsp vars form
    outputVars _ vars FOne = return vars
    outputVars _ vars FZero = return vars
