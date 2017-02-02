module QueryArrow.FO.Domain where

import QueryArrow.FO.Data

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

class DeterminedVars a where
  {-|
    This function must return all determined vars including those in the input set
  -}
    determinedVars :: DomainSizeFunction Atom -> DomainSizeFunction a

instance DeterminedVars Atom where
    determinedVars dsp vars a = dsp vars a \/ vars

instance DeterminedVars Formula where
    determinedVars dsp vars (FAtomic atom0) = determinedVars dsp vars atom0
    determinedVars dsp vars (Aggregate (FReturn vars2) form) =
        let vars1 = determinedVars dsp vars form in
        fromList vars2 /\ vars1
    determinedVars _ vars (FInsert _) = vars
    determinedVars dsp vars (FSequencing form1 form2) =
        let map1 = determinedVars dsp vars form1 in
            determinedVars dsp map1 form2
    determinedVars dsp vars (FChoice form1 form2) =
        let map1 = determinedVars dsp vars form1
            map2 = determinedVars dsp vars form2 in
            (map1 /\ map2)
    determinedVars dsp vars (FPar form1 form2) =
        let map1 = determinedVars dsp vars form1
            map2 = determinedVars dsp vars form2 in
            (map1 /\ map2)
    determinedVars _ vars (Aggregate Not _) = vars
    determinedVars dsp vars (Aggregate Distinct form) = determinedVars dsp vars form
    determinedVars _ vars (Aggregate Exists _) = vars
    determinedVars _ vars (Aggregate (Summarize funcs groupby) _) = (fromList (fst (unzip funcs))) \/ vars
    determinedVars dsp vars (Aggregate (Limit _) form) = determinedVars dsp vars form
    determinedVars dsp vars (Aggregate (OrderByAsc _) form) = determinedVars dsp vars form
    determinedVars dsp vars (Aggregate (OrderByDesc _) form) = determinedVars dsp vars form
    determinedVars _ vars FOne = vars
    determinedVars _ vars FZero = vars


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
