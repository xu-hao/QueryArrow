module FO.Domain where

import FO.Data

import Data.Map.Strict (Map, lookup, empty, intersectionWith, unionWith, unionsWith, insert, delete)
import Prelude hiding (lookup)
import Data.Set (Set, singleton, fromList)
import Algebra.Lattice
import Algebra.SemiBoundedLattice

-- Int must be nonnegative
data DomainSize = Unbounded
                | Bounded deriving (Eq, Show)

instance Num DomainSize where
    Unbounded + _ = Unbounded
    _ + Unbounded = Unbounded
    Bounded + Bounded = Bounded
    Unbounded * _ = Unbounded
    _ * Unbounded = Unbounded
    Bounded * Bounded = Bounded
    abs a = a
    signum _ = Bounded
    negate _ = error "negate: DomainSize cannot be negated"
    fromInteger i = if i < 0 then error ("DomainSize::fromInteger:" ++ show i) else Bounded

instance Ord DomainSize where
    Unbounded <= Unbounded = True
    Bounded <= Unbounded = True
    Unbounded <= Bounded = False
    Bounded <= Bounded = True


-- dmul :: DomainSize -> DomainSize -> DomainSize
-- dmul (Infinite a) (Infinite b) = liftM2 (*)

dmaxs :: [DomainSize] -> DomainSize
dmaxs = maximum . (Bounded : )

-- estimate domain size (upper bound)
type DomainSizeMap = Map Var DomainSize

lookupDomainSize :: Var -> DomainSizeMap -> DomainSize
lookupDomainSize var map1 = case lookup var map1 of
    Nothing -> Unbounded
    Just a -> a

-- anything DomainSize value is less than or equal to Unbounded so we can use union and intersection operations on DomainSizeMaps
mmins :: [DomainSizeMap] -> DomainSizeMap
mmins = unionsWith min

mmin :: DomainSizeMap -> DomainSizeMap -> DomainSizeMap
mmin = unionWith min
mmaxs :: [DomainSizeMap] -> DomainSizeMap
mmaxs = foldl1 (intersectionWith max) -- must have at least one

type DomainSizeFunction a = Set Var -> a -> Set Var

class DeterminedVars a where
    determinedVars :: DomainSizeFunction Atom -> DomainSizeFunction a

instance DeterminedVars Atom where
    determinedVars dsp = dsp

instance DeterminedVars Formula where
    determinedVars dsp vars (FAtomic atom0) = determinedVars dsp vars atom0
    determinedVars dsp vars (FReturn vars2) = fromList vars2 /\ vars
    determinedVars _  _ (FInsert _) = bottom
    determinedVars dsp _ (FTransaction ) = bottom
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
    determinedVars _ _ (Aggregate Not _) = bottom
    determinedVars _ _ (Aggregate Exists _) = bottom
    determinedVars _ _ (Aggregate (Summarize funcs groupby) _) = (fromList (fst (unzip funcs)))
    determinedVars dsp vars (Aggregate (Limit _) form) = determinedVars dsp vars form
    determinedVars dsp vars (Aggregate (OrderByAsc _) form) = determinedVars dsp vars form
    determinedVars dsp vars (Aggregate (OrderByDesc _) form) = determinedVars dsp vars form
    determinedVars _ _ _ = bottom
