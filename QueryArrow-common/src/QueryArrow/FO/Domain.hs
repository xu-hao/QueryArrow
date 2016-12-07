module QueryArrow.FO.Domain where

import QueryArrow.FO.Data

import Data.Map.Strict (Map, lookup, empty, intersectionWith, unionWith, unionsWith, insert, delete)
import Prelude hiding (lookup)
import Data.Set (Set, singleton, fromList)
import Data.Maybe (fromMaybe)
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

isVar :: Expr -> Bool
isVar (VarExpr _) = True
isVar _ = False

toDSP :: Map PredName [Int] -> DomainSizeFunction Atom
toDSP pm vars a@(Atom p args) =
  (case lookup p pm of
    Nothing -> freeVars a
    Just indexes -> fromList (map extractVar (filter isVar (map (args !!) indexes)))) \/ vars

class DeterminedVars a where
  {-|
    This function must return all determined vars including those in the input set
  -}
    determinedVars :: DomainSizeFunction Atom -> DomainSizeFunction a

instance DeterminedVars Atom where
    determinedVars dsp vars a = dsp vars a \/ vars

instance DeterminedVars Formula where
    determinedVars dsp vars (FAtomic atom0) = determinedVars dsp vars atom0
    determinedVars _ vars (FReturn vars2) = fromList vars2 /\ vars
    determinedVars _ vars (FInsert _) = vars
    determinedVars _ vars (FTransaction ) = vars
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
