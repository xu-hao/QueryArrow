module FO.Domain where

import FO.Data

import Data.Map.Strict (Map, lookup, empty, intersectionWith, unionWith, unionsWith, insert, delete)
import Prelude hiding (lookup)

-- Int must be nonnegative
data DomainSize = Unbounded
                | Bounded Int deriving (Eq, Show)

instance Num DomainSize where
    Unbounded + _ = Unbounded
    _ + Unbounded = Unbounded
    (Bounded a) + (Bounded b) = Bounded ( a + b)
    _ * (Bounded 0) = Bounded 0
    (Bounded 0) * _ = Bounded 0
    Unbounded * _ = Unbounded
    _ * Unbounded = Unbounded
    (Bounded a) * (Bounded b) = Bounded ( a * b)
    abs a = a
    signum _ = Bounded 1
    negate _ = error "negate: DomainSize cannot be negated"
    fromInteger i = if i < 0 then error ("DomainSize::fromInteger:" ++ show i) else Bounded (fromInteger i)

instance Ord DomainSize where
    Unbounded <= Unbounded = True
    (Bounded _) <= Unbounded = True
    Unbounded <= (Bounded _) = False
    (Bounded a) <= (Bounded b) = a <= b


-- dmul :: DomainSize -> DomainSize -> DomainSize
-- dmul (Infinite a) (Infinite b) = liftM2 (*)

dmaxs :: [DomainSize] -> DomainSize
dmaxs = maximum . (Bounded 0 : )

exprDomainSizeMap :: DomainSizeMap -> DomainSize -> Expr -> DomainSizeMap
exprDomainSizeMap varDomainSize maxval expr = case expr of
    VarExpr var -> insert var (min (lookupDomainSize var varDomainSize) maxval) empty
    _ -> empty

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

type DomainSizeFunction m a = a -> m DomainSizeMap

class DeterminedVars a where
    determinedVars :: Monad m => DomainSizeFunction m Atom -> a -> m DomainSizeMap

instance DeterminedVars Atom where
    determinedVars dsp = dsp

instance DeterminedVars Formula where
    determinedVars dsp  (FAtomic atom0) = determinedVars dsp  atom0
    determinedVars _  (FInsert _) = return empty
    determinedVars dsp  (FTransaction ) = return empty
    determinedVars dsp  (FSequencing form1 form2) = do
        map1 <- determinedVars dsp form1
        map2 <- determinedVars dsp form2
        return (unionWith min map1 map2)
    determinedVars dsp  (FChoice form1 form2) = do
        map1 <- determinedVars dsp form1
        map2 <- determinedVars dsp form2
        return (intersectionWith max map1 map2)
    determinedVars _  (Not _) = return empty
    determinedVars dsp  (Exists v form) = do
        dsp' <- determinedVars dsp  form
        return (delete v dsp')
    determinedVars _ _ = return empty
