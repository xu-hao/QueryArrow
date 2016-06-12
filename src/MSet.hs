module MSet (MSet (..), munion, mcomplement, mintersect, mdiff, rmintersect, lmintersect) where

import Data.List (union, intersect, (\\))
data MSet a = Include [a] | Exclude [a] deriving Show

munion :: Eq a => MSet a -> MSet a -> MSet a
munion (Include vars) (Include vars2) = Include (vars `union` vars2)
munion (Include vars) (Exclude vars2) = Exclude (vars2 \\ vars)
munion (Exclude vars) (Include vars2) = Exclude (vars \\ vars2)
munion (Exclude vars) (Exclude vars2) = Exclude (vars2 `intersect` vars)

mcomplement :: Eq a => MSet a -> MSet a
mcomplement (Include vars) = Exclude vars
mcomplement (Exclude vars) = Include vars

mintersect :: Eq a => MSet a -> MSet a -> MSet a
mintersect (Include vars) (Include vars2) = Include (vars `intersect` vars2)
mintersect (Exclude vars) (Include vars2) = Include (vars2 \\ vars)
mintersect (Include vars) (Exclude vars2) = Include (vars \\ vars2)
mintersect (Exclude vars) (Exclude vars2) = Exclude (vars `union` vars2)

mdiff :: Eq a => MSet a -> MSet a -> MSet a
mdiff s1 s2 = s1 `mintersect` (mcomplement s2)

rmintersect :: Eq a => [a] -> MSet a -> [a]
rmintersect vars (Include vars2) = vars `intersect` vars2
rmintersect vars (Exclude vars2) = vars \\ vars2


lmintersect :: Eq a => MSet a -> [a] -> [a]
lmintersect (Include vars2) vars = vars `intersect` vars2
lmintersect (Exclude vars2) vars = vars \\ vars2
