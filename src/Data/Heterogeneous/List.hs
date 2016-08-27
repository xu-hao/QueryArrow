{-# LANGUAGE DataKinds, KindSignatures, TypeOperators, GADTs, TypeFamilies, RankNTypes, ConstraintKinds, ScopedTypeVariables, MultiParamTypeClasses, TypeApplications #-}
module Data.Heterogeneous.List where

import Data.Constraint
{- a simplified version of hlist -}

data HList (l :: [*]) where
    HNil :: HList '[]
    HCons :: e -> HList l -> HList (e ': l)

infixr 5 .*.
(.*.) = HCons

data HVariant (l :: [*]) where
    HLeft :: e -> HVariant (e ': l)
    HRight :: HVariant l -> HVariant (e ': l)

type family Dual (a :: *) (b :: [*]) :: [*]
type instance Dual a '[] = '[]
type instance Dual a (e ': l) = (e -> a) ': Dual a l

type family HMap (f :: * -> *) (b :: [*]) :: [*]
type instance HMap f '[] = '[]
type instance HMap f (e ': l) = (f e) ': HMap f l

type family HMapConstraint (c :: * -> Constraint) (b :: [*]) :: Constraint
type instance HMapConstraint c (e ': l) = (c e, HMapConstraint c l)
type instance HMapConstraint c '[] = ()

hApply :: HList (Dual a l) -> HVariant l -> a
hApply l v = case v of
              HLeft e ->
                  case l of
                      HCons f _ -> f e
              HRight v' ->
                  case l of
                      HCons _ l' -> hApply l' v'

{- hlMap :: forall (c :: * -> Constraint) (f :: * -> *)  (l :: [*]) . (HMapConstraint c l) => (forall a . c a => a -> f a) -> HList l -> HList (HMap f l)
hlMap g l = case l of
            HNil -> HNil
            HCons e l' -> HCons (g e) (hlMap @c @f g l')

hvMap :: forall (c :: * -> Constraint) (f :: * -> *)  (l :: [*]) . (HMapConstraint c l) => (forall a . c a => a -> f a) -> HVariant l -> HVariant (HMap f l)
hvMap g l = case l of
            HLeft e -> HLeft (g e)
            HRight v' -> HRight (hvMap @c @f g v')

hFind :: forall (c :: * -> Constraint) (l :: [*]) . (HMapConstraint c l) => (forall a . c a => a -> Bool) -> HList l -> Maybe (HVariant l)
hFind g l = case l of
            HNil -> Nothing
            HCons e l' -> if (g e)
                then Just (HLeft e)
                else HRight <$> (hFind @c g l') -}

class Transform a where
    type RangeType a
    transform :: a -> RangeType a

instance Transform Int where
    type RangeType Int = Int
    transform a = 0 - a

instance Transform Bool where
    type RangeType Bool = Bool
    transform a = not a

hl :: HList '[Int, Int, Bool]
hl = 1 .*. 2 .*. True .*. HNil

type family TransformList (b :: [*]) :: Constraint
type instance TransformList (e ': l) = (Transform e, TransformList l)
type instance TransformList '[] = ()

type family RangeTypeList (b :: [*]) :: [*]
type instance RangeTypeList (e ': l) = RangeType e ': RangeTypeList l
type instance RangeTypeList '[] = '[]

hlTransform :: forall (l :: [*]) . (TransformList l) => HList l -> HList (RangeTypeList l)
hlTransform l = case l of
            HNil -> HNil
            HCons e l' -> HCons (transform e) (hlTransform l')

hl2 = hlTransform hl
