{-# LANGUAGE DataKinds, KindSignatures, TypeOperators, GADTs, TypeFamilies, RankNTypes, ConstraintKinds, ScopedTypeVariables, MultiParamTypeClasses, TypeApplications, AllowAmbiguousTypes #-}
module Data.Heterogeneous.List where

import Data.Constraint
{- a simplified version of hlist -}

data HList (l :: [*]) where
    HNil :: HList '[]
    HCons :: e -> HList l -> HList (e ': l)

data HList' f l where
    HNil' :: HList' f '[]
    HCons' :: f e -> HList' f l -> HList' f (e ': l)

toHList :: HList' f l -> HList (HMap f l)
toHList HNil' = HNil
toHList (HCons' e l) = HCons e (toHList l)

data HVariant (l :: [*]) where
    HLeft :: e -> HVariant (e ': l)
    HRight :: HVariant l -> HVariant (e ': l)

data HVariant' f (l :: [*]) where
    HLeft' :: f e -> HVariant' f (e ': l)
    HRight' :: HVariant' f l -> HVariant' f (e ': l)

toHVariant :: HVariant' f l -> HVariant (HMap f l)
toHVariant (HLeft'  e) = HLeft e
toHVariant (HRight' l) = HRight (toHVariant l)

hCoerceLL :: forall (f :: * -> *) (l :: [*]). (forall a . f a -> a) -> HList' f l -> HList l
hCoerceLL _ HNil' = HNil
hCoerceLL c (HCons' e l) = HCons (c e) (hCoerceLL c l)

hCoerceVV :: forall (f :: * -> *) (l :: [*]). (forall a . f a -> a) -> HVariant' f l -> HVariant l
hCoerceVV c (HLeft' e) = HLeft (c e)
hCoerceVV c (HRight' l) = HRight (hCoerceVV c l)

newtype Compose f g a = Compose (f (g a))

hComposeCoerceLL' :: HList' (Compose f g) l -> HList' f (HMap g l)
hComposeCoerceLL' HNil' = HNil'
hComposeCoerceLL' (HCons' (Compose e) l) = HCons' e (hComposeCoerceLL' l)

hComposeCoerceVV' :: HVariant' (Compose f g) l -> HVariant' f (HMap g l)
hComposeCoerceVV' (HLeft' (Compose a)) = HLeft' a
hComposeCoerceVV' (HRight' l) = HRight' (hComposeCoerceVV' l)


hMapCULL' :: forall (c :: * -> Constraint) (f :: * -> *) (g :: * -> *) (l :: [*]). (HMapConstraint c l) => (forall a . (c a) => f a -> g a) -> HList' f l -> HList' g l
hMapCULL' _ HNil' = HNil'
hMapCULL' t (HCons' e l) = HCons' (t e) (hMapCULL' @c @f @g t l)

hMapCUVV' :: forall (c :: * -> Constraint) (f :: * -> *) (g :: * -> *) (l :: [*]). (HMapConstraint c l) => (forall a . (c a) => f a -> g a) -> HVariant' f l -> HVariant' g l
hMapCUVV' t (HLeft' e) = HLeft' (t e)
hMapCUVV' t (HRight' l) = HRight' (hMapCUVV' @c @f @g t l)

-- deriving instance (HMapConstraint Show l) => Show (HList l) -- need undecidable instances

infixr 5 .*.
(.*.) :: e -> HList l -> HList (e ': l)
(.*.) = HCons

type family Dual (a :: *) (b :: [*]) :: [*]
type instance Dual a '[] = '[]
type instance Dual a (e ': l) = (e -> a) ': Dual a l

type family HMap (f :: * -> *) (b :: [*]) :: [*]
type instance HMap f '[] = '[]
type instance HMap f (e ': l) = (f e) ': HMap f l

type family HMapConstraint (c :: * -> Constraint) (b :: [*]) :: Constraint
type instance HMapConstraint c (e ': l) = (c e, HMapConstraint c l)
type instance HMapConstraint c '[] = ()

hApplyLV :: HList (Dual a l) -> HVariant l -> a
hApplyLV l v = case v of
              HLeft e ->
                  case l of
                      HCons f _ -> f e
              HRight v' ->
                  case l of
                      HCons _ l' -> hApplyLV l' v'

hApply2CULV :: forall (c :: * -> Constraint) (f :: * -> *) b (l :: [*]). (HMapConstraint c l) => (forall a . c a => a -> f a -> b) -> HList l -> HVariant (HMap f l) -> b
hApply2CULV t l v = case l of
                      HCons h l' ->
                          case v of
                              HLeft e -> t h e
                              HRight v' -> hApply2CULV @c @f t l' v'
                      HNil -> error "dependent type"

hApply2CULV' :: forall (c :: * -> Constraint) (f :: * -> *) (g :: * -> *) b (l :: [*]). (HMapConstraint c l) => (forall a . c a => f a -> g a -> b) -> HList' f l -> HVariant' g l -> b
hApply2CULV' t l v =
  case v of
      HLeft' e ->
          case l of
              HCons' h _ -> t h e
      HRight' v' ->
          case l of
              HCons' _ l' -> hApply2CULV' @c @f @g t l' v'


hMapULL :: forall (f :: * -> *)  (l :: [*]) . (forall a . a -> f a) -> HList l -> HList' f l
hMapULL g l = case l of
            HNil -> HNil'
            HCons e l' -> HCons' (g e) (hMapULL @f g l')

hMapCULL :: forall (c :: * -> Constraint) (f :: * -> *)  (l :: [*]) . (HMapConstraint c l) => (forall a . c a => a -> f a) -> HList l -> HList' f l
hMapCULL g l = case l of
            HNil -> HNil'
            HCons e l' -> HCons' (g e) (hMapCULL @c @f g l')

hMapACULL :: forall (c :: * -> Constraint) (f :: * -> *)  (m :: * -> *) (l :: [*]) . (HMapConstraint c l, Applicative m) => (forall a . c a => a -> m (f a)) -> HList l -> m (HList' f l)
hMapACULL g l = case l of
            HNil -> pure HNil'
            HCons e l' -> HCons' <$> g e <*> hMapACULL @c @f g l'

hMapUVV :: forall (c :: * -> Constraint) (f :: * -> *)  (l :: [*]) . (HMapConstraint c l) => (forall a . c a => a -> f a) -> HVariant l -> HVariant' f l
hMapUVV g l = case l of
            HLeft e -> HLeft' (g e)
            HRight v' -> HRight' (hMapUVV @c @f g v')

hFindULV :: forall (c :: * -> Constraint) (l :: [*]) . (HMapConstraint c l) => (forall a . c a => a -> Bool) -> HList l -> Maybe (HVariant l)
hFindULV g l = case l of
            HNil -> Nothing
            HCons e l' -> if (g e)
                then Just (HLeft e)
                else HRight <$> (hFindULV @c g l')

hApplyUL :: forall (c :: * -> Constraint) (l :: [*]) (b :: *) . (HMapConstraint c l) => (forall a . c a => a -> b) -> Int -> HList l -> Maybe b
hApplyUL g a l = case l of
            HNil -> Nothing
            HCons e l' -> if a == 0
                then Just (g e)
                else hApplyUL @c g a l'

hApplyCULV :: forall (c :: * -> Constraint) (f :: * -> *) (l :: [*]) . (HMapConstraint c l) => (forall a . c a => a -> f a) -> Int -> HList l -> Maybe (HVariant' f l)
hApplyCULV g a l = case l of
            HNil -> Nothing
            HCons e l' -> if a == 0
                then Just (HLeft' (g e))
                else HRight' <$> hApplyCULV @c @f g a l'

hApplyCULV' :: forall (c :: * -> Constraint) (f :: * -> *) (g :: * -> *) (l :: [*]) . (HMapConstraint c l) => (forall a . c a => f a -> g a) -> Int -> HList' f l -> Maybe (HVariant' g l)
hApplyCULV' h a l = case l of
            HNil' -> Nothing
            HCons' e l' -> if a == 0
                then Just (HLeft' (h e))
                else HRight' <$> hApplyCULV' @c @f @g h a l'
