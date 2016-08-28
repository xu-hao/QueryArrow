{-# LANGUAGE DataKinds, KindSignatures, TypeOperators, GADTs, TypeFamilies, RankNTypes, ConstraintKinds, ScopedTypeVariables, MultiParamTypeClasses, TypeApplications, AllowAmbiguousTypes #-}
module Data.Heterogeneous.List where

import Data.Constraint
import Data.Functor.Compose
import Data.Monoid (mempty, (<>))
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

data Op a b = Op (b -> a)

applyOp :: Op a b -> b -> a
applyOp (Op f) b = f b

type family HMap (f :: * -> *) (b :: [*]) :: [*]
type instance HMap f '[] = '[]
type instance HMap f (e ': l) = (f e) ': HMap f l

type family HMapConstraint (c :: * -> Constraint) (b :: [*]) :: Constraint
type instance HMapConstraint c (e ': l) = (c e, HMapConstraint c l)
type instance HMapConstraint c '[] = ()

hApplyLV :: HList' (Op a) l -> HVariant l -> a
hApplyLV l v = case v of
              HLeft e ->
                  case l of
                      HCons' f _ -> applyOp f e
              HRight v' ->
                  case l of
                      HCons' _ l' -> hApplyLV l' v'

hApply2CULV :: forall (c :: * -> Constraint) (f :: * -> *) b (l :: [*]). (HMapConstraint c l) => (forall a . c a => a -> f a -> b) -> HList l -> HVariant' f l -> b
hApply2CULV t l v = case l of
                      HCons h l' ->
                          case v of
                              HLeft' e -> t h e
                              HRight' v' -> hApply2CULV @c @f t l' v'
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

hMapCUL :: forall (c :: * -> Constraint) (b :: *)  (l :: [*]) . (HMapConstraint c l) => (forall a . c a => a -> b) -> HList l -> [b]
hMapCUL g l = case l of
            HNil -> []
            HCons e l' -> (g e) : (hMapCUL @c @b g l')

hMapACULL :: forall (c :: * -> Constraint) (f :: * -> *)  (m :: * -> *) (l :: [*]) . (HMapConstraint c l, Applicative m) => (forall a . c a => a -> m (f a)) -> HList l -> m (HList' f l)
hMapACULL g l = case l of
            HNil -> pure HNil'
            HCons e l' -> HCons' <$> g e <*> hMapACULL @c @f g l'

hMapACUL :: forall (c :: * -> Constraint) (b :: *)  (m :: * -> *) (l :: [*]) . (HMapConstraint c l, Applicative m) => (forall a . c a => a -> m b) -> HList l -> m [b]
hMapACUL g l = case l of
            HNil -> pure []
            HCons e l' -> (:) <$> g e <*> hMapACUL @c @b g l'

hMapACUL_ :: forall (c :: * -> Constraint) (m :: * -> *) (l :: [*]) . (HMapConstraint c l, Applicative m) => (forall a . c a => a -> m ()) -> HList l -> m ()
hMapACUL_ g l = case l of
            HNil -> pure mempty
            HCons e l' -> (<>) <$> g e <*> hMapACUL_ @c g l'

hMapCUVV :: forall (c :: * -> Constraint) (f :: * -> *)  (l :: [*]) . (HMapConstraint c l) => (forall a . c a => a -> f a) -> HVariant l -> HVariant' f l
hMapCUVV g l = case l of
            HLeft e -> HLeft' (g e)
            HRight v' -> HRight' (hMapCUVV @c @f g v')

hFindCULV :: forall (c :: * -> Constraint) (l :: [*]) . (HMapConstraint c l) => (forall a . c a => a -> Bool) -> HList l -> Maybe (HVariant l)
hFindCULV g l = case l of
            HNil -> Nothing
            HCons e l' -> if (g e)
                then Just (HLeft e)
                else HRight <$> (hFindCULV @c g l')

hApplyCUL :: forall (c :: * -> Constraint) (l :: [*]) (b :: *) . (HMapConstraint c l) => (forall a . c a => a -> b) -> Int -> HList l -> Maybe b
hApplyCUL g a l = case l of
            HNil -> Nothing
            HCons e l' -> if a == 0
                then Just (g e)
                else hApplyCUL @c g a l'

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
