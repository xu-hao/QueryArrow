{-# LANGUAGE DataKinds, KindSignatures, TypeOperators, GADTs, TypeFamilies, RankNTypes, ConstraintKinds, ScopedTypeVariables, MultiParamTypeClasses, TypeApplications, AllowAmbiguousTypes, TypeFamilyDependencies #-}
module QueryArrow.Data.Heterogeneous.List where

import Data.Constraint
import Data.Functor.Compose
import Data.Monoid (mempty, (<>))
import Data.Functor.Identity
import Data.Coerce

{- a simplified version of hlist -}

{-
type family AppF (f :: Maybe (* -> *)) (a :: *) = r :: * | r -> a f where
    AppC 'Nothing a = a
    AppC ('Just f) a = f a

is injective but GHC rejects it

-}
type family HMapF (f :: * -> *) (b :: [*]) :: [*] where
    HMapF f '[] = '[]
    HMapF f (e ': l) = f e ': HMapF f l

type family HMapC (c ::  (* -> Constraint)) (b :: [*]) :: Constraint where
    HMapC c (e ': l) = (c e, HMapC c l)
    HMapC c '[] = ()

data HList (l :: [*]) where
    HNil :: HList '[]
    HCons :: e -> HList l -> HList (e ': l)

data HList' f l where
    HNil' :: HList' f '[]
    HCons' :: f e -> HList' f l -> HList' f (e ': l)

toHList :: HList' f l -> HList (HMapF f l)
toHList HNil' = HNil
toHList (HCons' e l') = HCons e (toHList l')

coerceHList' :: HList' Identity l -> HList l
coerceHList' HNil' = HNil
coerceHList' (HCons' e l) = HCons (runIdentity e) (coerceHList' l)

coerceHList :: HList l -> HList' Identity l
coerceHList HNil = HNil'
coerceHList (HCons e l) = HCons' (Identity e) (coerceHList l)

data HVariant (l :: [*]) where
    HLeft :: e -> HVariant (e ': l)
    HRight :: HVariant l -> HVariant (e ': l)

data HVariant' f (l :: [*]) where
    HLeft' :: f e -> HVariant' f (e ': l)
    HRight' :: HVariant' f l -> HVariant' f (e ': l)

toHVariant :: HVariant' f l -> HVariant (HMapF f l)
toHVariant (HLeft'  e) = HLeft e
toHVariant (HRight' l) = HRight (toHVariant l)

coerceHVariant' :: HVariant' Identity l -> HVariant l
coerceHVariant' (HLeft'  e) = HLeft (runIdentity e)
coerceHVariant' (HRight' l) = HRight (coerceHVariant' l)

coerceHVariant :: HVariant l -> HVariant' Identity l
coerceHVariant (HLeft  e) = HLeft' (coerce e)
coerceHVariant (HRight l) = HRight' (coerceHVariant l)

toIntegerV :: HVariant l -> Int
toIntegerV (HLeft  _) = 0
toIntegerV (HRight l) = toIntegerV l + 1

hComposeCoerceLL :: HList' (Compose f g) l -> HList' f (HMapF g l)
hComposeCoerceLL HNil' = HNil'
hComposeCoerceLL (HCons' (Compose e) l) = HCons' e (hComposeCoerceLL l)

hComposeCoerceVV :: HVariant' (Compose f g) l -> HVariant' f (HMapF g l)
hComposeCoerceVV (HLeft' (Compose a)) = HLeft' a
hComposeCoerceVV (HRight' l) = HRight' (hComposeCoerceVV l)


-- deriving instance (HMapConstraint Show l) => Show (HList l) -- need undecidable instances

infixr 5 .*.
(.*.) :: e -> HList l -> HList (e ': l)
(.*.) = HCons

newtype Op a b = Op { applyOp :: b -> a }

{- hApplyLV :: HList' (Op a) l -> HVariant l -> a
hApplyLV l v = hZipWithCULV -}

-- zipWith

hZipWithACLV :: forall (c ::  (* -> Constraint)) (f ::  (* -> *)) (g ::  (* -> *)) b (m :: * -> *) (l :: [*]). (HMapC c l, Applicative m) => (forall a . c a => f a -> g a -> m b) -> HList' f l -> HVariant' g l -> m b
hZipWithACLV t l v =
  case v of
      HLeft' e ->
          case l of
              HCons' h _ -> t h e
      HRight' v' ->
          case l of
              HCons' _ l' -> hZipWithACLV @c @f @g t l' v'

-- mapA

hMapACLL :: forall (c ::  (* -> Constraint)) (f ::  (* -> *)) (g ::  (* -> *)) (m ::  (* -> *)) (l :: [*]) . (HMapC c l, Applicative m) => (forall a . c a => f a -> m (g a)) -> HList' f l -> m (HList' g l)
hMapACLL g l = case l of
            HNil' -> pure HNil'
            HCons' e l' -> HCons' <$> g e <*> hMapACLL @c @f @g g l'

hMapACULL :: forall (c ::  (* -> Constraint)) (f ::  (* -> *)) (m ::  (* -> *)) (l :: [*]) . (HMapC c l, Applicative m) => (forall a . c a => a -> m (f a)) -> HList l -> m (HList' f l)
hMapACULL g l = hMapACLL @c @Identity @f @m @l (g . runIdentity) (coerceHList l)

hMapACVV :: forall (c ::  (* -> Constraint)) (f ::  (* -> *)) (g ::  (* -> *)) (m ::  (* -> *)) (l :: [*]) . (HMapC c l, Applicative m) => (forall a . c a => f a -> m (g a)) -> HVariant' f l -> m (HVariant' g l)
hMapACVV g l = case l of
            HLeft' e -> HLeft' <$> g e
            HRight' v' -> HRight' <$> hMapACVV @c @f @g g v'
            
-- foldMapA

hFoldMapACL :: forall (c ::  (* -> Constraint)) (f ::  (* -> *)) (b :: *)  (m ::  (* -> *)) (l :: [*]) . (HMapC c l, Applicative m, Monoid b) => (forall a . c a =>  f a ->  m b) -> HList' f l ->  m b
hFoldMapACL g l = case l of
            HNil' -> pure mempty
            HCons' e l' -> (<>) <$> g e <*> hFoldMapACL @c @f @b g l'

hFoldMapCUL :: forall (c ::  (* -> Constraint)) (b :: *)  (l :: [*]) . (HMapC c l, Monoid b) => (forall a . c a =>  a ->  b) -> HList l ->  b
hFoldMapCUL g l = runIdentity (hFoldMapACL @c @Identity @b @Identity @l (Identity . g . runIdentity) (coerceHList l))

hFoldMapACUL :: forall (c ::  (* -> Constraint)) (b :: *) (m :: * -> *) (l :: [*]) . (HMapC c l, Applicative m, Monoid b) => (forall a . c a =>  a ->  m b) -> HList l ->  m b
hFoldMapACUL g l = hFoldMapACL @c @Identity @b @m @l (g . runIdentity) (coerceHList l)

hFoldMapACV :: forall (c ::  (* -> Constraint)) (f ::  (* -> *)) (b :: *)  (m ::  (* -> *)) (l :: [*]) . (HMapC c l, Applicative m, Monoid b) => (forall a . c a =>  f a ->  m b) -> HVariant' f l ->  m b
hFoldMapACV g v = case v of
            HLeft' e -> g e
            HRight' v' -> hFoldMapACV @c @f @b g v'
            
-- find

hFindCLV :: forall (c ::  (* -> Constraint)) (f ::  (* -> *)) (l :: [*]) . (HMapC c l) => (forall a . c a =>  f a -> Bool) -> HList' f l -> Maybe (HVariant' f l)
hFindCLV g l = case l of
            HNil' -> Nothing
            HCons' e l' -> if (g e)
                then Just (HLeft' e)
                else HRight' <$> (hFindCLV @c @f g l')

hFindCULV :: forall (c ::  (* -> Constraint)) (l :: [*]) . (HMapC c l) => (forall a . c a =>  a -> Bool) -> HList l -> Maybe (HVariant l)
hFindCULV g l = coerceHVariant' <$> hFindCLV @c @Identity @l (g . runIdentity) (coerceHList l)
-- apply

hApplyACL :: forall (c ::  (* -> Constraint)) (f ::  (* -> *)) (b :: *) (m :: (* -> *)) (l :: [*]). (HMapC c l, Applicative m) => (forall a . c a =>  f a -> m b) -> Int -> HList' f l -> m (Maybe b)
hApplyACL g a l = case l of
            HNil' -> pure Nothing
            HCons' e l' -> if a == 0
                then Just <$> g e
                else hApplyACL @c @f @b g (a-1) l'

hApplyCUL :: forall (c ::  (* -> Constraint)) (b :: *)  (l :: [*]). (HMapC c l) => (forall a . c a =>  a -> b) -> Int -> HList l -> Maybe b
hApplyCUL g a l = runIdentity (hApplyACL @c @Identity @b @Identity (Identity . g . runIdentity) a (coerceHList l))

hApplyACUL :: forall (c ::  (* -> Constraint)) (b :: *) (m :: * -> *) (l :: [*]). (HMapC c l, Applicative m) => (forall a . c a =>  a -> m b) -> Int -> HList l -> m (Maybe b)
hApplyACUL g a l = hApplyACL @c @Identity @b @m (g . runIdentity) a (coerceHList l)

hApplyCV :: forall (c ::  (* -> Constraint)) (f ::  (* -> *)) (b :: *)  (l :: [*]). (HMapC c l) => (forall a . c a =>  f a -> b) -> HVariant' f l -> b
hApplyCV g l = case l of
            HLeft' e -> g e
            HRight' l' -> hApplyCV @c @f @b g l'

hApplyACLV :: forall (c ::  (* -> Constraint)) (f ::  (* -> *)) (g ::  (* -> *)) (m ::  (* -> *)) (l :: [*]) . (HMapC c l, Applicative m) => (forall a . c a =>  f a ->  m ( g a)) -> Int -> HList' f  l ->  m (Maybe (HVariant' g l))
hApplyACLV g a l = case l of
    HNil' -> pure Nothing
    HCons' e l' -> if a == 0
        then Just <$> (HLeft' <$> (g e))
        else getCompose (HRight' <$> Compose (hApplyACLV @c @f @g g (a-1) l'))

hApplyACULV :: forall (c ::  (* -> Constraint)) (g ::  (* -> *)) (m :: * -> *) (l :: [*]). (HMapC c l, Applicative m) => (forall a . c a =>  a -> m (g a)) -> Int -> HList l -> m (Maybe (HVariant' g l))
hApplyACULV g a l = hApplyACLV @c @Identity @g @m (g . runIdentity) a (coerceHList l)
        