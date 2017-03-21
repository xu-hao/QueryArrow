{-# LANGUAGE StaticPointers, ExistentialQuantification, ScopedTypeVariables, GADTs, ConstraintKinds, FlexibleInstances, MultiParamTypeClasses, KindSignatures, UndecidableSuperClasses, FlexibleContexts, UndecidableInstances, TypeFamilies #-}

module QueryArrow.Distributed.Serializable where

import GHC.StaticPtr
import Data.Proxy
import Data.Constraint
import GHC.Fingerprint.Type
import Data.Maybe
import System.IO.Unsafe
import QueryArrow.Data.Some
import Data.Typeable
import Data.Monoid

type family PutTarget (put :: * -> *) :: *
type family GetTarget (get :: * -> *) :: *

class Puttable put a where
  sPut :: a -> put (PutTarget put)

class Gettable get a where
  sGet :: GetTarget get -> get (a, GetTarget get)

class Functional a b where
  applyFunc :: a -> b

type Pipeable put get b = (Functional (PutTarget put) b, Functional b (GetTarget get))

type Serializable put get b a = (Puttable put a, Gettable get a, Pipeable put get b)

-- instance (Puttable put a, Puttable put b, Monad put) => Puttable put (a, b) where
--   sPut (a, b) = do
--     sPut a
--     sPut b
-- instance (Gettable get a, Gettable get b, Monad get) => Gettable get (a, b) where
--   sGet = do
--     a <- sGet
--     b <- sGet
--     return (a, b)

data Package a b = Package a b

class (Serializable put get b a, Serializable put get b (StaticPtr (GetTarget get -> get (Some (DistributedSerializable c put get b), GetTarget get))), c a) => DistributedSerializable (c :: * -> Constraint) put get b a where
  getStaticPtr :: Proxy a -> StaticPtr (GetTarget get -> get (Some (DistributedSerializable c put get b), GetTarget get)) -- use proxy until type applications in pattern is available in GHC

getProxy :: a -> Proxy a
getProxy _ = Proxy

instance (Monoid (PutTarget put), Monad put) => Puttable put (Some (DistributedSerializable c put get b)) where
  sPut (Some a) = do
    d <- sPut (getStaticPtr (getProxy a) :: StaticPtr (GetTarget get -> get (Some (DistributedSerializable c put get b), GetTarget get)))
    a' <- sPut a
    return (d <> a')

instance (Monad get, Gettable get (StaticPtr (GetTarget get -> get (Some (DistributedSerializable c put get b), GetTarget get))), Monad get) => Gettable get (Some (DistributedSerializable c put get  b)) where
  sGet b0 = do
    (staticptr, b1) <- sGet b0
    deRefStaticPtr staticptr b1

serializeDistributed :: (Pipeable put get b, Monad put, Monoid (PutTarget put)) => Some (DistributedSerializable c put get b) -> put b
serializeDistributed a = applyFunc <$> sPut a

deserializeDistributed :: (Pipeable put get b, Monad get, Gettable get (StaticPtr (GetTarget get -> get (Some (DistributedSerializable c put get b), GetTarget get)))) => b -> get (Some (DistributedSerializable c put get b))
deserializeDistributed bs = do
  (a, _) <- sGet (applyFunc bs)
  return a
