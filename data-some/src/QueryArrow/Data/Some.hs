{-# LANGUAGE StaticPointers, ExistentialQuantification, ScopedTypeVariables, KindSignatures, GADTs, ConstraintKinds, FlexibleInstances #-}

module QueryArrow.Data.Some where

import GHC.StaticPtr
import Data.Proxy
import Data.Constraint
import GHC.Fingerprint.Type
import Data.Maybe
import System.IO.Unsafe

-- from http://cs.brynmawr.edu/~rae/papers/2016/thesis/eisenberg-thesis.pdf
data Some :: (* -> Constraint) -> * where
  Some :: c a => a -> Some c
