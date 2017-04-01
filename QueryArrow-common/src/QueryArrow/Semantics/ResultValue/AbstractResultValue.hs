{-# LANGUAGE TypeFamilies, MultiParamTypeClasses, FlexibleInstances, StandaloneDeriving, DeriveGeneric,
   RankNTypes, FlexibleContexts, GADTs, PatternSynonyms, ScopedTypeVariables #-}

module QueryArrow.Semantics.ResultValue.AbstractResultValue where

import Data.Binary
import QueryArrow.Data.Some
import QueryArrow.Semantics.Sendable
import QueryArrow.Semantics.ResultValue
import QueryArrow.Semantics.ResultValue.BinaryResultValue

type AbstractResultValue = Some ResultValue

deriving instance Show AbstractResultValue

instance Ord AbstractResultValue where
  compare (Some a) (Some b) = compare (toConcreteResultValue a) (toConcreteResultValue b)

instance Eq AbstractResultValue where
  Some a == Some b = toConcreteResultValue a == toConcreteResultValue b

instance Num AbstractResultValue where
  Some a + Some b = Some (toConcreteResultValue a + toConcreteResultValue b)
  Some a * Some b = Some (toConcreteResultValue a * toConcreteResultValue b)
  abs (Some a) = Some (abs (toConcreteResultValue a))
  signum (Some a) = Some (signum (toConcreteResultValue a))
  fromInteger i = Some (fromInteger i :: ConcreteResultValue)
  negate (Some a) = Some (negate (toConcreteResultValue a))

instance Fractional AbstractResultValue where
  fromRational i = Some (fromRational i :: ConcreteResultValue)
  recip (Some a) = Some (recip (toConcreteResultValue a))

instance Binary AbstractResultValue where
  put (Some a) = put a
  get = Some <$> (get :: Get BinaryResultValue)

instance Sendable AbstractResultValue where
  send h a = send h (BinaryWrapper a)
  
instance ResultValue AbstractResultValue where
  toConcreteResultValue (Some a) = toConcreteResultValue a
  castTypeOf (Some a) = castTypeOf a

instance Receivable AbstractResultValue where
  receive h = Some <$> (receive h :: IO BinaryResultValue)
