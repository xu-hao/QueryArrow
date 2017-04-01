{-# LANGUAGE MultiParamTypeClasses #-}

-- | remove `Action l ()` instance which causes overlapping instance
module QueryArrow.Data.Monoid.Action (Action(..)) where

class Action m s where
  act :: m -> s -> s
