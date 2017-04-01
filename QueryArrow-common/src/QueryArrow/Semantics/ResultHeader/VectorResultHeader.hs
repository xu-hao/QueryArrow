{-# LANGUAGE TypeFamilies, MultiParamTypeClasses, FlexibleInstances,
   RankNTypes, FlexibleContexts #-}

module QueryArrow.Semantics.ResultHeader.VectorResultHeader where
import QueryArrow.Syntax.Data
import Data.Vector (Vector)
import QueryArrow.Data.Monoid.Action
import QueryArrow.Semantics.ResultHeader
import QueryArrow.Semantics.Sendable

type ResultHeader = Vector Var

data ResultHeaderTransformer = RHId | RHTrans (Vector Var)

instance Sendable ResultHeader where

instance Receivable ResultHeader where

instance Monoid ResultHeaderTransformer where
  mempty = RHId
  RHId `mappend` a = a
  a `mappend` _ = a

instance Action ResultHeaderTransformer ResultHeader where
  RHId `act` a = a
  RHTrans vec1 `act` hdr = vec1

instance IResultHeader ResultHeader where
  type HeaderTransType ResultHeader = ResultHeaderTransformer
