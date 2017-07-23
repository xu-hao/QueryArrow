{-# LANGUAGE TypeFamilies, MultiParamTypeClasses, FlexibleInstances, ConstraintKinds,
   RankNTypes, FlexibleContexts #-}

module QueryArrow.Semantics.ResultHeader where
import QueryArrow.Data.Monoid.Action
import QueryArrow.Semantics.Sendable
import QueryArrow.Syntax.Data

class (Action (HeaderTransType hdr) hdr, Sendable hdr, Receivable hdr) => IResultHeader hdr where
  type HeaderTransType hdr
  toHeader :: [Var] -> hdr
