{-# LANGUAGE TypeFamilies, MultiParamTypeClasses, FlexibleInstances, ScopedTypeVariables, GADTs, UndecidableInstances, ConstraintKinds,
   RankNTypes, FlexibleContexts #-}
module QueryArrow.Semantics.ResultSet.TwoResultSet where

import QueryArrow.Semantics.ResultHeader
import QueryArrow.Semantics.ResultRow
import QueryArrow.Semantics.ResultSet
import QueryArrow.Semantics.ResultStream
import QueryArrow.Semantics.Sendable
import Data.Monoid
import QueryArrow.Data.Monoid.Action

data TwoResultSet trans a b = ResultSetRowType a ~ ResultSetRowType b =>
                                TwoResultSet trans (HeaderType (ResultSetRowType a)) a b

instance forall trans a b . (ResultSet a, ResultSet b, Sendable trans) => Sendable (TwoResultSet trans a b) where
  send h (TwoResultSet hdrtrans hdr a b) = do
    send h hdrtrans
    send h hdr
    send h (Partial a)
    send h (Partial b)
    send h (Nothing :: Maybe (ResultSetRowType a))

instance (ResultSet a, ResultSet b) => Sendable (Partial (TwoResultSet trans a b)) where
  send h (Partial (TwoResultSet _ _ a b)) = do
    send h (Partial a)
    send h (Partial b)

instance (Coherent trans (ResultSetRowType a), ResultSet a, ResultSet b, ResultSetTransType a ~ trans, ResultSetTransType b ~ trans) => ResultSet (TwoResultSet trans a b) where
  type ResultSetTransType (TwoResultSet trans a b) = trans
  type ResultSetRowType (TwoResultSet trans a b) = ResultSetRowType a
  toResultStream (TwoResultSet trans _ a b) =
    ResultStream (do
      runResultStream $ toResultStream (act trans a)
      runResultStream $ toResultStream (act trans b))
  getHeader (TwoResultSet hdrtrans hdr _ _) = act (tconvert hdrtrans :: HeaderTransType (HeaderType (ResultSetRowType a))) hdr

instance Monoid trans => Action trans (TwoResultSet trans a b) where
  act hdrtrans2 (TwoResultSet hdrtrans hdr a b)  =
    TwoResultSet (hdrtrans2 <> hdrtrans) hdr a b
