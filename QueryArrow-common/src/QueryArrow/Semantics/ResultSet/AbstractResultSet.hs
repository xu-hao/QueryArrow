{-# LANGUAGE TypeFamilies, MultiParamTypeClasses, FlexibleInstances, ScopedTypeVariables, GADTs, UndecidableInstances, ConstraintKinds,
   RankNTypes, FlexibleContexts #-}
module QueryArrow.Semantics.ResultSet.AbstractResultSet where

import QueryArrow.Semantics.ResultSet
import QueryArrow.Semantics.Sendable
import QueryArrow.Data.Monoid.Action

data AbstractResultSet trans row = forall a . (ResultSet a, Action trans a, ResultSetTransType a ~ trans, ResultSetRowType a ~ row) => AbstractResultSet a

instance Sendable (AbstractResultSet trans row) where
  send h (AbstractResultSet rset) =
    send h rset

instance Sendable (Partial (AbstractResultSet trans row)) where
  send h (Partial (AbstractResultSet rset)) =
    send h (Partial rset)

instance (Coherent trans row) => ResultSet (AbstractResultSet trans row) where
  type ResultSetTransType (AbstractResultSet trans row) = trans
  type ResultSetRowType (AbstractResultSet trans row) = row
  toResultStream (AbstractResultSet a) = toResultStream a
  getHeader (AbstractResultSet a) = getHeader a

instance Action trans (AbstractResultSet trans row) where
  act trans (AbstractResultSet a) =
    AbstractResultSet (act trans a)
