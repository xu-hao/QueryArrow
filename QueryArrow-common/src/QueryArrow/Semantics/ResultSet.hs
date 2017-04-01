{-# LANGUAGE TypeFamilies, MultiParamTypeClasses, FlexibleInstances, ScopedTypeVariables, GADTs, UndecidableInstances, ConstraintKinds,
   RankNTypes, FlexibleContexts #-}
module QueryArrow.Semantics.ResultSet where

import QueryArrow.Semantics.ResultHeader
import QueryArrow.Semantics.ResultRow
import QueryArrow.Semantics.ResultStream
import QueryArrow.Semantics.Sendable
import QueryArrow.Syntax.Data
import Data.Set (Set)
import Data.Proxy
import QueryArrow.Data.Monoid.Action

class TotalConvertible a b where
  tconvert :: a -> b

newtype Partial a = Partial a

class (Sendable trans, Monoid trans, TotalConvertible trans (RowTransType row), TotalConvertible trans (HeaderTransType (HeaderType row)), IResultRow row) => Coherent trans row where
  combineRow :: HeaderType row -> row -> HeaderType row -> trans
  filterRow :: Proxy row -> Set Var -> HeaderType row -> trans
  alignHeaders :: Proxy row -> HeaderType row -> HeaderType row -> trans

-- | the sendable instance uses the follow binary format
-- result set transformation
-- header
-- 0x1
-- row
-- ...
-- 0x1
-- row
-- 0x0
class (Coherent (ResultSetTransType a) (ResultSetRowType a), Sendable (Partial a), Action (ResultSetTransType a) a, Sendable a, IResultRow (ResultSetRowType a)) => ResultSet a where
  type ResultSetRowType a
  type ResultSetTransType a
  toResultStream :: a -> DBResultStream (ResultSetRowType a)
  getHeader :: a -> HeaderType (ResultSetRowType a)
