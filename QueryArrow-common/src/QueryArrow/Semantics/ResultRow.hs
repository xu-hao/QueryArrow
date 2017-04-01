{-# LANGUAGE TypeFamilies, MultiParamTypeClasses, FlexibleInstances, ConstraintKinds,
   RankNTypes, FlexibleContexts #-}

module QueryArrow.Semantics.ResultRow where
import QueryArrow.Semantics.ResultValue
import QueryArrow.Semantics.ResultHeader
import QueryArrow.Semantics.Sendable
import QueryArrow.Syntax.Data
import Data.Vector (Vector)
import QueryArrow.Data.Monoid.Action

type RowSendable row = (Sendable row, Sendable (ElemType row))

class (Show row, Eq row, ResultValue (ElemType row), Ord (ElemType row), Num (ElemType row), Fractional (ElemType row), Action (RowTransType row) row, RowSendable row, IResultHeader (HeaderType row)) => IResultRow row where
    type ElemType row
    type HeaderType row
    type RowTransType row
    updateRow :: [(Var, ElemType row)] -> HeaderType row -> row -> row
    newRow :: HeaderType row -> Vector (ElemType row) -> row
    ext :: Var -> HeaderType row -> row -> Maybe (ElemType row)
