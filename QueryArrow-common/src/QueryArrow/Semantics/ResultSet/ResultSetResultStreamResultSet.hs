{-# LANGUAGE TypeFamilies, MultiParamTypeClasses, FlexibleInstances, ScopedTypeVariables, GADTs, UndecidableInstances, ConstraintKinds,
   RankNTypes, FlexibleContexts #-}
module QueryArrow.Semantics.ResultSet.ResultSetResultStreamResultSet where

import QueryArrow.Semantics.ResultHeader
import QueryArrow.Semantics.ResultRow
import QueryArrow.Semantics.ResultSet
import QueryArrow.Semantics.ResultStream
import QueryArrow.Semantics.Sendable
import Control.Monad.IO.Class
import Data.Conduit
import Data.Monoid
import QueryArrow.Data.Monoid.Action

data ResultSetResultStreamResultSet trans a = ResultSetResultStreamResultSet trans (HeaderType (ResultSetRowType a)) (DBResultStream a)

instance forall trans a . (Sendable trans, ResultSet a) => Sendable (ResultSetResultStreamResultSet trans a) where
  send h (ResultSetResultStreamResultSet hdrtrans hdr (ResultStream rsrs)) = do
    send h hdrtrans
    send h hdr
    runConduitRes (rsrs =$=
      awaitForever (\rs -> liftIO $ send h rs))
    send h (Nothing :: Maybe (ResultSetRowType a))

instance forall trans a . (Sendable trans, ResultSet a) => Sendable (Partial (ResultSetResultStreamResultSet trans a)) where
  send h (Partial (ResultSetResultStreamResultSet _ _ (ResultStream rsrs))) =
    runConduitRes (rsrs =$=
      awaitForever (\rs -> liftIO $ send h rs))

instance (Coherent trans (ResultSetRowType a), ResultSet a, Sendable a) => ResultSet (ResultSetResultStreamResultSet trans a) where
  type ResultSetTransType (ResultSetResultStreamResultSet trans a) = trans
  type ResultSetRowType (ResultSetResultStreamResultSet trans a) = ResultSetRowType a
  toResultStream (ResultSetResultStreamResultSet hdrtrans _ (ResultStream rsetrs)) = do
    let rowtrans = tconvert hdrtrans :: RowTransType (ResultSetRowType a)
    ResultStream (rsetrs =$= awaitForever (\rset ->
      mapOutput (act rowtrans) (case toResultStream rset of ResultStream rs -> rs)))
  getHeader (ResultSetResultStreamResultSet hdrtrans hdr _) = act (tconvert hdrtrans :: HeaderTransType (HeaderType (ResultSetRowType a))) hdr

instance Monoid trans => Action trans (ResultSetResultStreamResultSet trans row) where
  act hdrtrans2 (ResultSetResultStreamResultSet hdrtrans hdr rsetrs)  =
    ResultSetResultStreamResultSet (hdrtrans2 <> hdrtrans) hdr rsetrs
