{-# LANGUAGE TypeFamilies, MultiParamTypeClasses, FlexibleInstances, ScopedTypeVariables, GADTs, UndecidableInstances, ConstraintKinds,
   RankNTypes, FlexibleContexts #-}
module QueryArrow.Semantics.ResultSet.ResultStreamResultSet where

import QueryArrow.Semantics.ResultHeader
import QueryArrow.Semantics.ResultRow
import QueryArrow.Semantics.ResultSet
import QueryArrow.Semantics.ResultStream
import QueryArrow.Semantics.Sendable
import Data.Conduit
import Data.Monoid
import Control.Monad.IO.Class
import QueryArrow.Data.Monoid.Action

data ResultStreamResultSet trans row = ResultStreamResultSet trans (HeaderType row) (DBResultStream row)

instance forall row . Sendable row => Sendable (DBResultStream row) where
  send h (ResultStream rs) =
    runConduitRes (rs =$=
      awaitForever (\row -> liftIO $ send h (Just row)))

instance Sendable row => Sendable (Partial (ResultStreamResultSet trans row)) where
  send h (Partial (ResultStreamResultSet _ _ rs)) = send h rs

instance forall trans row . (Sendable trans, IResultRow row) => Sendable (ResultStreamResultSet trans row) where
  send h (ResultStreamResultSet hdrtrans hdr rs) = do
    send h hdrtrans
    send h hdr
    send h rs
    send h (Nothing :: Maybe row)

instance (Coherent trans row) => ResultSet (ResultStreamResultSet trans row) where
  type ResultSetTransType (ResultStreamResultSet trans row) = trans
  type ResultSetRowType (ResultStreamResultSet trans row) = row
  toResultStream (ResultStreamResultSet hdrtrans _ (ResultStream rs)) = ResultStream (mapOutput (act (tconvert hdrtrans :: RowTransType row)) rs)
  getHeader (ResultStreamResultSet hdrtrans hdr _) = act (tconvert hdrtrans :: HeaderTransType (HeaderType row)) hdr

instance (Action trans row) => Action trans (DBResultStream row) where
  act trans (ResultStream rs) = ResultStream (mapOutput (act trans) rs)

instance (Monoid trans) => Action trans (ResultStreamResultSet trans row) where
  act hdrtrans2 (ResultStreamResultSet hdrtrans hdr rs) =
    ResultStreamResultSet (hdrtrans2 <> hdrtrans) hdr rs
