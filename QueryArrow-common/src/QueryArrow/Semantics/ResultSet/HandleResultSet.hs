{-# LANGUAGE TypeFamilies, MultiParamTypeClasses, FlexibleInstances, ScopedTypeVariables, GADTs, UndecidableInstances, ConstraintKinds,
   RankNTypes, FlexibleContexts #-}
module QueryArrow.Semantics.ResultSet.HandleResultSet where

import QueryArrow.Semantics.ResultHeader
import QueryArrow.Semantics.ResultRow
import QueryArrow.Semantics.ResultSet
import QueryArrow.Semantics.ResultSet.ResultStreamResultSet ()
import System.IO
import QueryArrow.Semantics.ResultStream
import QueryArrow.Semantics.Sendable
import Control.Monad.IO.Class
import Data.Conduit
import Data.Monoid
import QueryArrow.Data.Monoid.Action

-- | HandleResultSet takes a handle and act like a ResultSet. Make sure that the handle is not being used by other code while
-- while the result set is being used. The handle can be used after the code is done using this result
data HandleResultSet trans row = HandleResultSet trans (HeaderType row) Handle


instance (Receivable trans, Receivable (HeaderType row)) => Receivable (HandleResultSet trans row) where
  receive h = do
    hdrtrans <- receive h
    hdr <- receive h
    return (HandleResultSet hdrtrans hdr h)

instance (Coherent trans row, Receivable row) => Sendable (Partial (HandleResultSet trans row)) where
  send h (Partial rset) =
    send h (toResultStream rset)

instance (Coherent trans row, Receivable row) => Sendable (HandleResultSet trans row) where
  send h rset = do
    send h (mempty :: trans)
    send h (getHeader rset)
    send h (toResultStream rset)

instance (Coherent trans row, Receivable row) => ResultSet  (HandleResultSet trans row) where
  type ResultSetRowType (HandleResultSet trans row) = row
  type ResultSetTransType (HandleResultSet trans row) = trans
  toResultStream (HandleResultSet hdrtrans _ h) = do
    let rowtrans = tconvert hdrtrans :: RowTransType row
    let toRS = ResultStream (do
            mrow <- liftIO $ receive h
            case mrow of
              Nothing -> return ()
              Just row -> do
                yield (act rowtrans row)
                case toRS of
                  ResultStream rs -> rs)
    toRS
  getHeader (HandleResultSet hdrtrans hdr _) = act (tconvert hdrtrans :: HeaderTransType (HeaderType row)) hdr


instance Monoid trans => Action trans (HandleResultSet trans row) where
  act hdrtrans2 (HandleResultSet hdrtrans hdr h)  =
    HandleResultSet (hdrtrans2 <> hdrtrans) hdr h
