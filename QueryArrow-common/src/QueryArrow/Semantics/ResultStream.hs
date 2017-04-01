{-# LANGUAGE TypeFamilies, MultiParamTypeClasses, FlexibleInstances,
   RankNTypes, FlexibleContexts #-}
module QueryArrow.Semantics.ResultStream (eos, listResultStream, depleteResultStream, getAllResultsInStream, takeResultStream, closeResultStream,
    resultStreamTake, emptyResultStream, transformResultStream, isResultStreamEmpty, filterResultStream, mapResultStream, IResultRow(..), ResultStream(..), DBResultStream)
    where

import Prelude  hiding (lookup, take, map, null, mapM)
import QueryArrow.Semantics.ResultRow
import Data.Conduit
import Data.Conduit.Combinators
import Control.Monad.Trans.Resource
import QueryArrow.Data.Monoid.Action

-- We need to use Producer instead of Source because of PartialResultSetResultStreamResultSet
-- We need to use newtype because Producer is a polymorphic type and therefore belong to a larger universe and cannot be
-- instantiated without ImpredicativePolymorphism.
newtype ResultStream m row = ResultStream {runResultStream :: Producer m row}

type DBResultStream row = ResultStream (ResourceT IO) row

listResultStream :: (Monad m) => [a] -> ResultStream m a
listResultStream results  = ResultStream (yieldMany results)

depleteResultStream :: (Monad m) => ResultStream m row -> m ()
depleteResultStream (ResultStream rs) =
    runConduit (rs =$ sinkNull)

closeResultStream :: (Monad m) => ResultStream m row -> ResultStream m row
closeResultStream (ResultStream rs) =
    ResultStream (rs =$ sinkNull)

getAllResultsInStream :: (Monad m) => ResultStream m row -> m [row]
getAllResultsInStream (ResultStream stream) =
    runConduit (stream =$ sinkList)

takeResultStream :: (Monad m) => Int -> ResultStream m row -> ResultStream m row
takeResultStream n (ResultStream stream) = ResultStream (stream =$= take n)

resultStreamTake :: (Monad m) => Int -> ResultStream m row -> m [row]
resultStreamTake n rs =
    getAllResultsInStream (takeResultStream n rs)

-- don't use this if you have following actions
emptyResultStream :: (Monad m) => ResultStream m a
emptyResultStream = listResultStream []

isResultStreamEmpty :: (Monad m) => ResultStream m a -> m Bool
isResultStreamEmpty (ResultStream rs) = runConduit (rs =$ null)

eos :: (Monad m) => ResultStream m a -> ResultStream m Bool
eos stream = ResultStream (replicateM 1 (isResultStreamEmpty stream))

transformResultStream :: (Monad m, Action trans row) => trans -> ResultStream m row -> ResultStream m row
transformResultStream hdrtrans (ResultStream rs) = ResultStream (mapOutput (act hdrtrans) rs)

filterResultStream :: (Monad m) => ResultStream m row -> (row -> m Bool) -> ResultStream m row
filterResultStream (ResultStream rs) p = ResultStream (rs =$= filterM p)

mapResultStream :: (Monad m) => ResultStream m row -> (row -> m row) -> ResultStream m row
mapResultStream (ResultStream rs) p = ResultStream (rs =$= mapM p)
