{-# LANGUAGE TypeFamilies, MultiParamTypeClasses, FlexibleInstances,
   RankNTypes, FlexibleContexts #-}
module QueryArrow.DB.ResultStream (eos, ResultStream, ResultStreamTrans, listResultStream, depleteResultStream, getAllResultsInStream, takeResultStream, closeResultStream,
    resultStreamTake, emptyResultStream, projResultStream, isResultStreamEmpty, filterResultStream, mapResultStream, IResultRow(..), bracketPStream, resultStream2)
    where

import Prelude  hiding (lookup, take, map, null, mapM)
import Control.Monad.IO.Class (liftIO)
import QueryArrow.Syntax.Term
import Data.Conduit
import Data.Conduit.Combinators
import Control.Monad.Trans.Resource
import Data.Set (Set)

class (Monoid row, Show row, Num (ElemType row), Ord (ElemType row), Fractional (ElemType row), Eq row) => IResultRow row where
    type ElemType row
    proj :: Set Var -> row -> row
    sing :: Var -> ElemType row -> row
    get :: Var -> row -> ElemType row

type ResultStream m o = ConduitT () o m ()

type ResultStreamTrans m o = ConduitT o o m ()

listResultStream :: (Monad m) => [a] -> ResultStream m a
listResultStream = yieldMany

depleteResultStream :: (Monad m) => ResultStream m row -> m ()
depleteResultStream rs =
    runConduit (rs .| sinkNull)

closeResultStream :: (Monad m) => ResultStream m row -> ResultStream m row
closeResultStream rs =
    rs .| sinkNull

getAllResultsInStream :: (Monad m) => ResultStream m row -> m [row]
getAllResultsInStream stream =
    runConduit (stream .| sinkList)

takeResultStream :: (Monad m) => Int -> ResultStream m row -> ResultStream m row
takeResultStream n stream = stream .| take n

resultStreamTake :: (Monad m) => Int -> ResultStream m row -> m [row]
resultStreamTake n rs =
    getAllResultsInStream (takeResultStream n rs)

-- don't use this if you have following actions
emptyResultStream :: (Monad m) => ResultStream m a
emptyResultStream = listResultStream []

isResultStreamEmpty :: (Monad m) => ResultStream m a -> m Bool
isResultStreamEmpty rs = runConduit (rs .| null)

eos :: (Monad m) => ResultStream m a -> ResultStream m Bool
eos stream = yieldM (isResultStreamEmpty stream)

projResultStream :: (Monad m, IResultRow row) => Set Var -> ResultStream m row -> ResultStream m row
projResultStream vars1 rs = rs .| map (proj vars1)

filterResultStream :: (Monad m) => ResultStream m row -> (row -> m Bool) -> ResultStream m row
filterResultStream rs p = rs .| filterM p

mapResultStream :: (Monad m) => ResultStream m row -> (row -> m row) -> ResultStream m row
mapResultStream rs p = rs .| mapM p

bracketPStream :: (MonadResource m) => IO a -> (a -> IO ()) -> (a -> ResultStream m row) -> ResultStream m row
bracketPStream = bracketP

resultStream2 :: (MonadResource m) => IO [row] -> IO () -> ResultStream m row
resultStream2 mrow finish = bracketP (return ()) (const finish) (const (do
    rows <- liftIO mrow
    listResultStream rows))

