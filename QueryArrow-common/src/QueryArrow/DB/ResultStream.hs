{-# LANGUAGE TypeFamilies, MultiParamTypeClasses, FlexibleInstances,
   RankNTypes, FlexibleContexts #-}
module QueryArrow.DB.ResultStream (eos, ResultStream(..), listResultStream, depleteResultStream, getAllResultsInStream, takeResultStream, closeResultStream,
    resultStreamTake, emptyResultStream, projResultStream, isResultStreamEmpty, filterResultStream, mapResultStream, IResultRow(..), resultStream2, bracketPStream)
    where

import Prelude  hiding (lookup, take, map, null, mapM)
import Control.Applicative (empty, (<|>), Alternative)
import Control.Monad (ap)
import Control.Monad.IO.Class (MonadIO, liftIO)
import Control.Monad.Trans.Class (lift, MonadTrans)
import QueryArrow.FO.Data
import Data.Conduit
import Data.Conduit.Combinators
import Control.Monad.Trans.Resource
import Data.Set (Set)

class (Monoid row, Show row, Num (ElemType row), Ord (ElemType row), Fractional (ElemType row), Eq row) => IResultRow row where
    type ElemType row
    proj :: Set Var -> row -> row
    sing :: Var -> ElemType row -> row
    get :: Var -> row -> ElemType row

newtype ResultStream m row = ResultStream (Producer m row)

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
resultStreamTake n =
    getAllResultsInStream . takeResultStream n

-- don't use this if you have following actions
emptyResultStream :: (Monad m) => ResultStream m a
emptyResultStream = listResultStream []

isResultStreamEmpty :: (Monad m) => ResultStream m a -> m Bool
isResultStreamEmpty (ResultStream rs) = runConduit (rs =$ null)

eos :: (Monad m) => ResultStream m a -> ResultStream m Bool
eos stream = ResultStream (replicateM 1 (isResultStreamEmpty stream))

instance MonadTrans ResultStream where
    lift f = ResultStream (replicateM 1 f)

instance (Monad m) => Functor (ResultStream m) where
    fmap f (ResultStream rs) = ResultStream (rs =$= map f)

instance (Monad m) => Applicative (ResultStream m) where
    pure a = ResultStream (yield a)
    (<*>) = ap

instance (Monad m) => Monad (ResultStream m) where
    ResultStream rs >>= f = ResultStream (rs =$= awaitForever (\r -> case f r of (ResultStream rs') -> rs'))

instance (Monad m) => Alternative (ResultStream m) where
    empty = emptyResultStream
    ResultStream rs1 <|> ResultStream rs2 = ResultStream (do
        rs1
        rs2)

projResultStream :: (Monad m, IResultRow row) => Set Var -> ResultStream m row -> ResultStream m row
projResultStream vars1 (ResultStream rs) = ResultStream (rs =$= map (proj vars1))

filterResultStream :: (Monad m) => ResultStream m row -> (row -> m Bool) -> ResultStream m row
filterResultStream (ResultStream rs) p = ResultStream (rs =$= filterM p)

mapResultStream :: (Monad m) => ResultStream m row -> (row -> m row) -> ResultStream m row
mapResultStream (ResultStream rs) p = ResultStream (rs =$= mapM p)

instance (MonadIO m) => MonadIO (ResultStream m) where
    liftIO = lift . liftIO

resultStream2 :: (MonadResource m) => IO [row] -> IO () -> ResultStream m row
resultStream2 mrow finish = bracketPStream (return ()) (\_ -> finish) (\_ -> do
    rows <- liftIO mrow
    listResultStream rows)

bracketPStream :: (MonadResource m) => IO a -> (a -> IO ()) -> (a -> ResultStream m row) -> ResultStream m row
bracketPStream acq rel act = ResultStream (bracketP acq rel (\a -> case act a of ResultStream rs -> rs))
