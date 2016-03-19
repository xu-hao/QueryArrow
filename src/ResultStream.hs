{-# LANGUAGE TypeFamilies, MultiParamTypeClasses, FunctionalDependencies, ExistentialQuantification, FlexibleInstances, OverloadedStrings,
   RankNTypes, FlexibleContexts, GADTs #-}
module ResultStream (eos, ResultStream(..), listResultStream, depleteResultStream, getAllResultsInStream, takeResultStream,
    resultStreamTake, emptyResultStream, transformResultStream, isResultStreamEmpty, filterResultStream, ResultRow(..), resultStream2, bracketPStream) where

import Prelude  hiding (lookup, take, map, null)
import Control.Applicative ((<$>), empty, (<|>), Alternative)
import Control.Arrow ((+++))
import Data.Either.Utils
import Control.Monad (ap, guard)
import Control.Monad.IO.Class (MonadIO, liftIO)
import Control.Monad.Trans.State.Strict (StateT, evalStateT, get, put)
import Control.Monad.Trans.Class (lift, MonadTrans)
import Data.Monoid
import FO.Data
import Data.Conduit
import Data.Conduit.Combinators
import Control.Monad.Trans.Resource

class (Monoid row, Show row) => ResultRow row where
    transform :: [Var] -> [Var] -> row -> row

newtype ResultStream m row = ResultStream (Producer m row)

listResultStream :: (Monad m) => [a] -> ResultStream m a
listResultStream results  = ResultStream (yieldMany results)

depleteResultStream :: (Monad m) => ResultStream m row -> m ()
depleteResultStream (ResultStream rs) =
    (runConduit (rs =$ sinkNull))

getAllResultsInStream :: (Monad m, Show row) => ResultStream m row -> m [row]
getAllResultsInStream (ResultStream stream) =
    (runConduit (stream =$ sinkList))

takeResultStream :: (Monad m) => Int -> ResultStream m row -> ResultStream m row
takeResultStream n (ResultStream stream) = ResultStream (stream =$= take n)

resultStreamTake :: (Monad m, Show row) => Int -> ResultStream m row -> m [row]
resultStreamTake n =
    getAllResultsInStream . takeResultStream n

-- don't use this if you have following actions
emptyResultStream :: (Monad m) => ResultStream m a
emptyResultStream = listResultStream []

isResultStreamEmpty :: (Monad m) => ResultStream m a -> m Bool
isResultStreamEmpty (ResultStream rs) = (runConduit (rs =$ null))

eos :: (Monad m, Show a) => ResultStream m a -> ResultStream m Bool
eos stream = ResultStream (replicateM 1 ((isResultStreamEmpty stream)))

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

transformResultStream :: (Monad m, ResultRow row) => [Var] -> [Var] -> ResultStream m row -> ResultStream m row
transformResultStream vars1 vars2 (ResultStream rs) = ResultStream (rs =$= map (transform vars1 vars2))

filterResultStream :: (Monad m, ResultRow row) => ResultStream m row -> (row -> m Bool) -> ResultStream m row
filterResultStream (ResultStream rs) p = ResultStream (rs =$= filterM p)


instance (MonadIO m) => MonadIO (ResultStream m) where
    liftIO = lift . liftIO

resultStream2 :: (MonadResource m) => IO [row] -> IO () -> ResultStream m row
resultStream2 mrow finish = bracketPStream (return ()) (\_ -> finish) (\_ -> do
    rows <- liftIO mrow
    listResultStream rows)

bracketPStream :: (MonadResource m) => IO a -> (a -> IO ()) -> (a -> ResultStream m row) -> ResultStream m row
bracketPStream acq rel act = ResultStream (bracketP acq rel (\a -> case act a of ResultStream rs -> rs))
