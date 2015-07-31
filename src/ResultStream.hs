{-# LANGUAGE TypeFamilies, MultiParamTypeClasses, FunctionalDependencies, ExistentialQuantification, FlexibleInstances, OverloadedStrings,
   RankNTypes, FlexibleContexts, GADTs #-}
module ResultStream (eos, ResultStream(..), Iteratee, listResultStream, depleteResultStream, getAllResultsInStream, takeResultStream,
    resultStreamTake, emptyResultStream) where

import Prelude  hiding (lookup)
import Control.Applicative ((<$>))
import Control.Arrow ((+++))
import Data.Either.Utils
import Control.Monad.IO.Class (MonadIO, liftIO)
import Control.Monad.Trans.State.Strict (StateT, evalStateT, get, put)
import Control.Monad.Trans.Class (lift, MonadTrans)

-- result stream see Takusen
type Iteratee row seed m = row -> seed -> m (Either seed seed)

newtype ResultStream m row where
    ResultStream :: {runResultStream :: (forall seed. Iteratee row seed m -> seed -> m (Either seed seed))} -> ResultStream m row

listResultStream :: (Functor m, Monad m) => [a] -> ResultStream m a
listResultStream results  = ResultStream (\iteratee seed->
    foldEitherM (flip iteratee) seed results) where
        foldEitherM _ a [] = return (Right a)
        foldEitherM g a (b : bs) = do
            c <- g a b
            case c of
                Left d -> return c
                Right d -> foldEitherM g d bs

depleteResultStream :: (Functor m, Monad m) => ResultStream m row -> m ()
depleteResultStream rs =
    fromEither <$> runResultStream rs (\ _ seed -> return (Right seed)) ()

getAllResultsInStream :: (Functor m, Monad m) => ResultStream m row -> m [row]
getAllResultsInStream stream =
    fromEither <$> runResultStream stream (\row seed  -> return (Right (seed ++ [row]))) []

takeResultStream :: (Functor m, Monad m) => Int -> ResultStream m row -> ResultStream m row
takeResultStream n stream = ResultStream(\iteratee seed -> do
    seedeither <- runResultStream stream (\row (n, seed') ->
        if n == 0
            then return (Left (n, seed'))
            else do
                seedeither <- iteratee row seed'
                let f x = (n-1,x)
                return ((+++) f f seedeither)
        ) (n, seed)
    return ((+++) snd snd seedeither))

resultStreamTake :: (Functor m, Monad m) => Int -> ResultStream m row -> m [row]
resultStreamTake n =
    getAllResultsInStream . takeResultStream n

-- don't use this if you have following actions
emptyResultStream :: (Functor m, Monad m) => ResultStream m a
emptyResultStream = listResultStream []

eos :: (Functor m, Monad m) => ResultStream m a -> ResultStream m Bool
eos stream = lift (null <$> resultStreamTake 1 stream)

instance MonadTrans ResultStream where
    lift f = ResultStream (\iteratee seed -> do
        a <- f
        iteratee a seed)

-- instance Applicative m => Applicative (ResultStream m seed) where

instance Functor (ResultStream m) where
    fmap f (ResultStream enumerator) = ResultStream (\iteratee seed ->
        enumerator (iteratee . f) seed)

instance (Functor m, Monad m) => Monad (ResultStream m) where
    return a = ResultStream (\iteratee seed -> iteratee a seed)
    (ResultStream enumerator) >>= f = ResultStream (\iteratee seed ->
        enumerator (\row seed2 -> runResultStream (f row) iteratee seed2) seed)

instance MonadIO (ResultStream IO) where
    liftIO f = ResultStream (\iteratee seed -> f >>= \a-> runResultStream (return a) iteratee seed)

instance MonadIO (ResultStream (StateT s IO)) where
    liftIO f = ResultStream (\iteratee seed -> lift f >>= \a-> runResultStream (return a) iteratee seed)
