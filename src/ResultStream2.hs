{-# LANGUAGE TypeFamilies, MultiParamTypeClasses, FunctionalDependencies, ExistentialQuantification, FlexibleInstances, OverloadedStrings,
   RankNTypes, FlexibleContexts, GADTs #-}
module ResultStream2 (eos, ResultStream(..), Iteratee, listResultStream, depleteResultStream, getAllResultsInStream, takeResultStream,
    resultStreamTake, emptyResultStream, transformResultStream, isResultStreamEmpty, filterResultStream, ResultRow(..), resultStream2) where

import Prelude  hiding (lookup)
import Control.Applicative ((<$>), empty, (<|>), Alternative)
import Control.Arrow ((+++))
import Data.Either.Utils
import Control.Monad (ap, guard)
import Control.Monad.IO.Class (MonadIO, liftIO)
import Control.Monad.Trans.State.Strict (StateT, evalStateT, get, put)
import Control.Monad.Trans.Class (lift, MonadTrans)
import Data.Monoid
import FO.Data

class (Monoid row, Show row) => ResultRow row where
    transform :: [Var] -> [Var] -> row -> row

-- result stream see Takusen
type Iteratee row seed m = row -> seed -> m (Either seed seed)

newtype ResultStream m row where
    ResultStream :: {runResultStream :: (forall seed. Iteratee row seed m -> seed -> m (Either seed seed))} -> ResultStream m row

listResultStream :: (Monad m) => [a] -> ResultStream m a
listResultStream results  = ResultStream (\iteratee seed->
    foldEitherM (flip iteratee) seed results) where
        foldEitherM _ a [] = return (Right a)
        foldEitherM g a (b : bs) = do
            c <- g a b
            case c of
                Left d -> return c
                Right d -> foldEitherM g d bs

depleteResultStream :: (Monad m) => ResultStream m row -> m ()
depleteResultStream rs =
    fromEither <$> runResultStream rs (\ _ seed -> return (Right seed)) ()

getAllResultsInStream :: (Monad m, Show row) => ResultStream m row -> m [row]
getAllResultsInStream stream =
    fromEither <$> runResultStream stream (\row seed  -> return (Right (seed ++ [row]))) []

takeResultStream :: (Monad m) => Int -> ResultStream m row -> ResultStream m row
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

resultStreamTake :: (Monad m, Show row) => Int -> ResultStream m row -> m [row]
resultStreamTake n =
    getAllResultsInStream . takeResultStream n

-- don't use this if you have following actions
emptyResultStream :: (Monad m) => ResultStream m a
emptyResultStream = listResultStream []

isResultStreamEmpty :: (Monad m) => ResultStream m a -> m Bool
isResultStreamEmpty (ResultStream rs) = do
    emp <- rs (\_ seed -> return (Left (not seed))) True
    return (fromEither emp)

eos :: (Monad m, Show a) => ResultStream m a -> ResultStream m Bool
eos stream = lift (null <$> resultStreamTake 1 stream)

instance MonadTrans ResultStream where
    lift f = ResultStream (\iteratee seed -> do
        a <- f
        iteratee a seed)

-- instance Applicative m => Applicative (ResultStream m seed) where

instance Functor (ResultStream m) where
    fmap f (ResultStream enumerator) = ResultStream (\iteratee seed ->
        enumerator (iteratee . f) seed)

instance (Functor m, Monad m) => Applicative (ResultStream m) where
    pure a = ResultStream (\iteratee seed -> iteratee a seed)
    (<*>) = ap

instance (Functor m, Monad m) => Monad (ResultStream m) where
    (ResultStream enumerator) >>= f = ResultStream (\iteratee seed ->
        enumerator (\row seed2 -> runResultStream (f row) iteratee seed2) seed)

instance (Functor m, Monad m) => Alternative (ResultStream m) where
    empty = emptyResultStream
    (ResultStream enumerator1) <|> (ResultStream enumerator2) = ResultStream (\iteratee seed -> do
        mseed <- enumerator1 iteratee seed
        case mseed of
             Left _ -> return mseed
             Right seed' -> enumerator2 iteratee seed')

transformResultStream :: (Monad m, ResultRow row) => [Var] -> [Var] -> ResultStream m row -> ResultStream m row
transformResultStream vars1 vars2 rs = do
    row <- rs
    return (transform vars1 vars2 row)

filterResultStream :: (Monad m, ResultRow row) => ResultStream m row -> (row -> m Bool) -> ResultStream m row
filterResultStream rs p = do
    row <- rs
    pass <- lift (p row)
    guard pass
    return row


instance (Functor m, MonadIO m) => MonadIO (ResultStream m) where
    liftIO = lift . liftIO

foldlM2 :: (MonadIO m) => Iteratee row seed m -> m () -> seed -> [row] -> m (Either seed seed)
foldlM2 iteratee finish seed rows = case rows of
    [] -> return (Right seed)
    (row : rest) -> do
        seednew <- iteratee row seed
        case seednew of
            Left _ -> do
                finish
                return seednew
            Right seednew2 ->
                foldlM2 iteratee finish seednew2 rest

resultStream2 :: (MonadIO m) => m [row] -> m () -> ResultStream m row
resultStream2 pre finish = ResultStream (\iteratee seed -> do
    rows <- pre
    foldlM2 iteratee finish seed rows)
