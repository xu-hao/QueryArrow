{-# LANGUAGE StaticPointers, ExistentialQuantification, ScopedTypeVariables, GADTs, ConstraintKinds, FlexibleInstances, MultiParamTypeClasses, UndecidableSuperClasses, FlexibleContexts, UndecidableInstances, TypeFamilies, TupleSections, GeneralizedNewtypeDeriving #-}

module QueryArrow.Distributed.Serializable.Binary where

import GHC.StaticPtr
import Data.ByteString.Lazy as BSL hiding (putStrLn)
import Data.Proxy
import Data.Constraint
import GHC.Fingerprint.Type
import Data.Binary.Builder
import Data.Binary.Get
import Data.Maybe
import System.IO.Unsafe
import QueryArrow.Data.Some
import Data.Typeable
import QueryArrow.Distributed.Serializable
import QueryArrow.Distributed.Serializable.Common
import Data.Functor.Identity
import Data.Int
import Control.Monad.Except
import Data.Monoid
import Control.Exception

newtype BinaryGet a = BinaryGet {unBinaryGet :: IO a} deriving (Functor, Applicative, Monad, MonadIO, MonadError IOException)
newtype BinaryPut a = BinaryPut {unBinaryPut :: Identity a} deriving (Functor, Applicative, Monad)

type instance PutTarget BinaryPut = Builder
type instance GetTarget BinaryGet = ByteString

instance Functional ByteString ByteString where
  applyFunc = id

instance Functional Builder ByteString where
  applyFunc = toLazyByteString

instance Puttable BinaryPut Fingerprint where
  sPut (Fingerprint w1 w2) = return (
                            putWord64be w1 <>
                            putWord64be w2)

instance Gettable BinaryGet Fingerprint where
  sGet b0 =
    case runGetOrFail (do
          w1 <- getWord64be
          w2 <- getWord64be
          return (Fingerprint w1 w2)) b0 of
            Left (_, _, e) ->
                throwError (userError e)
            Right (b1, _, fingerprint) ->
                return (fingerprint, b1)

serialize :: Some (DistributedSerializable c BinaryPut BinaryGet ByteString) -> ByteString
serialize = runIdentity . unBinaryPut . serializeDistributed

deserialize :: ByteString -> IO (Some (DistributedSerializable c BinaryPut BinaryGet ByteString))
deserialize = unBinaryGet . deserializeDistributed
