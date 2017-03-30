{-# LANGUAGE StaticPointers, ExistentialQuantification, ScopedTypeVariables, GADTs, ConstraintKinds, FlexibleInstances, MultiParamTypeClasses, UndecidableInstances, TypeApplications, TemplateHaskell #-}

module QueryArrow.Distributed.Serializable.Binary.Serialization where

import QueryArrow.Distributed.Serializable
import QueryArrow.Distributed.Serializable.TH
import Data.Binary.Get
import Data.Binary.Builder
import Data.Int
import qualified Data.ByteString as BS
import qualified Data.ByteString.Lazy as BSL
import Language.Haskell.TH
import Data.Constraint.Trivial
import Data.Constraint
import GHC.StaticPtr
import Data.Typeable
import QueryArrow.Data.Some
import Data.Functor.Identity
import Control.Monad.Except
import QueryArrow.Distributed.Serializable.Binary

-- Int64
instance Puttable BinaryPut Int64 where
  sPut i64 = return (putInt64be i64)

instance Gettable BinaryGet Int64 where
  sGet b0 =
    case runGetOrFail getInt64be b0 of
      Left (_, _, e) ->
        throwError (userError ("sGet Int64: " ++ e ++ " bs = " ++ show b0))
      Right (b1, _, i64) ->
        return (i64, b1)

-- instance DistributedSerializable Show Identity (ExceptT String IO) Int64 where
--   getStaticPtr _ = static getSomeShowInt64
--
-- getSomeShowInt64 :: BSL.ByteString -> ExceptT String IO (Some (DistributedSerializable Show Identity (ExceptT String IO)), BSL.ByteString )
-- getSomeShowInt64 bo = do
--   (i64, b1) <- sGet bo
--   return (Some (i64 :: Int64), b1)

$(deriveDistributedSerializable ''Unconstrained ''BinaryPut ''BinaryGet ''BSL.ByteString ''Int64)
$(deriveDistributedSerializable ''Show ''BinaryPut ''BinaryGet ''BSL.ByteString ''Int64)



-- ByteString
-- instance Serializable BS.ByteString where
--   sGet = do
--     n <- getInt64be
--     getByteString (fromIntegral n)
--   sPut = putByteString
--
-- -- $(deriveDistributedSerializable ''Unconstrained ''BS.ByteString)
-- -- $(deriveDistributedSerializable ''Show ''BS.ByteString)
--
-- -- Lazy ByteString
-- instance Serializable BSL.ByteString where
--   sGet = do
--     n <- getInt64be
--     getLazyByteString (fromIntegral n)
--   sPut = putLazyByteString
--
-- -- $(deriveDistributedSerializable ''Unconstrained ''BSL.ByteString)
-- -- $(deriveDistributedSerializable ''Show ''BSL.ByteString)
