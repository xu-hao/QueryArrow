{-# LANGUAGE StaticPointers, ExistentialQuantification, ScopedTypeVariables, GADTs, ConstraintKinds, FlexibleInstances, MultiParamTypeClasses, UndecidableInstances, TypeApplications, TemplateHaskell #-}

module QueryArrow.Distributed.Serializable.MessagePack.Serialization where

import QueryArrow.Distributed.Serializable
import QueryArrow.Distributed.Serializable.TH
import Data.MessagePack
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
import QueryArrow.Distributed.Serializable.MessagePack

-- Int64
instance Puttable MessagePackPut Int64 where
  sPut i64 = return (\tl -> ObjectWord (fromIntegral i64) : tl)

instance Gettable MessagePackGet Int64 where
  sGet (ObjectWord b0 : tl) = return (fromIntegral b0, tl)

$(deriveDistributedSerializable ''Unconstrained ''MessagePackPut ''MessagePackGet ''Object ''Int64)
$(deriveDistributedSerializable ''Show ''MessagePackPut ''MessagePackGet ''Object ''Int64)



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
