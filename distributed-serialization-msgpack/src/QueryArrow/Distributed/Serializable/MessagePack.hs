{-# LANGUAGE StaticPointers, ExistentialQuantification, ScopedTypeVariables, GADTs, ConstraintKinds, FlexibleInstances, MultiParamTypeClasses, UndecidableSuperClasses, FlexibleContexts, UndecidableInstances, TypeFamilies, TupleSections, GeneralizedNewtypeDeriving #-}

module QueryArrow.Distributed.Serializable.MessagePack where

import GHC.StaticPtr
import Data.ByteString.Lazy as BSL hiding (putStrLn)
import Data.Proxy
import Data.Constraint
import GHC.Fingerprint.Type
import Data.MessagePack
import Data.Maybe
import System.IO.Unsafe
import QueryArrow.Data.Some
import Data.Typeable
import QueryArrow.Distributed.Serializable
import Data.Functor.Identity
import Data.Int
import Control.Monad.Except
import Data.Monoid
import Control.Exception
import QueryArrow.Distributed.Serializable.Common

newtype MessagePackGet a = MessagePackGet {unMessagePackGet :: IO a} deriving (Functor, Applicative, Monad, MonadIO, MonadError IOException)
newtype MessagePackPut a = MessagePackPut {unMessagePackPut :: Identity a} deriving (Functor, Applicative, Monad)

type instance PutTarget MessagePackPut = [Object] -> [Object]
type instance GetTarget MessagePackGet = [Object]

instance Functional Object [Object] where
  applyFunc (ObjectArray arr) = arr
  applyFunc a = [a]

instance Functional ([Object] -> [Object]) Object where
  applyFunc builder = ObjectArray (builder [])

instance Puttable MessagePackPut Fingerprint where
  sPut (Fingerprint w1 w2) = return (\tl -> ObjectWord w1 : ObjectWord w2 : tl)

instance Gettable MessagePackGet Fingerprint where
  sGet (ObjectWord w1 : ObjectWord w2 : tl) =
          return (Fingerprint w1 w2, tl)
  sGet object = throwError (userError ("sGet Fingerprint: cannot parse object " ++ show object))

serialize :: Some (DistributedSerializable c MessagePackPut MessagePackGet Object) -> Object
serialize = runIdentity . unMessagePackPut . serializeDistributed

deserialize :: Object -> IO (Some (DistributedSerializable c MessagePackPut MessagePackGet Object))
deserialize = unMessagePackGet . deserializeDistributed
