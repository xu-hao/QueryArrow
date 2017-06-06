{-# LANGUAGE StaticPointers, ExistentialQuantification, ScopedTypeVariables, GADTs, ConstraintKinds, FlexibleInstances, MultiParamTypeClasses, UndecidableSuperClasses, FlexibleContexts, UndecidableInstances, TypeFamilies, TupleSections, GeneralizedNewtypeDeriving #-}

module QueryArrow.Distributed.Serializable.Common where

import GHC.StaticPtr
import Data.Proxy
import Data.Constraint
import GHC.Fingerprint.Type
import Data.Maybe
import System.IO.Unsafe
import QueryArrow.Data.Some
import Data.Typeable
import QueryArrow.Distributed.Serializable
import Data.Functor.Identity
import Data.Int
import Data.Monoid
import Control.Exception
import Control.Monad.IO.Class
import Control.Monad.Error

instance Puttable put Fingerprint => Puttable put (StaticPtr a) where
  sPut ptr = sPut (staticKey ptr)

instance (MonadIO get, MonadError IOException get, Gettable get Fingerprint) => Gettable get (StaticPtr a) where
  sGet b0 = do
    (key, b1) <- sGet b0
    -- liftIO $ putStrLn ("lookup key " ++ show key)
    mstaticptr <- liftIO $ unsafeLookupStaticPtr key
    case mstaticptr of
      Nothing -> throwError (userError ("sGet (StaticPtr a): cannot find static ptr from key " ++ show key))
      Just staticptr -> do
        -- liftIO $ putStrLn ("found ptr")
        return (staticptr, b1)
