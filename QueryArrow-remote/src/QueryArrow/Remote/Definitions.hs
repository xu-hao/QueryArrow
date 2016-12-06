{-# LANGUAGE TypeFamilies, MultiParamTypeClasses, StaticPointers, RankNTypes, GADTs, ConstraintKinds, FlexibleContexts, TypeApplications, ScopedTypeVariables, FlexibleInstances #-}

module QueryArrow.Remote.Definitions where


class Channel a where
  type SendType a
  type ReceiveType a
  send :: a -> SendType a -> IO ()
  receive :: a -> IO (ReceiveType a)
  rpc :: a -> SendType a -> IO (ReceiveType a)
  rpc a s = send a s >> receive a


data Dict c where
  Dict :: forall c . c => Dict c
