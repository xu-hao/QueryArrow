{-# LANGUAGE MultiParamTypeClasses, TypeFamilies #-}

module QueryArrow.Remote.Channel where

import QueryArrow.Remote.Definitions
import System.IO (Handle)
import QueryArrow.RPC.Message
import Data.Aeson

newtype HandleChannel a b = HandleChannel Handle
instance (ToJSON a, FromJSON b) => Channel (HandleChannel a b) where
  type SendType (HandleChannel a b) = a
  type ReceiveType (HandleChannel a b) = b
  send (HandleChannel h) a = sendMsg h (toJSON a)
  receive (HandleChannel h) = do
    Just res <- receiveMsg h
    let Success v = fromJSON res
    return v
