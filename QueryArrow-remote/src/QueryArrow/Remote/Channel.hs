{-# LANGUAGE MultiParamTypeClasses, TypeFamilies #-}

module QueryArrow.Remote.Channel where

import QueryArrow.Remote.Definitions
import System.IO (Handle)
import QueryArrow.RPC.Message
import Data.MessagePack

newtype HandleChannel a b = HandleChannel Handle
instance (MessagePack a, MessagePack b) => Channel (HandleChannel a b) where
  type SendType (HandleChannel a b) = a
  type ReceiveType (HandleChannel a b) = b
  send (HandleChannel h) a = sendMsgPack h a
  receive (HandleChannel h) = do
    Just res <- receiveMsgPack h
    return res
