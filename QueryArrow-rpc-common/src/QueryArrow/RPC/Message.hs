module QueryArrow.RPC.Message where

import Prelude hiding (length)
import Data.Binary.Get
import Data.Binary.Put
import System.IO
import Data.ByteString.Lazy (hGet, length, hPut)
import Data.Aeson
import Data.MessagePack

receiveMsg :: FromJSON a => Handle -> IO (Maybe a)
receiveMsg handle = do
    reqlenbs <- hGet handle 8
    let reqlen = runGet getWord64be reqlenbs
    req <- hGet handle (fromIntegral reqlen)
    return (decode req)

sendMsg :: ToJSON a => Handle -> a -> IO ()
sendMsg handle req0 = do
    let req = encode req0
    let reqlen = length req
    let reqlenbs = runPut (putWord64be (fromIntegral reqlen))
    hPut handle reqlenbs
    hPut handle req

receiveMsgPack :: MessagePack a => Handle -> IO (Maybe a)
receiveMsgPack handle = do
    reqlenbs <- hGet handle 8
    let reqlen = runGet getWord64be reqlenbs
    req <- hGet handle (fromIntegral reqlen)
    unpack req

sendMsgPack :: MessagePack a => Handle -> a -> IO ()
sendMsgPack handle req0 = do
    let req = pack req0
    let reqlen = length req
    let reqlenbs = runPut (putWord64be (fromIntegral reqlen))
    hPut handle reqlenbs
    hPut handle req
