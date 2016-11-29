module QueryArrow.RPC.Message where

import Prelude hiding (length)
import Data.Binary.Get
import Data.Binary.Put
import System.IO
import Data.ByteString.Lazy
import Data.Aeson


receiveMsg :: FromJSON a => Handle -> IO (Maybe a)
receiveMsg handle = do
    reqlenbs <- hGet handle 4
    let reqlen = runGet getWord32be reqlenbs
    req <- hGet handle (fromIntegral reqlen)
    return (decode req)

sendMsg :: ToJSON a => Handle -> a -> IO ()
sendMsg handle req0 = do
    let req = encode req0
    let reqlen = length req
    let reqlenbs = runPut (putWord32be (fromIntegral reqlen))
    hPut handle reqlenbs
    hPut handle req
