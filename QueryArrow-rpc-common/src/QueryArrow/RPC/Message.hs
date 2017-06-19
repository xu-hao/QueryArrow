{-# LANGUAGE ScopedTypeVariables, RankNTypes #-}
module QueryArrow.RPC.Message where

import Prelude hiding (length)
import Data.Binary.Get
import Data.Binary.Put
import System.IO
import Data.ByteString.Lazy (hGet, length, hPut)
import Data.Aeson
import Data.MessagePack
import qualified Data.ByteString.Lazy as BSL

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

receiveMsgPack :: forall a . (Show a, MessagePack a) => Handle -> IO (Maybe a)
receiveMsgPack handle = do
    reqlenbs <- hGet handle 8
    let reqlen = runGet getWord64be reqlenbs
--    putStrLn ("waiting for message")
    req <- hGet handle (fromIntegral reqlen)
--    putStrLn ("receive " ++ show reqlen ++ " bytes: " ++ show (BSL.unpack req))
    writeToTmpLog '>' reqlen
    a <- unpack req
    writeToTmpLog '>' (a :: a)
    Just <$> unpack req

sendMsgPack :: (Show a, MessagePack a) => Handle -> a -> IO ()
sendMsgPack handle req0 = do
    let req = pack req0
    let reqlen = length req
--    putStrLn ("send " ++ show reqlen ++ " bytes: " ++ show (BSL.unpack req))
--    hFlush stdout
    let reqlenbs = runPut (putWord64be (fromIntegral reqlen))
    writeToTmpLog '<' reqlenbs
    hPut handle reqlenbs
    writeToTmpLog '<' req0
    hPut handle req
--    putStrLn ("message sent")

writeToTmpLog :: Show a => Char -> a -> IO ()
writeToTmpLog ch a = withFile "/tmp/qatest.log" WriteMode $ \ h -> do
                    hPutStrLn h (replicate 10 ch ++ "begin")
                    hPutStrLn h (show a)
                    hPutStrLn h (replicate 10 ch ++ "end") 
