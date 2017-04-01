{-# LANGUAGE GADTs, PatternSynonyms #-}

module QueryArrow.Semantics.Sendable where

import System.IO
import Data.Binary
import Data.Binary.Put
import qualified Data.ByteString as BS
import qualified Data.ByteString.Lazy as BSL
import QueryArrow.Syntax.Data hiding ((.|.))
import Data.Bits
import Control.Monad

class Sendable a where
  send :: Handle -> a -> IO ()

class Receivable a where
  receive :: Handle -> IO a

data BinaryWrapper a = Binary a => BinaryWrapper a

instance Binary a => Sendable (BinaryWrapper a) where
  send h (BinaryWrapper a) = BSL.hPut h (encode a)

hGetWord8 :: Handle -> IO Word8
hGetWord8 h = fromIntegral . BS.head <$> BS.hGet h 1

hPutWord8 :: Handle -> Word8 -> IO ()
hPutWord8 h i = send h (BinaryWrapper (runPut (putWord8 i)))

putPosVarInt :: Integer -> Put
putPosVarInt i = do
  let i0 = fromInteger (i .&. 0x7f)
  let i2 = shiftR i 7
  if i2 == 0
    then putWord8 i0
    else do
      putWord8 (0x80 .|. i0)
      putPosVarInt i2

getPosVarInt :: Get Integer
getPosVarInt =
  getPosVarInt2 0 0 where
    getPosVarInt2 :: Integer -> Int -> Get Integer
    getPosVarInt2 i0 n = do
      i <- getWord8
      let i1 = shiftL i0 n .|. fromIntegral (i .&. 0x7f)
      if i .&. 0x80 == 0
        then return i1
        else getPosVarInt2 i1 (n + 7)

receivePosVarInt :: Handle -> IO Integer
receivePosVarInt h =
  receivePosVarInt2 h 0 0 where
    receivePosVarInt2 :: Handle -> Integer -> Int -> IO Integer
    receivePosVarInt2 h i0 n = do
      i <- hGetWord8 h
      let i1 = shiftL i0 n .|. fromIntegral (i .&. 0x7f)
      if i .&. 0x80 == 0
        then return i1
        else receivePosVarInt2 h i1 (n + 7)

sendPosVarInt :: Handle -> Integer -> IO ()
sendPosVarInt h i = BSL.hPut h (runPut (putPosVarInt i))

instance Sendable a => Sendable (Maybe a) where
  send h Nothing =
    BSL.hPut h (BSL.singleton 0)
  send h (Just x) = do
    BSL.hPut h (BSL.singleton 1)
    send h x

instance Receivable a => Receivable (Maybe a) where
  receive h = do
    b <- BSL.hGet h 1
    if BSL.head b == 0
      then return Nothing
      else Just <$> receive h

instance Sendable Int where
  send h i | i >= 0 = sendPosVarInt h (fromIntegral i)
  send _ i = error ("send of Int: cannot send int " ++ show i)

instance Receivable Int where
  receive h = fromInteger <$> receivePosVarInt h

instance Sendable Word8 where
  send = hPutWord8

instance Receivable Word8 where
  receive = hGetWord8

instance Sendable Char where
  send  = hPutChar

instance Receivable Char where
  receive = hGetChar

instance Sendable Var where
  send h (Var v) = send h v

instance Receivable Var where
  receive h = Var <$> receive h

pattern NullBinary :: Word8
pattern NullBinary = 0x0
pattern Int32TypeBinary :: Word8
pattern Int32TypeBinary = 0x1
pattern Int64TypeBinary :: Word8
pattern Int64TypeBinary = 0x2
pattern StringTypeBinary :: Word8
pattern StringTypeBinary = 0x3
pattern ByteStringTypeBinary :: Word8
pattern ByteStringTypeBinary = 0x4
pattern RefTypeBinary :: Word8
pattern RefTypeBinary = 0x5
pattern TypeVarBinary :: Word8
pattern TypeVarBinary = 0x6

instance Sendable a => Sendable (GenCastType a) where
  send h Int32Type = hPutWord8 h Int32TypeBinary
  send h Int64Type = hPutWord8 h Int64TypeBinary
  send h TextType = hPutWord8 h StringTypeBinary
  send h ByteStringType = hPutWord8 h ByteStringTypeBinary
  send h (RefType v) = do
    hPutWord8 h RefTypeBinary
    send h v
  send h (TypeVar a) = do
    hPutWord8 h TypeVarBinary
    send h a

instance Receivable a => Receivable (GenCastType a) where
  receive h = do
    cons <- hGetWord8 h
    case cons of
      Int32TypeBinary -> return Int32Type
      Int64TypeBinary -> return Int64Type
      StringTypeBinary -> return TextType
      ByteStringTypeBinary -> return ByteStringType
      RefTypeBinary -> RefType <$> receive h
      TypeVarBinary -> TypeVar <$> receive h
      _ -> error ("receive of GenCastType a: unsupported constructor byte " ++ show cons)

instance Sendable a => Sendable [a] where
  send h arr = do
    sendPosVarInt h (fromIntegral (length arr))
    mapM_ (send h) arr

instance Receivable a => Receivable [a] where
  receive h = do
    len <- fromIntegral <$> receivePosVarInt h
    replicateM len (receive h)

instance (Sendable a, Sendable b) => Sendable (a, b) where
  send h (a,b) = do
    send h a
    send h b

instance (Receivable a, Receivable b) => Receivable (a, b) where
  receive h = do
    a <- receive h
    b <- receive h
    return (a, b)

instance (Sendable a, Sendable b) => Sendable (Either a b) where
  send h (Left a) = do
    hPutWord8 h 0
    send h a
  send h (Right b) = do
    hPutWord8 h 1
    send h b

instance (Receivable a, Receivable b) => Receivable (Either a b) where
  receive h = do
    cons <- hGetWord8 h
    case cons of
      0 -> Left <$> receive h
      1 -> Right <$> receive h
      _ -> error ("receive of Either a b: unsupported constructor byte: " ++ show cons)
