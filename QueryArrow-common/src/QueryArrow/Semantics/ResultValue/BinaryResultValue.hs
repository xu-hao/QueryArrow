{-# LANGUAGE TypeFamilies, MultiParamTypeClasses, FlexibleInstances, PatternSynonyms,
   RankNTypes, FlexibleContexts, GADTs, ScopedTypeVariables #-}

module QueryArrow.Semantics.ResultValue.BinaryResultValue where

import Data.ByteString (ByteString)
import qualified Data.ByteString as BS
import Data.ByteString.Lazy (fromStrict, toStrict)
import Data.Binary
import Data.Binary.Get
import Data.Binary.Put
import QueryArrow.Semantics.ResultValue
import QueryArrow.Semantics.Sendable
import QueryArrow.Syntax.Data
import Data.Text.Encoding

data BSType = BSInt32Type | BSInt64Type | BSTextType | BSByteStringType | BSListType | BSNull deriving (Show, Eq)
data BinaryResultValue = BinaryResultValue BSType ByteString deriving (Show, Eq)

instance Binary BinaryResultValue where
  put (BinaryResultValue ty bs) =
    case ty of
      BSInt32Type -> do
        putWord8 Int32ValueBinary
        putByteString bs
      BSInt64Type -> do
        putWord8 Int64ValueBinary
        putByteString bs
      BSTextType -> do
        putWord8 StringValueBinary
        putPosVarInt (fromIntegral (BS.length bs))
        putByteString bs
      BSByteStringType -> do
        putWord8 ByteStringValueBinary
        putPosVarInt (fromIntegral (BS.length bs))
        putByteString bs
      BSListType -> do
        putWord8 ListValueBinary
        putPosVarInt (fromIntegral (BS.length bs))
        putByteString bs
      BSNull ->
        putWord8 NullValueBinary

  get = do
    typebyte <- getWord8
    case typebyte of
      Int32ValueBinary -> BinaryResultValue BSInt32Type <$> getByteString 4
      Int64ValueBinary -> BinaryResultValue BSInt64Type <$> getByteString 8
      StringValueBinary -> do
        len <- getPosVarInt
        bs <- getByteString (fromInteger len)
        return (BinaryResultValue BSTextType bs)
      ByteStringValueBinary -> do
        len <- getPosVarInt
        bs <- getByteString (fromIntegral len)
        return (BinaryResultValue BSTextType bs)
      ListValueBinary -> do
        len <- getPosVarInt
        bs <- getByteString (fromIntegral len)
        return (BinaryResultValue BSListType bs)
      NullValueBinary ->
        return (BinaryResultValue BSNull BS.empty)
      _ -> error ("get of BinaryResultValue: unsupported type byte " ++ show typebyte)

instance Receivable BinaryResultValue where
  receive h = do
    typebyte <- hGetWord8 h
    case typebyte of
      Int32ValueBinary -> BinaryResultValue BSInt32Type <$> BS.hGet h 4
      Int64ValueBinary -> BinaryResultValue BSInt64Type <$> BS.hGet h 8
      StringValueBinary -> do
        len <- receivePosVarInt h
        bs <- BS.hGet h (fromInteger len)
        return (BinaryResultValue BSTextType bs)
      ByteStringValueBinary -> do
        len <- receivePosVarInt h
        bs <- BS.hGet h (fromIntegral len)
        return (BinaryResultValue BSTextType bs)
      ListValueBinary -> do
        len <- receivePosVarInt h
        bs <- BS.hGet h (fromIntegral len)
        return (BinaryResultValue BSListType bs)
      NullValueBinary ->
        return (BinaryResultValue BSNull BS.empty)
      _ -> error ("get of BinaryResultValue: unsupported type byte " ++ show typebyte)
-- instance Binary StringWrapper where
--   get = do
--     len <- getPosVarInt
--     bs <- getByteString (fromInteger len)
--     return (StringWrapper (toString bs))
--   put (StringWrapper str) = do
--     let bs = fromString str
--     putPosVarInt (fromIntegral (BS.length bs))
--     putByteString bs
--
-- instance Binary a => Binary [a] where
--   get = do
--     len <- getPosVarInt
--     replicateM (fromInteger len) get
--   put arr = do
--     putPosVarInt (fromIntegral (length arr))
--     mapM_ put arr
--
-- instance (Binary a, Binary b, Binary c) => Binary (a,b,c) where
--   get = do
--     a <- get
--     b <- get
--     c <- get
--     return (a,b,c)
--   put (a,b,c) = do
--     put a
--     put b
--     put c
instance Sendable BinaryResultValue where
  send h a = send h (BinaryWrapper a)

instance ResultValue BinaryResultValue where
  toConcreteResultValue (BinaryResultValue ty bs) =
    case ty of
      BSInt32Type -> Int32Value (runGet getInt32be (fromStrict bs))
      BSInt64Type -> Int64Value (runGet getInt64be (fromStrict bs))
      BSTextType -> StringValue (decodeUtf8 bs)
      BSByteStringType -> ByteStringValue bs
      BSListType -> listValue (map (runGet get . fromStrict) (splitByteString bs))
      BSNull -> Null
  fromConcreteResultValue crv =
    case castTypeOf crv of
      Int32Type -> BinaryResultValue BSInt32Type (toStrict (runPut (put crv)))
      Int64Type -> BinaryResultValue BSInt64Type (toStrict (runPut (put crv)))
      TextType -> BinaryResultValue BSTextType (toStrict (runPut (put crv)))
      ByteStringType -> BinaryResultValue BSByteStringType (toStrict (runPut (put crv)))
      ListType _ -> BinaryResultValue BSListType (toStrict (runPut (put crv)))
      NullType -> BinaryResultValue BSNull BS.empty

  castTypeOf br = castTypeOf (toConcreteResultValue br)
