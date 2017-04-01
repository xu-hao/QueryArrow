{-# LANGUAGE TypeFamilies, MultiParamTypeClasses, FlexibleInstances, StandaloneDeriving, DeriveGeneric,
   RankNTypes, FlexibleContexts, GADTs, PatternSynonyms, ScopedTypeVariables #-}

module QueryArrow.Semantics.ResultValue where

import qualified Data.Text as T
import Data.ByteString (ByteString)
import qualified Data.ByteString as BS
import qualified Data.ByteString.Lazy as BSL
import GHC.Generics
import Data.Int (Int64, Int32)
import Data.Binary
import Data.Binary.Get
import Data.Binary.Put
import Data.Text.Encoding
import QueryArrow.Syntax.Data hiding (Serialize(..), (.|.))
import QueryArrow.Semantics.Sendable

-- result value
class (Binary a, Sendable a, Show a, Eq a) => ResultValue a where
   castTypeOf :: a -> CastType
   toConcreteResultValue :: a -> ConcreteResultValue

data ConcreteResultValue = StringValue T.Text | Int64Value Int64 | Int32Value Int32 | ByteStringValue ByteString | RefValue String Location String | Null deriving (Eq , Ord, Show, Read, Generic)

instance Binary ConcreteResultValue where
  put (StringValue t) = do
    putWord8 StringTypeBinary
    let bs = encodeUtf8 t
    putPosVarInt (fromIntegral (BS.length bs))
    putByteString bs
  put (ByteStringValue bs) = do
    putWord8 ByteStringTypeBinary
    putPosVarInt (fromIntegral (BS.length bs))
    putByteString bs
  put (RefValue a b c) = do
    putWord8 RefTypeBinary
    let bs = runPut (do
              put a
              put b
              put c)
    putPosVarInt (fromIntegral (BSL.length bs))
    putLazyByteString bs
  put (Int32Value i) = do
    putWord8 Int32TypeBinary
    putInt32be i
  put (Int64Value i) = do
    putWord8 Int64TypeBinary
    putInt64be i
  put Null =
    putWord8 NullBinary

  get = do
    tyby <- getWord8
    case tyby of
      StringTypeBinary -> do
        len <- getPosVarInt
        bs <- getByteString (fromInteger len)
        return (StringValue (decodeUtf8 bs))
      ByteStringTypeBinary -> do
        len <- getPosVarInt
        bs <- getByteString (fromInteger len)
        return (ByteStringValue bs)
      RefTypeBinary -> do
        _ <- getPosVarInt
        ty <- get
        loc <- get
        path <- get
        return (RefValue ty loc path)
      Int32TypeBinary -> Int32Value <$> getInt32be
      Int64TypeBinary -> Int64Value <$> getInt64be
      NullBinary -> return Null
      _ -> error ("get of ConcreteResultValue: unsupported type byte " ++ show tyby)

instance Sendable ConcreteResultValue where
  send h a = send h (BinaryWrapper a)

instance ResultValue ConcreteResultValue where
  toConcreteResultValue = id
  castTypeOf (Int64Value _) = Int64Type
  castTypeOf (Int32Value _) = Int32Type
  castTypeOf (StringValue _) = TextType
  castTypeOf (ByteStringValue _) = ByteStringType
  castTypeOf (RefValue reftype _ _) = RefType reftype
  castTypeOf Null = error "typeOf: null value"

instance Num ConcreteResultValue where
    Int64Value a + Int64Value b = Int64Value (a + b)
    Int64Value a * Int64Value b = Int64Value (a * b)
    abs (Int64Value a) = Int64Value (abs a)
    signum (Int64Value a) = Int64Value (signum a)
    negate (Int64Value a) = Int64Value (negate a)
    fromInteger i = Int64Value (fromInteger i)

instance Fractional ConcreteResultValue where
    Int64Value a / Int64Value b = Int64Value (round (fromIntegral a / fromIntegral b))
    fromRational a = Int64Value (round (fromRational a))

instance Real ConcreteResultValue where
  toRational (Int64Value a) = fromIntegral a

instance Enum ConcreteResultValue where
  toEnum = Int64Value . fromIntegral
  fromEnum (Int64Value a) = fromIntegral a

instance Integral ConcreteResultValue where
  Int64Value a `quotRem` Int64Value b = let (q, r) = a `quotRem` b in (Int64Value q, Int64Value r)
  toInteger (Int64Value a) = toInteger a
