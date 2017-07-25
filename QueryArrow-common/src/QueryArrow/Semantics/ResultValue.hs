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
   fromConcreteResultValue :: ConcreteResultValue -> a

data ConcreteResultValue = StringValue T.Text | Int64Value Int64 | Int32Value Int32 | ByteStringValue ByteString | ConsValue String | AppValue ConcreteResultValue ConcreteResultValue | Null deriving (Eq , Ord, Show, Read, Generic)

pattern ListNilValue :: ConcreteResultValue
pattern ListNilValue = ConsValue "[]"

pattern ListConsValue :: ConcreteResultValue -> ConcreteResultValue -> ConcreteResultValue
pattern ListConsValue a b = ConsValue "(:)" `AppValue` a `AppValue` b

listValue :: [ConcreteResultValue] -> ConcreteResultValue
listValue = foldr ListConsValue ListNilValue

valueListFromValue :: ConcreteResultValue -> [ConcreteResultValue]
valueListFromValue (ListConsValue a b) = let tl = valueListFromValue b in
                                      a:tl
valueListFromValue ListNilValue = []
valueListFromValue a = error ("valueListFromValue: malformatted list " ++ show a)

pattern NullValueBinary :: Word8
pattern NullValueBinary = 0x0

pattern StringValueBinary :: Word8
pattern StringValueBinary = 0x1

pattern ByteStringValueBinary :: Word8
pattern ByteStringValueBinary = 0x2

pattern Int32ValueBinary :: Word8
pattern Int32ValueBinary = 0x3

pattern Int64ValueBinary :: Word8
pattern Int64ValueBinary = 0x4

pattern ListValueBinary :: Word8
pattern ListValueBinary = 0x5

splitByteString :: ByteString -> [ByteString]
splitByteString bs = runGet splitByteString' (BSL.fromStrict bs) where
  splitByteString' = do
    b <- isEmpty
    if b
      then return []
      else do
        len <- getWord64be
        bs <- getByteString (fromIntegral len)
        bss <- splitByteString'
        return (bs : bss)

listValueToByteString :: ConcreteResultValue -> ByteString
listValueToByteString crv =
  let vlist = valueListFromValue crv in
      BSL.toStrict (runPut $ mapM_ (\rv -> do
        let bs = runPut (put rv)
        putWord64be (fromIntegral (BSL.length bs))
        putByteString (BSL.toStrict bs)) vlist)

instance Binary ConcreteResultValue where
  put (StringValue t) = do
    putWord8 StringValueBinary
    let bs = encodeUtf8 t
    putPosVarInt (fromIntegral (BS.length bs))
    putByteString bs
  put (ByteStringValue bs) = do
    putWord8 ByteStringValueBinary
    putPosVarInt (fromIntegral (BS.length bs))
    putByteString bs
  put (Int32Value t) = do
    putWord8 Int32ValueBinary
    putInt32be t
  put (Int64Value t) = do
    putWord8 Int64ValueBinary
    putInt64be t
  put v@ListNilValue = do
    putWord8 StringValueBinary
    putByteString (listValueToByteString v)
  put v@(ListConsValue _ _) = do
    putWord8 StringValueBinary
    putByteString (listValueToByteString v)
  put Null =
    putWord8 NullValueBinary

  get = do
    tyby <- getWord8
    case tyby of
      StringValueBinary -> do
        len <- getPosVarInt
        bs <- getByteString (fromInteger len)
        return (StringValue (decodeUtf8 bs))
      ByteStringValueBinary -> do
        len <- getPosVarInt
        bs <- getByteString (fromInteger len)
        return (ByteStringValue bs)
      ListValueBinary -> do
        len <- getPosVarInt
        bs <- getByteString (fromInteger len)
        return (listValue (map (runGet get . BSL.fromStrict) (splitByteString bs)))
      Int32ValueBinary -> Int32Value <$> getInt32be
      Int64ValueBinary -> Int64Value <$> getInt64be
      NullBinary -> return Null
      _ -> error ("get of ConcreteResultValue: unsupported type byte " ++ show tyby)

instance Sendable ConcreteResultValue where
  send h a = send h (BinaryWrapper a)

instance ResultValue ConcreteResultValue where
  toConcreteResultValue = id
  fromConcreteResultValue = id
  castTypeOf (Int64Value _) = Int64Type
  castTypeOf (Int32Value _) = TextType
  castTypeOf (StringValue _) = TextType
  castTypeOf (ByteStringValue _) = ByteStringType
  castTypeOf ListNilValue = ListType (TypeVar "<list elem>")
  castTypeOf (ListConsValue _ _) = ListType (TypeVar "<list elem>")
  castTypeOf (ConsValue _) = error "typeOf: cons value"
  castTypeOf Null = NullType

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
