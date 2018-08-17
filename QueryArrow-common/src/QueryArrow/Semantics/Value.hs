{-# LANGUAGE PatternSynonyms #-} 

module QueryArrow.Semantics.Value where

import Data.Int
import Data.ByteString (ByteString)
import qualified Data.Text as T
import QueryArrow.Syntax.Type


-- result value
  
data ResultValue = StringValue T.Text | Int64Value Int64 | Int32Value Int32 | ByteStringValue ByteString | ConsValue String | AppValue ResultValue ResultValue | Null deriving (Eq , Ord, Show, Read)
  
pattern ListNilValue :: ResultValue
pattern ListNilValue = ConsValue "[]"
  
pattern ListConsValue :: ResultValue -> ResultValue -> ResultValue
pattern ListConsValue a b = ConsValue "(:)" `AppValue` a `AppValue` b

listValue :: [ResultValue] -> ResultValue
listValue = foldr ListConsValue ListNilValue
  
valueListFromValue :: ResultValue -> [ResultValue]
valueListFromValue (ListConsValue a b) = let tl = valueListFromValue b in
                                        a:tl
valueListFromValue ListNilValue = []
valueListFromValue a = error ("valueListFromValue: malformatted list " ++ show a)

castTypeOf :: ResultValue -> CastType
castTypeOf (Int64Value _) = Int64Type
castTypeOf (Int32Value _) = TextType
castTypeOf (StringValue _) = TextType
castTypeOf (ByteStringValue _) = ByteStringType
castTypeOf ListNilValue = ListType AnyType
castTypeOf (ListConsValue _ _) = ListType AnyType
castTypeOf (ConsValue _) = error "typeOf: cons value"
castTypeOf Null = error "typeOf: null value"
  
instance Num ResultValue where
    Int64Value a + Int64Value b = Int64Value (a + b)
    Int64Value a * Int64Value b = Int64Value (a * b)
    abs (Int64Value a) = Int64Value (abs a)
    signum (Int64Value a) = Int64Value (signum a)
    negate (Int64Value a) = Int64Value (negate a)
    fromInteger i = Int64Value (fromInteger i)
  
instance Fractional ResultValue where
    Int64Value a / Int64Value b = Int64Value (round (fromIntegral a / fromIntegral b))
    fromRational a = Int64Value (round (fromRational a))
  
instance Real ResultValue where
    toRational (Int64Value a) = fromIntegral a
  
instance Enum ResultValue where
    toEnum = Int64Value . fromIntegral
    fromEnum (Int64Value a) = fromIntegral a
  
instance Integral ResultValue where
    Int64Value a `quotRem` Int64Value b = let (q, r) = a `quotRem` b in (Int64Value q, Int64Value r)
    toInteger (Int64Value a) = toInteger a

{-  deriving instance Generic CastType
  instance MessagePack CastType
  instance MessagePack NetworkResultValue
  
  instance MessagePack AbstractResultValue where
    fromObject a = do
      b <- fromObject a
      return (AbstractResultValue (b :: NetworkResultValue))
    toObject (AbstractResultValue a) = toObject (toNetworkResultValue a)
  
  splitByteString :: ByteString -> [ByteString]
  splitByteString bs = runGet splitByteString' (fromStrict bs) where
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
        toStrict (runPut $ mapM_ (\rv -> do
          let nrv = toNetworkResultValue rv
          let bs = pack nrv
          putWord64be (fromIntegral (BSL.length bs))
          putByteString (toStrict bs)) vlist)
  
  instance ResultValue NetworkResultValue where
    toConcreteResultValue (NetworkResultValue ty bs) =
      case ty of
        Just Int64Type -> Int64Value (fromIntegral (runGet getWord64be (fromStrict bs)))
        Just Int32Type -> Int32Value (fromIntegral (runGet getWord32be (fromStrict bs)))
        Just TextType -> StringValue (decodeUtf8 bs)
        Just ByteStringType -> ByteStringValue bs
        Just (ListType _) -> listValue (map ((toConcreteResultValue :: NetworkResultValue -> ConcreteResultValue) . fromMaybe (error "toConcreteResultValue: cannot unpack bytestring") . unpack . fromStrict) (splitByteString bs))
        Nothing -> Null
        _ -> error ("toConcreteResultValue: unsupported network value type: " ++ show ty)
    toNetworkResultValue = id
    castTypeOf (NetworkResultValue (Just ty) _) = ty
    castTypeOf _ = error "typeOf: null value"
-}

{-    toNetworkResultValue (Int64Value i) = NetworkResultValue (Just Int64Type) (toStrict (runPut (putWord64be (fromIntegral i))))
    toNetworkResultValue (Int32Value i) = NetworkResultValue (Just Int32Type) (toStrict (runPut (putWord32be (fromIntegral i))))
    toNetworkResultValue (StringValue s) = NetworkResultValue (Just TextType) (encodeUtf8 s)
    toNetworkResultValue (ByteStringValue bs) = NetworkResultValue (Just ByteStringType) bs
    toNetworkResultValue (Null) = NetworkResultValue Nothing BS.empty
    toNetworkResultValue crv@NilValue = NetworkResultValue (Just (ListType (TypeVar "<list elem>"))) (listValueToByteString crv)
    toNetworkResultValue crv@(ListConsValue _ _) = NetworkResultValue (Just (ListType (TypeVar "<list elem>"))) (listValueToByteString crv)
    toNetworkResultValue (ConsValue _) = error ("cannot convert cons value to network result value") -}

  