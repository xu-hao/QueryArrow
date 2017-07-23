{-# LANGUAGE TemplateHaskell, StandaloneDeriving, DeriveGeneric, TypeFamilies, FlexibleInstances #-}
module QueryArrow.Remote.Serialization where

import QueryArrow.Remote.Definitions
import QueryArrow.DB.NoTranslation
import Data.Scientific
import Data.MessagePack
import GHC.Generics
import QueryArrow.Serialization ()
import Foreign.StablePtr
import Foreign.Ptr
import GHC.Fingerprint
import GHC.StaticPtr
import Data.Map.Strict
import QueryArrow.Syntax.Data
import Data.Maybe (fromJust)
import System.IO.Unsafe (unsafePerformIO)
import Data.Binary.Get
import Data.Binary.Put
import Data.ByteString.Lazy

deriving instance Generic Fingerprint

deriving instance Generic a => Generic (NTDBQuery a)

instance MessagePack (Ptr a) where
  fromObject (ObjectBin n) =
    let i = runGet getWord64be (fromStrict n) in return (intPtrToPtr (fromIntegral i))
  toObject sptr =
    ObjectBin (toStrict (runPut (putWord64be (fromIntegral (ptrToIntPtr sptr)))))

instance MessagePack (StablePtr a) where
  fromObject js = castPtrToStablePtr <$> fromObject js
  toObject sptr =
    toObject (castStablePtrToPtr sptr)

instance MessagePack Fingerprint

instance (MessagePack a, Generic a) => MessagePack (NTDBQuery a)

-- instance FromJSON (StaticPtr a) where
--   parseJSON js = do
--     sk <- parseJSON js
--     return (fromJust (unsafePerformIO (unsafeLookupStaticPtr sk)))
--
-- instance ToJSON (StaticPtr a) where
--   toJSON sptr =
--     toJSON (staticKey sptr)
