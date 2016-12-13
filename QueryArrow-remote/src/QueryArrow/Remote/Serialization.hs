{-# LANGUAGE TemplateHaskell, StandaloneDeriving, DeriveGeneric, TypeFamilies, FlexibleInstances #-}
module QueryArrow.Remote.Serialization where

import QueryArrow.Remote.Definitions
import QueryArrow.DB.NoTranslation
import Data.Scientific
import Data.Aeson
import GHC.Generics
import QueryArrow.Serialization ()
import Foreign.StablePtr
import Foreign.Ptr
import GHC.Fingerprint
import GHC.StaticPtr
import Data.Map.Strict
import QueryArrow.FO.Data
import Data.Maybe (fromJust)
import System.IO.Unsafe (unsafePerformIO)

deriving instance Generic Fingerprint

deriving instance Generic a => Generic (NTDBQuery a)

instance FromJSON (Ptr a) where
  parseJSON (Number n) =
    case floatingOrInteger n of
      Left d -> error (show (d :: Double))
      Right i -> return (intPtrToPtr (fromInteger i))
  parseJSON js = error (show js)

instance FromJSON (StablePtr a) where
  parseJSON js = castPtrToStablePtr <$> parseJSON js

instance ToJSON Fingerprint
instance FromJSON Fingerprint

instance (FromJSON a, Generic a) => FromJSON (NTDBQuery a)
instance (ToJSON a, Generic a) => ToJSON (NTDBQuery a)

instance ToJSON (Ptr a) where
  toJSON sptr =
    Number (fromIntegral (ptrToIntPtr sptr))

instance ToJSON (StablePtr a) where
  toJSON sptr =
    toJSON (castStablePtrToPtr sptr)

instance FromJSON (StaticPtr a) where
  parseJSON js = do
    sk <- parseJSON js
    return (fromJust (unsafePerformIO (unsafeLookupStaticPtr sk)))

instance ToJSON (StaticPtr a) where
  toJSON sptr =
    toJSON (staticKey sptr)
