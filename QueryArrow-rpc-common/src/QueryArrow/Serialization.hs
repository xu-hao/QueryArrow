{-# LANGUAGE DeriveGeneric, StandaloneDeriving, FlexibleInstances #-}
module QueryArrow.Serialization where

import Data.MessagePack
import GHC.Generics
import Data.Map.Strict (toList, Map)
import qualified Data.Map.Strict as Map
import Data.Namespace.Path
import Data.Set (Set, fromList, toAscList)
import Data.ByteString (ByteString)
import Data.Text.Encoding
import Data.ByteString.Base64 as B64
import Data.Aeson

import QueryArrow.FO.Data
import QueryArrow.DB.DB

data ResultSet = ResultSet {
    errorstr :: String,
    results :: [MapResultRow]
} deriving (Generic, Show)

data QuerySet = QuerySet {
    qsheaders :: Set Var,
    qsquery :: DynCommand,
    qsparams :: MapResultRow
} deriving (Generic, Show)

data DynCommand = Quit | Dynamic String | Static [Command] deriving (Generic, Show)

deriving instance Generic Var
deriving instance Generic ResultValue
deriving instance Generic Command
deriving instance Generic Formula
deriving instance Generic Lit
deriving instance Generic Sign
deriving instance Generic Atom
deriving instance Generic Pred
deriving instance Generic PredType
deriving instance Generic PredKind
deriving instance Generic ParamType
deriving instance Generic Expr
deriving instance Generic CastType
deriving instance Generic Aggregator
deriving instance Generic Summary
deriving instance Show Command

instance (Key a, MessagePack a) => MessagePack (ObjectPath a) where
    fromObject arr = do
      a : l <- fromObject arr
      return (ObjectPath (NamespacePath l) a)

    toObject (ObjectPath (NamespacePath l) a) = toObject (a : l)

instance (Ord a, MessagePack a) => MessagePack (Set a) where
    fromObject = fmap fromList . fromObject
    toObject = toObject . toAscList

instance MessagePack Formula
instance MessagePack Lit
instance MessagePack Sign
instance MessagePack Atom
instance MessagePack Pred
instance MessagePack PredType
instance MessagePack PredKind
instance MessagePack ParamType
instance MessagePack Expr
instance MessagePack CastType
instance MessagePack Aggregator
instance MessagePack Summary
instance MessagePack ResultValue
instance MessagePack Var
instance MessagePack Command
instance MessagePack DynCommand
instance MessagePack ResultSet
instance MessagePack QuerySet

instance (ToJSON a, ToJSON b) => ToJSON (Map b a) where
   toJSON pm = toJSON (toList pm)

instance (FromJSON a, FromJSON b, Ord b) => FromJSON (Map b a) where
   parseJSON js = Map.fromList <$> parseJSON js

instance FromJSON (ObjectPath String) where
    parseJSON str = do
      a : l <- parseJSON str
      return (ObjectPath (NamespacePath l) a)

instance ToJSON (ObjectPath String) where
    toJSON (ObjectPath (NamespacePath l) a) = toJSON (a : l)


instance ToJSON ByteString where
    toJSON bs = toJSON (decodeUtf8 (B64.encode bs))
instance FromJSON ByteString where
    parseJSON str = do
      bs <- parseJSON str
      return (B64.decodeLenient (encodeUtf8 bs))

instance FromJSON Formula
instance ToJSON Formula
instance FromJSON Lit
instance ToJSON Lit
instance FromJSON Sign
instance ToJSON Sign
instance FromJSON Atom
instance ToJSON Atom
instance FromJSON Pred
instance ToJSON Pred
instance FromJSON PredType
instance ToJSON PredType
instance FromJSON PredKind
instance ToJSON PredKind
instance FromJSON ParamType
instance ToJSON ParamType
instance FromJSON Expr
instance ToJSON Expr
instance FromJSON CastType
instance ToJSON CastType
instance FromJSON Aggregator
instance ToJSON Aggregator
instance FromJSON Summary
instance ToJSON Summary
instance FromJSON ResultValue
instance ToJSON ResultValue
instance FromJSON Var
instance ToJSON Var
instance FromJSON Command
instance ToJSON Command
instance FromJSON DynCommand
instance ToJSON DynCommand
instance FromJSON ResultSet
instance ToJSON ResultSet
instance FromJSON QuerySet
instance ToJSON QuerySet

resultSet :: [MapResultRow] -> ResultSet
resultSet rows =
    ResultSet "" rows

errorSet :: String -> ResultSet
errorSet vars =
    ResultSet vars []
