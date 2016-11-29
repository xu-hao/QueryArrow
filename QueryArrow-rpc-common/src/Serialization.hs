{-# LANGUAGE DeriveGeneric, StandaloneDeriving, FlexibleInstances #-}
module Serialization where

import Prelude hiding (lookup)
import Data.Aeson
import GHC.Generics
import Data.Map.Strict (toList, fromList)
import Data.Namespace.Path
import Data.Set (Set)

import FO.Data
import DB.DB

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

instance FromJSON MapResultRow where
  parseJSON str = fromList <$> (parseJSON str)

instance ToJSON MapResultRow where
  toJSON row = toJSON (toList row)

instance FromJSON (ObjectPath String) where
    parseJSON str = do
      a : l <- parseJSON str
      return (ObjectPath (NamespacePath l) a)

instance ToJSON (ObjectPath String) where
    toJSON (ObjectPath (NamespacePath l) a) = toJSON (a : l)

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
