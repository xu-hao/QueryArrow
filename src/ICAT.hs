{-# LANGUAGE DeriveGeneric, FlexibleContexts, TemplateHaskell #-}
module ICAT where

import FO.Data

import Data.Aeson
import GHC.Generics
import ICATGen

data ICATDBConnInfo = ICATDBConnInfo {
    db_host :: String,
    db_password :: String,
    db_name :: String,
    catalog_database_type :: String,
    db_port :: Int,
    db_username :: String
} deriving (Show, Generic)

instance FromJSON ICATDBConnInfo
instance ToJSON ICATDBConnInfo

standardPreds :: [Pred]
standardPreds = preds ++ [
        Pred "le" (PredType EssentialPred [Key "BigInt", Key "BigInt"]),
        Pred "lt" (PredType EssentialPred [Key "BigInt", Key "BigInt"]),
        Pred "eq" (PredType EssentialPred [Key "Any", Key "Any"]),
        Pred "like" (PredType EssentialPred [Key "String", Key "Pattern"]),
        Pred "like_regex" (PredType EssentialPred [Key "String", Key "Pattern"])]
