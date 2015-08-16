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
        Pred "le" (PredType ObjectPred [Key "BigInt", Key "BigInt"]),
        Pred "lt" (PredType ObjectPred [Key "BigInt", Key "BigInt"]),
        Pred "eq" (PredType ObjectPred [Key "Any", Key "Any"]),
        Pred "like" (PredType ObjectPred [Key "String", Key "Pattern"]),
        Pred "like_regex" (PredType ObjectPred [Key "String", Key "Pattern"])]
