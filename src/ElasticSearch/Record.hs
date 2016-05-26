{-# LANGUAGE DeriveGeneric #-}

module ElasticSearch.Record where

import Data.Aeson
import GHC.Generics

data ESRecord = ESRecord {
    obj_id :: Int,
    meta_id :: Int,
    attribute :: String,
    value :: String,
    unit :: String
} deriving (Show, Generic)

instance FromJSON ESRecord
instance ToJSON ESRecord
