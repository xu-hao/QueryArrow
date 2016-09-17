{-# LANGUAGE DeriveGeneric #-}

module ElasticSearch.QueryResultHits where

import Data.Aeson
import GHC.Generics
import ElasticSearch.Record

data ESQueryResultHits = ESQueryResultHits {
    total :: Int,
    max_score :: Double,
    hits :: [ESHit]
} deriving (Show, Generic)

instance FromJSON ESQueryResultHits
instance ToJSON ESQueryResultHits

data ESHit = ESHit {
_index :: String,
_type :: String,
_score :: Double,
    _id :: String,
    _source :: ESRecord
} deriving (Show, Generic)

instance FromJSON ESHit
instance ToJSON ESHit
