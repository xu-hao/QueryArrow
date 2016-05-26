{-# LANGUAGE DeriveGeneric #-}

module ElasticSearch.QueryResult where

import Data.Aeson
import GHC.Generics
import ElasticSearch.QueryResultHits

data ESShards = ESShards {
    total :: Int,
    successful :: Int,
    failed :: Int
} deriving (Show, Generic)

data ESQueryResult = ESQueryResult {
    took :: Int,
    timed_out :: Bool,
    _shards :: ESShards,
    hits :: ESQueryResultHits
} deriving (Show, Generic)

instance FromJSON ESQueryResult
instance ToJSON ESQueryResult

instance FromJSON ESShards
instance ToJSON ESShards
