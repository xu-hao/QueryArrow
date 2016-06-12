{-# LANGUAGE DeriveGeneric #-}

module ElasticSearch.Record where

import QueryPlan

import Data.Aeson (parseJSON, toJSON, Object, FromJSON, ToJSON, Value)
import Data.Map.Strict (Map)
import Data.Text (Text)
import Control.Applicative ((<$>))

newtype ESRecord = ESRecord (Map Text Value) deriving Show

instance FromJSON ESRecord where
    parseJSON a = ESRecord <$> parseJSON a

instance ToJSON ESRecord where
    toJSON (ESRecord a) = toJSON a
