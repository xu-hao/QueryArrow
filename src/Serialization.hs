{-# LANGUAGE DeriveGeneric #-}
module Serialization where

import Prelude hiding (lookup)
import Data.Aeson
import GHC.Generics
import Data.Map.Strict (Map, lookup)


data ResultSet = ResultSet {
    headers :: [String],
    results :: [[String]]
} deriving (Generic, Show)

instance FromJSON ResultSet
instance ToJSON ResultSet

resultSet :: [Map String String] -> [String] -> ResultSet
resultSet rows vars =
    ResultSet vars (map (\ row -> map (\var -> case lookup var row of
                                                    Nothing -> "null"
                                                    Just v -> v) vars) rows)
