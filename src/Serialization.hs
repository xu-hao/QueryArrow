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

data QuerySet = QuerySet {
    qsheaders :: [String],
    qsquery :: String,
    qsuser :: String,
    qszone :: String,
    qssession :: String
} | OpenSession | CloseSession {qssession :: String} deriving (Generic, Show)

instance FromJSON ResultSet
instance ToJSON ResultSet
instance FromJSON QuerySet
instance ToJSON QuerySet

resultSet :: [String] -> [Map String String] -> ResultSet
resultSet vars rows =
    ResultSet vars (map (\ row -> map (\var -> case lookup var row of
                                                    Nothing -> "null"
                                                    Just v -> v) vars) rows)
