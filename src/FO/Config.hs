{-# LANGUAGE DeriveGeneric #-}
module FO.Config where

import Data.Aeson
import GHC.Generics
-- config info

data VerificationInfo = VerificationInfo {
    verifier_type :: String,
    verifier_path :: String,
    rule_file_path :: String,
    cpu_limit:: Float,
    memory_limit::Float
} deriving (Show, Generic)

instance FromJSON VerificationInfo
instance ToJSON VerificationInfo
