{-# LANGUAGE MultiParamTypeClasses, DeriveGeneric, TypeSynonymInstances, FlexibleInstances #-}

module QueryArrow.Include where

import Data.Aeson
import QueryArrow.Plugin
import GHC.Generics
import QueryArrow.Config
import QueryArrow.Semantics.ResultRow.VectorResultRow
import QueryArrow.Semantics.ResultValue.AbstractResultValue

data IncludePlugin = IncludePlugin

newtype IncludePluginConfig = IncludePluginConfig {
    include :: String
} deriving Generic

instance FromJSON IncludePluginConfig

instance ToJSON IncludePluginConfig

instance Plugin IncludePlugin trans (VectorResultRow AbstractResultValue) where
  getDB _ getDB0 ps0 = do
    let ps = getDBSpecificConfig ps0
    ps3 <- getConfig (include ps)
    getDB0 ps3
