{-# LANGUAGE MultiParamTypeClasses, DeriveGeneric, TypeSynonymInstances, FlexibleInstances #-}

module QueryArrow.DB.Include where

import Data.Aeson
import QueryArrow.Plugin
import GHC.Generics
import QueryArrow.DB.DB
import QueryArrow.Config

data IncludePlugin = IncludePlugin

newtype IncludePluginConfig = IncludePluginConfig {
    include :: String
} deriving Generic

instance FromJSON IncludePluginConfig

instance ToJSON IncludePluginConfig

instance Plugin IncludePlugin MapResultRow where
  getDB _ getDB0 ps0 = do
    let ps = getDBSpecificConfig ps0
    ps3 <- getConfig (include ps)
    getDB0 ps3
