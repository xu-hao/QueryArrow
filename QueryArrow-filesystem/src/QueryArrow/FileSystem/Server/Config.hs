{-# LANGUAGE DeriveGeneric #-}
module QueryArrow.FileSystem.Server.Config where

import GHC.Generics
import Data.Aeson

data FSServerInfo = TranslationInfo {
    fs_server_addr :: String,
    fs_server_port :: Int,
    fs_server_protocols :: [String],
    fs_server_root :: String
} deriving (Show, Generic)

instance FromJSON FSServerInfo
instance ToJSON FSServerInfo
