{-# LANGUAGE DeriveGeneric, TemplateHaskell #-}
module QueryArrow.RPC.Config where

import Data.Aeson
import GHC.Generics

data TCPServerConfig = TCPServerConfig {
    tcp_server_addr :: String,
    tcp_server_port :: Int
} deriving (Show, Generic)

data HTTPServerConfig = HTTPServerConfig {
    http_server_port :: Int
} deriving (Show, Generic)

data FileSystemServerConfig = FileSystemServerConfig {
    fs_server_addr :: String,
    fs_server_port :: Int,
    fs_server_root :: String
} deriving (Show, Generic)

data UDSServerConfig = UDSServerConfig {
    uds_server_addr :: String
} deriving (Show, Generic)

instance FromJSON TCPServerConfig
instance ToJSON TCPServerConfig

instance FromJSON HTTPServerConfig
instance ToJSON HTTPServerConfig

instance FromJSON FileSystemServerConfig
instance ToJSON FileSystemServerConfig

instance FromJSON UDSServerConfig
instance ToJSON UDSServerConfig
