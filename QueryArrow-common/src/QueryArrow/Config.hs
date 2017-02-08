{-# LANGUAGE DeriveGeneric, TemplateHaskell #-}
module QueryArrow.Config where

import Data.Aeson
import Data.Aeson.TH
import GHC.Generics

import qualified Data.ByteString.Lazy as B

-- config info

data DBTrans = DBTrans {
    db_info :: ICATDBConnInfo,
    db_plugins :: Maybe [DBTrans]
} deriving (Show)


data ICATDBConnInfo = ICATDBConnInfo {
    qap_name :: String,
    catalog_database_type :: String,
    db_config :: Maybe Value
} deriving (Show)

data TranslationInfo = TranslationInfo {
    db_plugin :: DBTrans,
    servers :: [DBServer]
} deriving (Show, Generic)

data DBServer = DBServer {
  server_protocol :: String,
  server_config :: Maybe Value
} deriving (Show, Generic)

instance FromJSON DBServer
instance ToJSON DBServer

instance FromJSON TranslationInfo
instance ToJSON TranslationInfo

$(deriveJSON defaultOptions{omitNothingFields = True} ''DBTrans)
$(deriveJSON defaultOptions{omitNothingFields = True} ''ICATDBConnInfo)

getConfig :: FromJSON a => String -> IO a
getConfig filepath = do
    d <- eitherDecode <$> B.readFile filepath
    case d of
            Left err -> error ("getConfig: " ++ filepath ++ " " ++ err)
            Right ps -> return ps
