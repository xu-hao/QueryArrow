{-# LANGUAGE TemplateHaskell #-}
module QueryArrow.Config where

import Data.Aeson
import Data.Aeson.TH
import Data.Maybe
import Data.Yaml
import System.FilePath

import qualified Data.ByteString as B
import qualified Data.ByteString.Lazy as BL

-- config info

class DBTree a where
  db_plugins :: a -> [ICATDBConnInfo]

data ICATDBConnInfo = ICATDBConnInfo {
    qap_name :: String,
    catalog_database_type :: String,
    db_config :: Maybe Value
} deriving (Show)

data TranslationInfo = TranslationInfo {
    db_plugin :: ICATDBConnInfo,
    servers :: [DBServer]
} deriving (Show)

data DBServer = DBServer {
  server_protocol :: String,
  server_config :: Maybe Value
} deriving (Show)

$(deriveJSON defaultOptions ''DBServer)
$(deriveJSON defaultOptions ''TranslationInfo)
$(deriveJSON defaultOptions{omitNothingFields = True} ''ICATDBConnInfo)

getConfig :: FromJSON a => String -> IO a
getConfig filepath = do
    let ext = takeExtension filepath
    d <- case ext of
      "json" -> eitherDecode <$> BL.readFile filepath
      _ -> decodeEither <$> B.readFile filepath
    case d of
            Left err -> error ("getConfig: " ++ filepath ++ " " ++ err)
            Right ps -> return ps

getDBSpecificConfig :: FromJSON a => ICATDBConnInfo -> a
getDBSpecificConfig ps =
  case fromJSON (fromJust (db_config ps)) of
     Error err -> error err
     Success fsconf -> fsconf
