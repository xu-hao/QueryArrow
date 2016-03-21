{-# LANGUAGE DeriveGeneric #-}
module Config where

import Data.Aeson
import GHC.Generics
import FO.Data
import ICAT

import Control.Applicative ((<$>))
import qualified Data.ByteString.Lazy as B

-- config info
data VerificationInfo = VerificationInfo {
    verifier_type :: String,
    verifier_path :: String,
    rule_file_path :: String,
    cpu_limit:: Float,
    memory_limit::Float
} deriving (Show, Generic)

data DBTrans = DBTrans {
    db_info :: ICATDBConnInfo
} deriving (Show, Generic)


data ICATDBConnInfo = ICATDBConnInfo {
    db_host :: String,
    db_password :: String,
    db_name :: String,
    catalog_database_type :: String,
    db_port :: Int,
    db_username :: String
} deriving (Show, Generic)

data TranslationInfo = TranslationInfo {
    db_plugins :: [DBTrans],
    hide_predicate :: [String],
    add_predicate :: [String],
    verifier :: VerificationInfo,
    rewriting_file_path :: String
} deriving (Show, Generic)

instance FromJSON ICATDBConnInfo
instance ToJSON ICATDBConnInfo

instance FromJSON VerificationInfo
instance ToJSON VerificationInfo

instance FromJSON TranslationInfo
instance ToJSON TranslationInfo

instance FromJSON DBTrans
instance ToJSON DBTrans


getConfig :: FromJSON a => String -> IO a
getConfig filepath = do
    d <- eitherDecode <$> B.readFile filepath
    case d of
            Left err -> error ("getConfig: " ++ filepath ++ " " ++ err)
            Right ps -> return ps
