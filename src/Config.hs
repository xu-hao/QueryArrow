{-# LANGUAGE DeriveGeneric #-}
module Config where

import Data.Aeson
import GHC.Generics
import FO.Data
import ICAT

import Control.Applicative ((<$>))
import qualified Data.ByteString.Lazy as B
import qualified Data.ByteString.Lazy.Char8 as B8
import Data.Map.Strict

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

data PredInfo = PredInfo {
    pred_name :: String,
    pred_arity :: Int
} deriving (Show, Generic)
data InsMapInfo = InsMapInfo {
    ins_pred_name :: String,
    insert_to :: Int,
    delete_from :: [Int]
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
    add_predicate :: [PredInfo],
    verifier :: VerificationInfo,
    insert_map :: [InsMapInfo],
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

instance FromJSON PredInfo
instance ToJSON PredInfo

instance FromJSON InsMapInfo
instance ToJSON InsMapInfo

getConfig :: FromJSON a => String -> IO a
getConfig filepath = do
    d <- eitherDecode <$> B.readFile filepath
    case d of
            Left err -> error ("getConfig: " ++ filepath ++ " " ++ err)
            Right ps -> return ps
