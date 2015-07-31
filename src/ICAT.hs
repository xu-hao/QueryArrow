{-# LANGUAGE DeriveGeneric, FlexibleContexts #-}
module ICAT where

import Data.Aeson
import GHC.Generics
import FO

data ICATDBConnInfo = ICATDBConnInfo {
    db_host :: String,
    db_password :: String,
    db_name :: String,
    catalog_database_type :: String,
    db_port :: Int,
    db_username :: String
} deriving (Show, Generic)

instance FromJSON ICATDBConnInfo
instance ToJSON ICATDBConnInfo

standardPreds :: [Pred]
standardPreds = [
        Pred "DATA_OBJ" (PredType ObjectPred [Key "DataId"]),
        Pred "COLL_OBJ" (PredType ObjectPred [Key "CollId"]),
        Pred "META_OBJ" (PredType ObjectPred [Key "MetaId"]),
        Pred "DATA_NAME" (PredType PropertyPred [Key "DataId", Property "String"]),
        Pred "COLL" (PredType ObjectPred [Key "ObjectId", Key "CollId"]),
        Pred "DATA_REPL_NUM" (PredType PropertyPred [Key "DataId", Property "ReplNum"]),
        Pred "DATA_SIZE" (PredType PropertyPred [Key "DataId", Property "BigInt"]),
        Pred "COLL_NAME" (PredType PropertyPred [Key "CollId", Property "String"]),
        Pred "DATA_PATH" (PredType PropertyPred [Key "DataId", Property "Path"]),
        Pred "DATA_CHECKSUM" (PredType PropertyPred [Key "DataId", Property "Checksum"]),
        Pred "DATA_CREATE_TIME" (PredType PropertyPred [Key "DataId", Property "Time"]),
        Pred "DATA_MODIFY_TIME" (PredType PropertyPred [Key "DataId", Property "Time"]),
        Pred "COLL_CREATE_TIME" (PredType PropertyPred [Key "CollId", Property "Time"]),
        Pred "COLL_MODIFY_TIME" (PredType PropertyPred [Key "CollId", Property "Time"]),
        Pred "META" (PredType ObjectPred [Key "ObjectId", Key "MetaId"]),
        Pred "META_ATTR_NAME" (PredType PropertyPred [Key "MetaId", Key "String"]),
        Pred "META_ATTR_VALUE" (PredType PropertyPred [Key "MetaId", Key "String"]),
        Pred "META_ATTR_UNITS" (PredType PropertyPred [Key "MetaId", Key "String"]),
        Pred "le" (PredType ObjectPred [Key "BigInt", Key "BigInt"]),
        Pred "lt" (PredType ObjectPred [Key "BigInt", Key "BigInt"]),
        Pred "eq" (PredType ObjectPred [Key "Any", Key "Any"]),
        Pred "like" (PredType ObjectPred [Key "String", Key "Pattern"]),
        Pred "like_regex" (PredType ObjectPred [Key "String", Key "Pattern"])]
