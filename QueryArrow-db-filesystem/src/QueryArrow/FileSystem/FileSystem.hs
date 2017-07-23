{-# LANGUAGE DeriveGeneric, FlexibleInstances, MultiParamTypeClasses, TypeSynonymInstances, GADTs #-}
module QueryArrow.FileSystem.FileSystem where

import QueryArrow.DB.DB
import QueryArrow.Config
import QueryArrow.Plugin
import QueryArrow.DB.AbstractDatabaseList
import QueryArrow.Data.Heterogeneous.List
import QueryArrow.Semantics.ResultRow.VectorResultRow
import QueryArrow.Semantics.ResultValue.AbstractResultValue
import QueryArrow.Semantics.ResultSet.VectorResultSetTransformer
import QueryArrow.FileSystem.Query
import QueryArrow.FileSystem.ICAT
import QueryArrow.FileSystem.Connection ()

import Data.Aeson
import GHC.Generics
import Data.Maybe

instance FromJSON ICATFSConnInfo
instance ToJSON ICATFSConnInfo

data ICATFSConnInfo = ICATFSConnInfo {
  fs_root :: String,
  fs_host :: String,
  fs_port :: Int,
  fs_hostmap :: [(String, Int, String)],
  db_namespace :: String
} deriving (Show, Generic)


data FileSystemPlugin = FileSystemPlugin

instance Plugin FileSystemPlugin (ResultSetTransformer AbstractResultValue) (VectorResultRow AbstractResultValue) where
  getDB _ _ ps = do
      let conn = FileSystemConnInfo
      let fsconf = getDBSpecificConfig ps
      db <- makeFileSystemDBAdapter (db_namespace fsconf) (fs_root fsconf) (fs_host fsconf) (fs_port fsconf) (fs_hostmap fsconf) conn
      return (AbstractDatabase db)
