{-# LANGUAGE RankNTypes, ScopedTypeVariables, TypeApplications, TypeFamilies, DataKinds, GADTs, FlexibleContexts, DeriveGeneric, TypeSynonymInstances, FlexibleInstances, MultiParamTypeClasses #-}
module QueryArrow.Remote.NoTranslation.TCP.TCP where

import QueryArrow.DB.DB
import QueryArrow.DB.NoTranslation
import QueryArrow.Config
import QueryArrow.Remote.NoTranslation.Client
import QueryArrow.Remote.Channel
import QueryArrow.Remote.NoTranslation.Serialization ()
import QueryArrow.Data.Heterogeneous.List
import QueryArrow.DB.AbstractDatabaseList
import QueryArrow.Plugin

import Network
import Data.Aeson
import GHC.Generics
import Data.Maybe

instance FromJSON ICATDBConnInfo2
instance ToJSON ICATDBConnInfo2

data ICATDBConnInfo2 = ICATDBConnInfo2 {
  db_host :: String,
  db_port :: Int
} deriving (Show, Generic)

data RemoteTCPPlugin = RemoteTCPPlugin

instance Plugin RemoteTCPPlugin MapResultRow where
  getDB _ ps  (AbstractDBList HNil) =
    let fsconf0 = fromJSON (fromJust (db_config ps)) in
        case fsconf0 of
          Error err -> error err
          Success fsconf -> do
              h <- connectTo (db_host fsconf) (PortNumber (fromIntegral (db_port fsconf)))
              db <- getQueryArrowClient (HandleChannel h)
              return (AbstractDatabase (NoTranslationDatabase db))
  getDB _ _ _ = error "RemoteTCPPlugin: config error"
