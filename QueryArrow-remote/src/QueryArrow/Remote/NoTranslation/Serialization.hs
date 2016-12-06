{-# LANGUAGE TemplateHaskell, StandaloneDeriving, DeriveGeneric, TypeFamilies #-}
module QueryArrow.Remote.NoTranslation.Serialization where

import QueryArrow.Remote.NoTranslation.Definitions
import Data.Aeson
import GHC.Generics
import QueryArrow.Serialization ()
import QueryArrow.Remote.Serialization ()

deriving instance Generic RemoteCommand
deriving instance Generic RemoteResultSet


instance FromJSON RemoteCommand
instance ToJSON RemoteCommand

instance FromJSON RemoteResultSet
instance ToJSON RemoteResultSet
