{-# LANGUAGE TemplateHaskell, StandaloneDeriving, DeriveGeneric, TypeFamilies #-}
module QueryArrow.Remote.NoTranslation.Serialization where

import QueryArrow.Remote.NoTranslation.Definitions
import Data.MessagePack
import GHC.Generics
import QueryArrow.Serialization ()
import QueryArrow.Remote.Serialization ()

deriving instance Generic RemoteCommand
deriving instance Generic RemoteResultSet


instance MessagePack RemoteCommand

instance MessagePack RemoteResultSet
