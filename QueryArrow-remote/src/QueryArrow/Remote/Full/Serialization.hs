{-# LANGUAGE TemplateHaskell, StandaloneDeriving, DeriveGeneric, TypeFamilies #-}
module QueryArrow.Remote.Full.Serialization where

import QueryArrow.Remote.Full.Definitions
import QueryArrow.Translation
import Data.Aeson
import GHC.Generics
import QueryArrow.FO.Data
import QueryArrow.DB.DB
import QueryArrow.Serialization ()
import QueryArrow.Remote.Serialization ()

deriving instance Generic (RemoteCommand a)
deriving instance Generic (RemoteResultSet a)

instance (DBFormulaType a ~ Formula, RowType (StatementType (ConnectionType a)) ~ MapResultRow) => FromJSON (RemoteCommand a)
instance (DBFormulaType a ~ Formula, RowType (StatementType (ConnectionType a)) ~ MapResultRow) => ToJSON (RemoteCommand a)

instance (FromJSON a, DBFormulaType a ~ Formula, RowType (StatementType (ConnectionType a)) ~ MapResultRow) => FromJSON (RemoteResultSet a)
instance (ToJSON a, DBFormulaType a ~ Formula, RowType (StatementType (ConnectionType a)) ~ MapResultRow) => ToJSON (RemoteResultSet a)
