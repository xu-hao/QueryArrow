{-# LANGUAGE DeriveGeneric, StandaloneDeriving, FlexibleInstances #-}
module QueryArrow.Serialization where

import Data.MessagePack
import GHC.Generics
import Data.Namespace.Path
import Data.Set (Set, fromList, toAscList)
import Data.ByteString (ByteString)
import Data.Aeson
import Data.Int
import qualified Data.ByteString.Base64 as B64
import qualified Data.ByteString.Char8 as B8
import Data.Yaml
import Data.Text (Text)
import Control.Comonad.Cofree

import QueryArrow.FO.Data
import QueryArrow.FO.TypeChecker
import QueryArrow.Semantics.Value
import QueryArrow.Syntax.Type
import QueryArrow.DB.DB

type Error = (Int, Text)

data ResultSet = ResultSetError Error | ResultSetNormal [MapResultRow] deriving (Generic, Show, Read, Eq)

data QuerySet = QuerySet {
    qsheaders :: Set Var,
    qsquery :: DynCommand,
    qsparams :: MapResultRow
} deriving (Generic, Show, Read)

data DynCommand = Quit | Dynamic String | Static [Command] deriving (Generic, Show, Read)

deriving instance Generic Var
deriving instance Generic Command
deriving instance Generic (FormulaF a f)
deriving instance Generic Lit
deriving instance Generic Sign
deriving instance Generic Atom
deriving instance Generic Pred
deriving instance Generic PredType
deriving instance Generic PredKind
deriving instance Generic ParamType
deriving instance Generic (ExprF a)
deriving instance Generic Aggregator
deriving instance Generic Summary
deriving instance Generic ResultValue
deriving instance Generic CastType
deriving instance Show Command
deriving instance Read Command

instance MessagePack ResultValue
instance MessagePack CastType

instance (Key a, MessagePack a) => MessagePack (ObjectPath a) where
    fromObject arr = do
      a : l <- fromObject arr
      return (ObjectPath (NamespacePath l) a)

    toObject (ObjectPath (NamespacePath l) a) = toObject (a : l)

instance (Ord a, MessagePack a) => MessagePack (Set a) where
    fromObject = fmap fromList . fromObject
    toObject = toObject . toAscList


instance MessagePack a => MessagePack (FormulaF () a)
instance MessagePack Formula
instance MessagePack FormulaT
-- (Annotated a (Formula0 Tie)) where
--   toObject (Annotated a form) = ObjectArray [toObject a, toObject form]
--   fromObject (ObjectArray [a, b]) = Annotated <$> fromObject a <*> fromObject b

instance MessagePack Integer where
  toObject i = toObject (fromInteger i :: Int64)
  fromObject a = (fromIntegral :: Int64 -> Integer) <$> fromObject a

instance MessagePack Lit
instance MessagePack Sign
instance MessagePack Atom
instance MessagePack Pred
instance MessagePack PredType
instance MessagePack PredKind
instance MessagePack ParamType
instance MessagePack a => MessagePack (ExprF a)
instance MessagePack Expr
instance MessagePack Aggregator
instance MessagePack Summary
instance MessagePack Var
instance MessagePack Command
instance MessagePack DynCommand
instance MessagePack ResultSet
instance MessagePack QuerySet

instance FromJSON (ObjectPath String) where
    parseJSON str = do
      a : l <- parseJSON str
      return (ObjectPath (NamespacePath l) a)

instance ToJSON (ObjectPath String) where
    toJSON (ObjectPath (NamespacePath l) a) = toJSON (a : l)

instance FromJSONKey Var where

instance ToJSONKey Var where

instance FromJSON a => FromJSON (FormulaF () a)
instance ToJSON a => ToJSON (FormulaF () a)
instance FromJSON Formula
instance ToJSON Formula
instance FromJSON FormulaT
instance ToJSON FormulaT
instance FromJSON Lit
instance ToJSON Lit
instance FromJSON Sign
instance ToJSON Sign
instance FromJSON Atom
instance ToJSON Atom
instance FromJSON Pred
instance ToJSON Pred
instance FromJSON PredType
instance ToJSON PredType
instance FromJSON PredKind
instance ToJSON PredKind
instance FromJSON ParamType
instance ToJSON ParamType
instance FromJSON a => FromJSON (ExprF a)
instance ToJSON a => ToJSON (ExprF a)
instance FromJSON Expr
instance ToJSON Expr
instance FromJSON CastType
instance ToJSON CastType
instance FromJSON Aggregator
instance ToJSON Aggregator
instance FromJSON Summary
instance ToJSON Summary
instance FromJSON Var
instance ToJSON Var
instance FromJSON Command
instance ToJSON Command
instance FromJSON DynCommand
instance ToJSON DynCommand
instance FromJSON ResultSet
instance ToJSON ResultSet
instance FromJSON QuerySet
instance ToJSON QuerySet

instance FromJSON ResultValue
instance ToJSON ResultValue

instance FromJSON ByteString where
  parseJSON a = do
    bs <- parseJSON a
    case B64.decode (B8.pack bs) of
      Left err -> error err
      Right bs -> return bs

instance ToJSON ByteString where
  toJSON a = toJSON (B8.unpack (B64.encode a))

resultSet :: [MapResultRow] -> ResultSet
resultSet rows =
    ResultSetNormal rows

errorSet :: Error -> ResultSet
errorSet vars =
    ResultSetError vars

loadPreds :: FilePath -> IO [Pred]
loadPreds path = do
    content <- decodeFileEither path
    case content of
        Right preds -> return preds
        Left exception -> error ("error cannot parse " ++ path ++ ", exception: " ++ show exception)
