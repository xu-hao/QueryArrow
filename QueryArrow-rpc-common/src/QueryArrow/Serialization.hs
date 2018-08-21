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

import QueryArrow.Syntax.Term
import QueryArrow.Semantics.TypeChecker
import QueryArrow.Semantics.Value
import QueryArrow.Syntax.Type
import QueryArrow.DB.DB
import QueryArrow.Serialize

type Error = (Int, Text)

data ResultSet = ResultSetError Error | ResultSetNormal [MapResultRow] deriving (Generic, Show, Read, Eq)

data QuerySet = QuerySet {
    qsheaders :: Set Var,
    qsquery :: DynCommand,
    qsparams :: MapResultRow
} deriving (Generic, Show, Read)

data DynCommand = Quit | Dynamic String | Static [Command] deriving (Generic, Show, Read)




deriving instance Generic Command






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
instance MessagePack Bind
instance MessagePack Var
instance MessagePack Command
instance MessagePack DynCommand
instance MessagePack ResultSet
instance MessagePack QuerySet


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

