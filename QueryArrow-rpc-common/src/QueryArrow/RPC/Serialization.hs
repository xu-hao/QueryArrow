{-# LANGUAGE StandaloneDeriving, FlexibleInstances #-}
module QueryArrow.RPC.Serialization where

import Data.MessagePack
import Data.Namespace.Path
import Data.Set (Set, fromList, toAscList)
import Data.Aeson
import Data.Int

import QueryArrow.Syntax.Term
import QueryArrow.Semantics.TypeChecker
import QueryArrow.Semantics.Value
import QueryArrow.Syntax.Type
import QueryArrow.DB.DB
import QueryArrow.Serialization ()
import QueryArrow.RPC.Data
import QueryArrow.RPC.DB

instance MessagePack ResultValue
instance MessagePack CastType

instance (Key a, MessagePack a) => MessagePack (ObjectPath a) where
    fromObject arr = do
      list <- fromObject arr
      case list of
        [] -> error "emtpy object path"
        a : l -> return (ObjectPath (NamespacePath l) a)

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


