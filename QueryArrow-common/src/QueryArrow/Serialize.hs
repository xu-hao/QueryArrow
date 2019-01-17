{-# LANGUAGE StandaloneDeriving, FlexibleInstances, DeriveGeneric #-}
module QueryArrow.Serialize where

import System.IO (FilePath)
import QueryArrow.Syntax.Term
import QueryArrow.Semantics.Value
import Data.Aeson
import Data.Yaml
import Data.Namespace.Path
import QueryArrow.Syntax.Type
import QueryArrow.Semantics.TypeChecker
import GHC.Generics

deriving instance Generic Var
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
deriving instance Generic Bind
deriving instance Generic ResultValue
deriving instance Generic CastType
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
instance FromJSON Bind
instance ToJSON Bind
instance FromJSON Var
instance ToJSON Var

loadPreds :: FilePath -> IO [Pred]
loadPreds path = do
    content <- decodeFileEither path
    case content of
        Right preds -> return preds
        Left exception -> error ("error cannot parse " ++ path ++ ", exception: " ++ show exception)

