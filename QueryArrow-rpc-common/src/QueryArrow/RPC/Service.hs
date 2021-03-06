{-# LANGUAGE RankNTypes, GADTs #-}

module QueryArrow.RPC.Service where

import QueryArrow.Semantics.TypeChecker
import QueryArrow.Syntax.Type
import QueryArrow.DB.DB
import Data.Aeson

class RPCService a where
  startService :: a -> AbstractDatabase MapResultRow FormulaT -> Value -> IO ()

data AbstractRPCService = forall a . RPCService a => AbstractRPCService a
