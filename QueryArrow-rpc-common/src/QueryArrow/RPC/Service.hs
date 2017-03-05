{-# LANGUAGE RankNTypes, GADTs #-}

module QueryArrow.RPC.Service where

import QueryArrow.FO.Types
import QueryArrow.DB.DB
import Data.Aeson

class RPCService a where
  startService :: a -> AbstractDatabase MapResultRow FormulaT -> Value -> IO ()

data AbstractRPCService = forall a . RPCService a => AbstractRPCService a
