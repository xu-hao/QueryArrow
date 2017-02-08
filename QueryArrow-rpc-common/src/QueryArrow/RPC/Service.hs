{-# LANGUAGE RankNTypes, GADTs #-}

module QueryArrow.RPC.Service where

import QueryArrow.FO.Data
import QueryArrow.DB.DB
import Data.Aeson

class RPCService a where
  startService :: a -> AbstractDatabase MapResultRow Formula -> Value -> IO ()

data AbstractRPCService = forall a . RPCService a => AbstractRPCService a
