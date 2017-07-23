{-# LANGUAGE RankNTypes, GADTs #-}

module QueryArrow.RPC.Service where

import QueryArrow.Syntax.Types
import QueryArrow.Semantics.ResultRow.VectorResultRow
import QueryArrow.Semantics.ResultSet.VectorResultSetTransformer
import QueryArrow.Semantics.ResultValue.AbstractResultValue
import QueryArrow.DB.DB
import Data.Aeson

class RPCService a where
  startService :: a -> AbstractDatabase (ResultSetTransformer AbstractResultValue) (VectorResultRow AbstractResultValue) FormulaT -> Value -> IO ()

data AbstractRPCService = forall a . RPCService a => AbstractRPCService a
