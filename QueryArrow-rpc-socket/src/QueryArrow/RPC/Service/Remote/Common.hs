{-# LANGUAGE OverloadedStrings, TypeFamilies #-}
module QueryArrow.RPC.Service.Remote.Common where

import QueryArrow.DB.DB

import Control.Monad.Trans.Resource
import QueryArrow.Control.Monad.Logger.HSLogger ()
import System.IO (Handle)
import QueryArrow.Syntax.Types
import QueryArrow.Remote.Channel
import QueryArrow.Remote.NoTranslation.Server
import QueryArrow.Remote.NoTranslation.Serialization ()
import QueryArrow.Semantics.ResultRow.VectorResultRow
import QueryArrow.Semantics.ResultValue.AbstractResultValue
import QueryArrow.Semantics.ResultSet.VectorResultSetTransformer
import QueryArrow.Semantics.ResultSet

worker2 :: (IDatabase db, DBFormulaType db ~ FormulaT,
          InputRowType (StatementType (ConnectionType db)) ~ VectorResultRow AbstractResultValue,
          ResultSetRowType (ResultSetType (StatementType (ConnectionType db))) ~ VectorResultRow AbstractResultValue,
          ResultSetTransType (ResultSetType (StatementType (ConnectionType db))) ~ ResultSetTransformer AbstractResultValue) =>
          Handle -> db -> ResourceT IO ()
worker2 handle tdb = runQueryArrowServer (HandleChannel handle) tdb
