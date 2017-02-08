{-# LANGUAGE OverloadedStrings, TypeFamilies #-}
module QueryArrow.RPC.Service.Remote.Common where

import QueryArrow.DB.DB

import Control.Monad.Trans.Resource
import QueryArrow.Control.Monad.Logger.HSLogger ()
import System.IO (Handle)
import QueryArrow.FO.Data
import QueryArrow.Remote.Channel
import QueryArrow.Remote.NoTranslation.Server
import QueryArrow.Remote.NoTranslation.Serialization ()

worker2 :: (IDatabase db, DBFormulaType db ~ Formula, RowType (StatementType (ConnectionType db)) ~ MapResultRow) =>
          Handle -> db -> ResourceT IO ()
worker2 handle tdb = runQueryArrowServer (HandleChannel handle) tdb
