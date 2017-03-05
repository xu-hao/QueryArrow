{-# LANGUAGE MultiParamTypeClasses, GADTs, RankNTypes #-}
module QueryArrow.Plugin where

import QueryArrow.Config
import QueryArrow.DB.DB
import QueryArrow.FO.Data
import QueryArrow.FO.Types
import QueryArrow.DB.AbstractDatabaseList
import QueryArrow.Data.Heterogeneous.List

type GetDBFunction row = ICATDBConnInfo -> IO (AbstractDatabase row FormulaT)


getDBs :: GetDBFunction row -> [ICATDBConnInfo] -> IO (AbstractDBList row)
getDBs _ [] = return (AbstractDBList HNil)
getDBs getDB0 (transinfo : l) = do
    db0 <- getDB0 transinfo
    case db0 of
      AbstractDatabase db -> do
        dbs0 <- getDBs getDB0 l
        case dbs0 of
          AbstractDBList dbs -> return (AbstractDBList (HCons db dbs))


class Plugin a row where
  getDB :: a -> GetDBFunction row -> GetDBFunction row

data AbstractPlugin row = forall a. (Plugin a row) => AbstractPlugin a
