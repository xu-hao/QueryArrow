{-# LANGUAGE MultiParamTypeClasses, GADTs, RankNTypes #-}
module QueryArrow.Plugin where

import QueryArrow.Config
import QueryArrow.DB.DB
import QueryArrow.Syntax.Data
import QueryArrow.Syntax.Types
import QueryArrow.DB.AbstractDatabaseList
import QueryArrow.Data.Heterogeneous.List

type GetDBFunction trans row = ICATDBConnInfo -> IO (AbstractDatabase trans row FormulaT)


getDBs :: GetDBFunction trans row -> [ICATDBConnInfo] -> IO (AbstractDBList trans row)
getDBs _ [] = return (AbstractDBList HNil)
getDBs getDB0 (transinfo : l) = do
    db0 <- getDB0 transinfo
    case db0 of
      AbstractDatabase db -> do
        dbs0 <- getDBs getDB0 l
        case dbs0 of
          AbstractDBList dbs -> return (AbstractDBList (HCons db dbs))


class Plugin a trans row where
  getDB :: a -> GetDBFunction trans row -> GetDBFunction trans row

data AbstractPlugin trans row = forall a. (Plugin a trans row) => AbstractPlugin a
