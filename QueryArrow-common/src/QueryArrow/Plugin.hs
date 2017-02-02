{-# LANGUAGE MultiParamTypeClasses, GADTs, RankNTypes #-}
module QueryArrow.Plugin where

import QueryArrow.Config
import QueryArrow.DB.DB
import QueryArrow.FO.Data
import QueryArrow.DB.AbstractDatabaseList

class Plugin a row where
  getDB :: a -> ICATDBConnInfo -> AbstractDBList row -> IO (AbstractDatabase row Formula)

data AbstractPlugin row = forall a. (Plugin a row) => AbstractPlugin a
