{-# LANGUAGE TypeFamilies, MultiParamTypeClasses, FlexibleInstances, ScopedTypeVariables,
   RankNTypes, FlexibleContexts, GADTs, TypeApplications #-}
module QueryArrow.DB.AbstractDatabaseList where

import QueryArrow.DB.DB
import QueryArrow.FO.Data
import QueryArrow.QueryPlan
import QueryArrow.Data.Heterogeneous.List

data AbstractDBList row where
    AbstractDBList :: (HMapConstraint IDatabase l, HMapConstraint (IDatabaseUniformRow row) l, HMapConstraint
                       (IDatabaseUniformDBFormula Formula) l, HMapConstraint IDBConnection (HMap ConnectionType l)) => HList l -> AbstractDBList row
