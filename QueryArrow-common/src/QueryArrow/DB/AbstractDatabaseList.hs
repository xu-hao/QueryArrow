{-# LANGUAGE TypeFamilies, MultiParamTypeClasses, FlexibleInstances, ScopedTypeVariables,
   RankNTypes, FlexibleContexts, GADTs #-}
module QueryArrow.DB.AbstractDatabaseList where

import QueryArrow.DB.DB
import QueryArrow.Semantics.TypeChecker
import QueryArrow.QueryPlan
import QueryArrow.Data.Heterogeneous.List

data AbstractDBList row where
    AbstractDBList :: (HMapC IDatabase l, HMapC (IDatabaseUniformRow row) l, HMapC
                       (IDatabaseUniformDBFormula FormulaT) l, HMapC IDBConnection (HMapF ConnectionType l)) => HList l -> AbstractDBList row
