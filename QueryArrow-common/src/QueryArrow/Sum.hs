{-# LANGUAGE TypeFamilies, MultiParamTypeClasses, FunctionalDependencies, ExistentialQuantification, FlexibleInstances, OverloadedStrings, ScopedTypeVariables, UndecidableInstances,
   RankNTypes, FlexibleContexts, GADTs, DeriveGeneric, TypeApplications #-}
module QueryArrow.Sum where

import QueryArrow.DB.DB
import QueryArrow.DB.ResultStream
import QueryArrow.FO.Data
import QueryArrow.QueryPlan
import QueryArrow.Config
import QueryArrow.ListUtils
import QueryArrow.Data.Heterogeneous.List

import Prelude  hiding (lookup)
import Data.Map.Strict (Map, (!), empty, member, insert, foldrWithKey, foldlWithKey, alter, lookup, fromList, toList, unionWith, unionsWith, intersectionWith, elems, delete, singleton, keys, filterWithKey)
import Control.Applicative ((<$>))
import Control.Monad.Trans.Either
import Control.Monad.Trans.Reader
import Control.Monad.Trans.State
import QueryArrow.FO.Types
import System.Log.Logger
import Data.Tree
import Algebra.SemiBoundedLattice
import Data.Set (Set, intersection)
-- exec query from dbname

-- getAllResults2 :: (MonadIO m, MonadBaseControl IO m, IResultRow row, Num (ElemType row), Ord (ElemType row)) => [AbstractDatabase row Formula] -> MSet Var -> Formula -> m [row]
-- getAllResults2 dbs rvars query = do
--     qp <- prepareQuery' dbs rvars query bottom
--     let (_, stream) = execQueryPlan ([], pure mempty) qp
--     getAllResultsInStream stream

queryPlan :: (HMapConstraint IDatabase l ) => HList l -> Formula ->  QueryPlan
queryPlan dbs formula =
    let qp = formulaToQueryPlan dbs formula
        qp1 = simplifyQueryPlan qp in
        qp1

-- queryPlan2 :: AbstractDBList row -> Set Var -> MSet Var -> Formula -> QueryPlan2
-- queryPlan2 dbs vars vars2  formula =
--     let qp = formulaToQueryPlan dbs formula
--         qp1 = simplifyQueryPlan qp
--         qp2 = calculateVars vars vars2 qp1 in
--         optimizeQueryPlan dbs qp2

-- rewriteQuery' :: (MonadIO m) => [InsertRewritingRule] -> [InsertRewritingRule] -> [InsertRewritingRule] -> MSet Var -> Formula -> Set Var -> m Formula
-- rewriteQuery' qr ir dr rvars qu0 vars = do
--     liftIO $ infoM "QA" ("original query: " ++ show qu0)
--     let qu = rewriteQuery  qr ir dr rvars qu0 vars
--     liftIO $ infoM "QA" ("rewritten query: " ++ show qu)
--     return qu
--
translate' :: (HMapConstraint (IDatabaseUniformDBFormula Formula) l, HMapConstraint IDatabase l) => HList l -> MSet Var -> Formula -> Set Var -> IO (QueryPlanT l)
translate' dbs rvars qu0 vars =
    let insp = queryPlan dbs qu0
        qp2 = calculateVars vars rvars insp
        qp3 = optimizeQueryPlan dbs qp2 in
    -- let qp3' = addTransaction' qp3
    -- liftIO $ printQueryPlan qp3
    -- qp4 <- prepareTransaction dbs [] qp3'
        translateQueryPlan dbs qp3

printQueryPlan qp = do
    infoM "QA" ("query plan:")
    infoM "QA" (drawTree (toTree  qp))

instance (IResultRow row, Num (ElemType row), Ord (ElemType row)) => IDBStatement (QueryPlan3 row) where
    type RowType (QueryPlan3 row) = row
    dbStmtClose qp = closeQueryPlan qp
    dbStmtExec qp rs = execQueryPlan rs qp

data SumDB row l where
   SumDB :: String -> HList l -> SumDB row l

instance (HMapConstraint IDatabase l) => IDatabase0 (SumDB row l ) where -- need undecidable instance
    type DBFormulaType (SumDB row l) = Formula
    getName (SumDB name _ ) = name
    getPreds (SumDB _ dbs ) = unions (hMapCUL @IDatabase getPreds dbs)
    supported _ _ _ = True
instance (HMapConstraint IDatabase l, HMapConstraint (IDatabaseUniformDBFormula Formula) l) => IDatabase1 (SumDB row l) where
    type DBQueryType (SumDB row l) = QueryPlanT l
    translateQuery (SumDB _ dbs) retvars form vars = translate' dbs (Include retvars) form vars

instance (IResultRow row, HMapConstraint (IDatabaseUniformRow row) l, HMapConstraint
                      IDBConnection (HMap ConnectionType l)) => IDatabase2 (SumDB row l) where
    data ConnectionType (SumDB row l ) = SumDBConnection (HList' ConnectionType l)
    dbOpen (SumDB _ dbs) = SumDBConnection <$> hMapACULL @(IDatabaseUniformRow row) @ConnectionType dbOpen dbs

instance (IResultRow row, HMapConstraint IDatabase l, HMapConstraint (IDatabaseUniformRow row) l, HMapConstraint
                      (IDatabaseUniformDBFormula Formula) l, HMapConstraint
                      IDBConnection (HMap ConnectionType l)) => IDatabase (SumDB row l)

instance (IResultRow row, HMapConstraint IDBConnection (HMap ConnectionType l)) => IDBConnection0 (ConnectionType (SumDB row l )) where
    dbClose (SumDBConnection dbs  ) = hMapACUL'_ @(IDBConnection) dbClose dbs
    dbBegin (SumDBConnection dbs  ) = hMapACUL'_ @(IDBConnection) dbBegin dbs
    dbCommit (SumDBConnection dbs  ) = hMapACUL'_ @(IDBConnection)  dbCommit dbs
    dbPrepare (SumDBConnection dbs  ) = hMapACUL'_ @(IDBConnection) dbPrepare dbs
    dbRollback (SumDBConnection dbs  ) = hMapACUL'_ @IDBConnection dbRollback dbs

instance (IResultRow row, HMapConstraint (IDatabaseUniformRow row) l, HMapConstraint
                      IDBConnection (HMap ConnectionType l)) => IDBConnection (ConnectionType (SumDB row l )) where
    type QueryType (ConnectionType (SumDB row l )) = QueryPlanT l
    type StatementType (ConnectionType (SumDB row l )) = QueryPlan3 row
    prepareQuery (SumDBConnection conns ) qu =
        prepareQueryPlan conns qu
    -- exec (TransDB _ dbs _ _  _ _ _) qp vars stream = snd (execQueryPlan dbs (vars, stream ) qp)

data AbstractDBList row where
    AbstractDBList :: (HMapConstraint IDatabase l, HMapConstraint (IDatabaseUniformRow row) l, HMapConstraint
                       (IDatabaseUniformDBFormula Formula) l, HMapConstraint IDBConnection (HMap ConnectionType l)) => HList l -> AbstractDBList row
