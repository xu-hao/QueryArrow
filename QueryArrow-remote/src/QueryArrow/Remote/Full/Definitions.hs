{-# LANGUAGE TypeFamilies, MultiParamTypeClasses, StaticPointers, RankNTypes, GADTs, ConstraintKinds, FlexibleContexts, TypeApplications, ScopedTypeVariables, FlexibleInstances #-}

module QueryArrow.Remote.Full.Definitions where

import QueryArrow.DB.DB
import QueryArrow.FO.Data
import Data.Set
import Data.Map
import GHC.StaticPtr
import Foreign.StablePtr
import QueryArrow.Remote.Definitions

data RemoteCommand db = GetDB
  | GetName
  | GetPreds
  | DeterminateVars
  | GetSF
  | TranslateQuery (Set Var) (DBFormulaType db) (Set Var)
  | DBOpen
  | DBClose (StablePtr (ConnectionType db))
  | DBBegin (StablePtr (ConnectionType db))
  | DBPrepare (StablePtr (ConnectionType db))
  | DBCommit (StablePtr (ConnectionType db))
  | DBRollback (StablePtr (ConnectionType db))
  | PrepareQuery (StablePtr (ConnectionType db)) (StablePtr (DBQueryType db))
  | DBStmtExec (StablePtr (StatementType (ConnectionType db))) [RowType (StatementType (ConnectionType db))]
  | DBStmtClose (StablePtr (StatementType (ConnectionType db)))
  | Quit

data RemoteResultSet db = DBResult db
  | StringResult String
  | PredListResult [Pred]
  | PredIntListMapResult (Map PredName [Int])
  | SFResult (StaticPtr (Dict (IDatabase0 db) -> db -> DBFormulaType db -> Set Var -> Bool))
  | VarSetResult (Set Var)
  | BoolResult Bool
  | ConnectionResult (StablePtr (ConnectionType db))
  | QueryResult (StablePtr (DBQueryType db))
  | UnitResult
  | StatementResult (StablePtr (StatementType (ConnectionType db)))
  | RowListResult [RowType (StatementType (ConnectionType db))]
