{-# LANGUAGE TypeFamilies, MultiParamTypeClasses, StaticPointers, RankNTypes, GADTs, ConstraintKinds, FlexibleContexts, TypeApplications, ScopedTypeVariables, FlexibleInstances #-}

module QueryArrow.Remote.NoTranslation.Definitions where

import QueryArrow.DB.DB
import QueryArrow.DB.NoTranslation
import QueryArrow.FO.Data
import Data.Set
import Foreign.Ptr
import Data.Map.Strict
import QueryArrow.FO.Types

data RemoteCommand  = GetName
  | GetPreds
  | Supported (Set Var) Formula (Set Var)
  | DBOpen
  | DBClose (Ptr ())
  | DBBegin (Ptr ())
  | DBPrepare (Ptr ())
  | DBCommit (Ptr ())
  | DBRollback (Ptr ())
  | DBStmtExec (Ptr ()) (NTDBQuery Formula) [MapResultRow]
  | CheckQuery VarTypeMap Formula VarTypeMap
  | Quit

data RemoteResultSet = StringResult String
  | PredListResult [Pred]
  | PredIntListMapResult (Map PredName [Int])
  | BoolResult Bool
  | ErrorResult (Int, String)
  | ConnectionResult (Ptr ())
  | UnitResult
  | RowListResult [MapResultRow] deriving (Show)
