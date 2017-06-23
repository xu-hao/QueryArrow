{-# LANGUAGE TypeFamilies, MultiParamTypeClasses, StaticPointers, RankNTypes, GADTs, ConstraintKinds, FlexibleContexts, ScopedTypeVariables, FlexibleInstances #-}

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
  | Supported (Set Var) FormulaT (Set Var)
  | DBOpen
  | DBClose (Ptr ())
  | DBBegin (Ptr ())
  | DBPrepare (Ptr ())
  | DBCommit (Ptr ())
  | DBRollback (Ptr ())
  | DBStmtExec (Ptr ()) (NTDBQuery FormulaT) [MapResultRow]
  | Quit deriving Show

data RemoteResultSet = StringResult String
  | PredListResult [Pred]
  | PredIntListMapResult (Map PredName [Int])
  | BoolResult Bool
  | ErrorResult (Int, String)
  | ConnectionResult (Ptr ())
  | UnitResult
  | RowListResult [MapResultRow] deriving (Show)
