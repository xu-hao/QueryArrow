{-# LANGUAGE FlexibleInstances, TypeFamilies #-}

module QueryArrow.FileSystem.Connection where

import Control.Monad.Free
import Control.Monad.Trans.State

import QueryArrow.DB.DB
import QueryArrow.DB.GenericDatabase
import QueryArrow.DB.NoConnection
import QueryArrow.FileSystem.Query
import QueryArrow.FileSystem.Commands

instance INoConnectionDatabase2 (GenericDatabase FileSystemTrans FileSystemConnInfo) where
    type NoConnectionQueryType (GenericDatabase FileSystemTrans FileSystemConnInfo) = FSProgram ()
    type NoConnectionRowType (GenericDatabase FileSystemTrans FileSystemConnInfo) = MapResultRow
    noConnectionDBStmtExec _ qu rs = do
      row <- rs
      fmap snd (runStateT (iterM interpret qu) row)
