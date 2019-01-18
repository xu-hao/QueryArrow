{-# LANGUAGE FlexibleInstances, TypeFamilies #-}

module QueryArrow.FileSystem.Connection where

import Control.Monad.Free
import Control.Monad.Trans.State
import Control.Monad.Trans.Reader

import QueryArrow.DB.DB
import QueryArrow.DB.GenericDatabase
import QueryArrow.DB.NoConnection
import QueryArrow.FileSystem.Query
import QueryArrow.FileSystem.Commands

instance INoConnectionDatabase2 (GenericDatabase FileSystemTrans FileSystemConn) where
    type NoConnectionQueryType (GenericDatabase FileSystemTrans FileSystemConn) = FSProgram ()
    type NoConnectionRowType (GenericDatabase FileSystemTrans FileSystemConn) = MapResultRow
    noConnectionDBStmtExec (GenericDatabase _ (FileSystemConn hostmap hostmap2) _ _) qu rs = do
      row <- rs
      runReaderT (execStateT (iterM interpret qu) row) (hostmap, hostmap2)
