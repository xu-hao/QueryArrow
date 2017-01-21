{-# LANGUAGE MultiParamTypeClasses, FlexibleInstances, TypeFamilies, OverloadedStrings #-}

module QueryArrow.FileSystem.Connection where

import Prelude hiding (lookup)
import Data.Map.Strict (insert, lookup)
import Data.Text (unpack, pack)
import Control.Monad.IO.Class (liftIO)
import qualified Data.ByteString.Char8 as BL
import Control.Monad.Free
import System.FilePath((</>))
import Control.Applicative((<$>))
import Control.Exception(throw)
import Control.Monad
import Control.Monad.Trans.State
import Control.Monad.Trans.Class

import QueryArrow.FO.Data
import QueryArrow.DB.DB
import QueryArrow.DB.GenericDatabase
import QueryArrow.DB.NoConnection
import QueryArrow.DB.ResultStream
import QueryArrow.Utils ()
import System.IO
import System.Directory
import QueryArrow.FileSystem.Query
import QueryArrow.FileSystem.Commands

instance INoConnectionDatabase2 (GenericDatabase FileSystemTrans FileSystemConnInfo) where
    type NoConnectionQueryType (GenericDatabase FileSystemTrans FileSystemConnInfo) = FSProgram ()
    type NoConnectionRowType (GenericDatabase FileSystemTrans FileSystemConnInfo) = MapResultRow
    noConnectionDBStmtExec _ qu rs = do
      row <- rs
      fmap snd (runStateT (iterM interpret qu) row)
