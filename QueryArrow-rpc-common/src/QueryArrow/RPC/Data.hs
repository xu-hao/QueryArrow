{-# LANGUAGE DeriveGeneric, OverloadedStrings, TypeFamilies #-}

module QueryArrow.RPC.Data where

import QueryArrow.DB.ResultStream
import QueryArrow.DB.DB
import QueryArrow.Syntax.Term
import QueryArrow.Semantics.Value
import QueryArrow.Semantics.TypeChecker
import QueryArrow.Utils

import Prelude hiding (lookup, length, map)
import Data.Map.Strict (map)
import Control.Monad.IO.Class (liftIO)
import Control.Monad.Trans.Resource
import Control.Monad.Except
import QueryArrow.Control.Monad.Logger.HSLogger ()
import Control.Monad.Trans.Except
import Control.Exception (SomeException(..))
import System.IO.Error(userError)
import Data.Set (Set)
import Data.Text (Text)
import Data.Conduit
import Text.Parsec
import GHC.Generics
import Debug.Trace

type Error = (Int, Text)

data ResultSet = ResultSetError Error | ResultSetNormal [MapResultRow] deriving (Generic, Show, Read, Eq)

data QuerySet = QuerySet {
    qsheaders :: Set Var,
    qsquery :: DynCommand,
    qsparams :: MapResultRow
} deriving (Generic, Show, Read)

data Command = Begin | Prepare | Commit | Rollback | Execute Formula deriving (Generic, Show, Read)

data DynCommand = Quit | Dynamic String | Static [Command] deriving (Generic, Show, Read)

resultSet :: [MapResultRow] -> ResultSet
resultSet rows =
    ResultSetNormal rows

errorSet :: Error -> ResultSet
errorSet vars =
    ResultSetError vars

