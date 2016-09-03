{-# LANGUAGE FlexibleContexts, MultiParamTypeClasses, FlexibleInstances, OverloadedStrings, GADTs, ExistentialQuantification, TemplateHaskell , ForeignFunctionInterface #-}

module Client.Plugin where

import FO.Data (Pred, Formula(..), Var(..), Expr(..), Atom(..), Aggregator(..), Summary(..), Lit(..), Sign(..))
import DB.DB
import QueryPlan
import DB.ResultStream
import Client.Template

import Prelude hiding (lookup)
import Data.Set (Set, singleton, fromList, empty)
import Data.Text (Text)
import qualified Data.Text as Text
import Control.Monad.Reader
import Control.Applicative (liftA2, pure)
import Control.Monad.Trans.Resource (runResourceT)
import Data.Map.Strict (lookup)
import qualified Data.Map.Strict as Map
import Control.Monad.Trans.Either (EitherT)
import Control.Monad.Error.Class (throwError)
import Control.Monad.IO.Class (liftIO)
import Foreign.StablePtr

$(functions "test/tdb-plugin.json")

foreign export ccall hs_commit :: StablePtr (Session Predicates) -> IO Int
hs_commit :: StablePtr (Session Predicates) -> IO Int
hs_commit sessionptr = do
    (Session _ conn _) <- deRefStablePtr sessionptr
    b <- dbCommit conn
    return (if b then 0 else -1)

foreign export ccall hs_rollback :: StablePtr (Session Predicates) -> IO Int
hs_rollback :: StablePtr (Session Predicates) -> IO Int
hs_rollback sessionptr = do
    (Session _ conn _) <- deRefStablePtr sessionptr
    dbRollback conn
    return 0
