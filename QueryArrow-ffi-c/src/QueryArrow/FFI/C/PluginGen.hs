{-# LANGUAGE FlexibleContexts, MultiParamTypeClasses, FlexibleInstances, OverloadedStrings, GADTs, ExistentialQuantification, TemplateHaskell , ForeignFunctionInterface #-}

module QueryArrow.FFI.C.PluginGen where

import FO.Data (Pred, Formula(..), Var(..), Expr(..), Atom(..), Aggregator(..), Summary(..), Lit(..), Sign(..))
import DB.DB
import QueryPlan
import DB.ResultStream
import QueryArrow.FFI.C.Template
import Config
import Utils(constructDBPredMap)

import Prelude hiding (lookup)
import Data.Set (Set, singleton, fromList, empty)
import Data.Text (Text, pack)
import qualified Data.Text as Text
import Data.Monoid ((<>))
import Control.Exception (catch, SomeException)
import Control.Monad.Reader
import Control.Applicative (liftA2, pure)
import Control.Monad.Trans.Resource (runResourceT)
import Data.Map.Strict (lookup)
import qualified Data.Map.Strict as Map
import Control.Monad.Trans.Either (EitherT)
import Control.Monad.Error.Class (throwError)
import Control.Monad.IO.Class (liftIO)
import Foreign.StablePtr
import Foreign.Ptr
import Foreign.C.Types
import Foreign.C.String
import Foreign.Storable
import Foreign.Marshal.Array
import System.Log.Logger (errorM, infoM)
import Logging
import QueryArrow.Data.PredicatesGen

$(functions "../QueryArrow-ffi-service/test/tdb-plugin.json")
