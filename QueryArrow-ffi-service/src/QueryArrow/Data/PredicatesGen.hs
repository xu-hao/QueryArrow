{-# LANGUAGE TemplateHaskell #-}

module QueryArrow.Data.PredicatesGen where

import FO.Data
import DB.DB
import Translation
import Rewriting
import Config
import Utils
import Data.Namespace.Path
import Data.Namespace.Namespace (lookupObject)
import Data.Maybe (fromMaybe)

import Prelude hiding (lookup)
import Language.Haskell.TH hiding (Pred)
import Language.Haskell.TH.Syntax (VarBangType)
import Data.Char (toLower)
import Data.List (nub)

import QueryArrow.Data.Template

$(structs "test/tdb-plugin.json")
