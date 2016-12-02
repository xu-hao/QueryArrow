{-# LANGUAGE TemplateHaskell #-}

module QueryArrow.Data.PredicatesGen where

import QueryArrow.FO.Data
import QueryArrow.DB.DB
import QueryArrow.Translation
import QueryArrow.Rewriting
import QueryArrow.Config
import QueryArrow.Utils
import Data.Maybe (fromMaybe)

import Prelude hiding (lookup)
import Language.Haskell.TH hiding (Pred)
import Language.Haskell.TH.Syntax (VarBangType)
import Data.Char (toLower)
import Data.List (nub)
import Data.Map.Strict (lookup)

import QueryArrow.Data.Template
import QueryArrow.Gen

$(structs configFilePath)
