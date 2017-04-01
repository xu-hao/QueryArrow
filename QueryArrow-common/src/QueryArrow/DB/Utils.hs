{-# LANGUAGE ExistentialQuantification, MultiParamTypeClasses, TypeSynonymInstances, FlexibleInstances, FlexibleContexts, TypeFamilies, ScopedTypeVariables #-}
module QueryArrow.DB.Utils where

import QueryArrow.Semantics.ResultHeader.VectorResultHeader

import Data.Map.Strict (toList)
import QueryArrow.Syntax.Types
import qualified Data.Vector as V




varTypeMapToResultStreamHeader :: VarTypeMap -> ResultHeader
varTypeMapToResultStreamHeader vtm =
    V.fromList (map fst (toList vtm))
