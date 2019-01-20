{-# LANGUAGE FlexibleContexts #-}
module QueryArrow.SQL.Mapping where

import QueryArrow.DB.GenericDatabase
import QueryArrow.Syntax.Term
import QueryArrow.Syntax.Type
import QueryArrow.SQL.SQL
import QueryArrow.Mapping
import QueryArrow.BuiltIn
import QueryArrow.Serialization
import Data.Namespace.Namespace
import Data.Yaml

import Data.Map.Strict (fromList, keys)
import System.Log.Logger

loadMappings :: FilePath -> IO [SQLMapping]
loadMappings path = do
    content <- decodeFileEither path
    case content of
        Right mappings -> return mappings
        Left exception -> error ("loadMappings: cannot parse file " ++ path ++ ", exception: " ++ show exception)

