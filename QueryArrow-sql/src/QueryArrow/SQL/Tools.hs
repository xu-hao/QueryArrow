module QueryArrow.SQL.Tools where

import QueryArrow.SQL.Parser
import QueryArrow.SQL.Translate
import QueryArrow.Syntax.Term
import QueryArrow.Syntax.Type

import Text.Parsec

import QueryArrow.SQL.SQL hiding (TransMonad)
import QueryArrow.SQL.Mapping
import Data.Namespace.Path
import Data.Foldable

immutable :: NamespacePath String -> [SQLMapping] -> Immutable
immutable namespacePath mapping = Immutable 
    (createSQLTableIndex namespacePath mapping)
    (createSQLPredIndex namespacePath mapping)
    (createSQLColIndex mapping)
    (createSQLSQLOperToPredIndex namespacePath)

parseSQL :: String -> IO SQL
parseSQL s = case parse sqlP "" s of
    Left err -> fail (show err)
    Right sql -> return sql

parseSQLTrans :: Immutable -> String -> IO Formula
parseSQLTrans transinfo s = do
    sql <- parseSQL s
    return (runTrans (translateSQLToQuery sql) transinfo)

parseSQLTransConfig :: [([String], String)] -> String -> IO Formula
parseSQLTransConfig config s = do
    let (namespaces, mappingfiles) = unzip config
    mappings <- mapM loadMappings mappingfiles
    let transinfo = fold (zipWith immutable (map NamespacePath namespaces) mappings)
    parseSQLTrans transinfo s