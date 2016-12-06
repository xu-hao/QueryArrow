{-# LANGUAGE FlexibleContexts #-}
module QueryArrow.SQL.ICAT where

import QueryArrow.DB.GenericDatabase
import QueryArrow.FO.Data
import QueryArrow.SQL.SQL
import QueryArrow.SQL.BuiltIn
import QueryArrow.ICAT
import QueryArrow.BuiltIn
import Data.Namespace.Namespace

import Data.Text (unpack)
import Data.Map.Strict (fromList, keys)
import Text.Read

nextidPred :: String -> Pred
nextidPred nextid = Pred (UQPredName nextid) (PredType ObjectPred [Key "Text"])

makeICATSQLDBAdapter :: String -> [String] -> Maybe String -> a -> IO (GenericDatabase  SQLTrans a)
makeICATSQLDBAdapter ns [predsPath, mappingsPath] nextid conninfo = do
    preds0 <- loadPreds predsPath
    let preds =
          case nextid of
            Nothing -> preds0
            Just nextid -> nextidPred nextid : preds0
    mappings <- loadMappings mappingsPath
    let (BuiltIn builtin) = sqlBuiltIn (lookupPred ns)
    let builtinpreds = keys builtin
    return (GenericDatabase (sqlStandardTrans ns preds mappings nextid) conninfo ns (qStandardPreds ns preds ++ builtinpreds))

loadMappings :: FilePath -> IO [(String, (Table, [SQLQualifiedCol]))]
loadMappings path = do
    content <- readFile path
    case readMaybe content of
        Just mappings -> return mappings
        Nothing -> error ("loadMappings: cannot parse file " ++ path)

sqlMapping :: String -> [Pred] -> [(String, (Table, [SQLQualifiedCol]))] -> PredTableMap
sqlMapping ns preds mappings =
    let sqlStandardPredsMap = qStandardPredsMap ns preds
        lookupPred n = case lookupObject (QPredName ns [] n) sqlStandardPredsMap of
                Nothing -> error ("sqlMapping: cannot find predicate " ++ n)
                Just pred1 -> pred1 in
        fromList (map (\(n, m) -> (lookupPred n, m)) mappings)

lookupPred :: String -> String -> Pred
lookupPred ns n =
              let sqlStandardBuiltInPredsMap = qStandardBuiltInPredsMap ns in
                  case lookupObject (QPredName ns [] n) sqlStandardBuiltInPredsMap of
                    Nothing -> error ("sqlStandardTrans: cannot find predicate " ++ n)
                    Just pred1 -> pred1

sqlStandardTrans :: String -> [Pred] -> [(String, (Table, [SQLQualifiedCol]))] -> Maybe String -> SQLTrans
sqlStandardTrans ns preds mappings nextid =
        (SQLTrans
            (sqlBuiltIn (lookupPred ns))
            (sqlMapping ns preds mappings) (nextidPred <$> nextid))
