{-# LANGUAGE FlexibleContexts #-}
module QueryArrow.SQL.Mapping where

import QueryArrow.DB.GenericDatabase
import QueryArrow.Syntax.Term
import QueryArrow.Syntax.Type
import QueryArrow.SQL.SQL
import QueryArrow.SQL.BuiltIn
import QueryArrow.Mapping
import QueryArrow.BuiltIn
import QueryArrow.Mapping
import QueryArrow.Serialize
import Data.Namespace.Namespace
import Data.Yaml

import Data.Map.Strict (fromList, keys)
import System.Log.Logger

nextidPred :: [String] -> String -> Pred
nextidPred nss nextid = Pred (PredName nss nextid) (PredType ObjectPred [ParamType True False True False TextType])

makeICATSQLDBAdapter :: String -> String -> String -> Maybe String -> a -> IO (GenericDatabase SQLTrans a)
makeICATSQLDBAdapter ns predsPath mappingsPath nextid conninfo = do
    debugM "SQL" ("loading predicate from " ++ predsPath)
    preds0 <- loadPreds predsPath
    let preds =
          case nextid of
            Nothing -> preds0
            Just nextid -> nextidPred [] nextid : preds0
    mappings <- loadMappings mappingsPath
    let (BuiltIn builtin) = sqlBuiltIn (lookupPred ns)
    let builtinpreds = keys builtin
    debugM "SQL" ("preds standard = " ++ show (qStandardPreds ns preds))
    debugM "SQL" ("preds builtin = " ++ show (map lookupPredByName builtinpreds))
    return (GenericDatabase (sqlStandardTrans ns preds mappings nextid) conninfo ns (qStandardPreds ns preds ++ map lookupPredByName builtinpreds))

loadMappings :: FilePath -> IO [SQLMapping]
loadMappings path = do
    content <- decodeFileEither path
    case content of
        Right mappings -> return mappings
        Left exception -> error ("loadMappings: cannot parse file " ++ path ++ ", exception: " ++ show exception)

sqlMapping :: String -> [Pred] -> [SQLMapping] -> PredTableMap
sqlMapping ns preds mappings =
    let sqlStandardPredsMap = qStandardPredsMap ns preds
        lookupPred n = case lookupObject (QPredName ns [] n) sqlStandardPredsMap of
                Nothing -> error ("sqlMapping: cannot find predicate " ++ n)
                Just pred1 -> predName pred1 in
        fromList (map (\(SQLMapping n m l) -> (lookupPred n, (m, l))) mappings)

lookupPred :: String -> String -> PredName
lookupPred ns n = predName (lookupPredByName (PredName [ns] n))

lookupPredByName :: PredName -> Pred
lookupPredByName n@(PredName [ns] _) =
              let sqlStandardBuiltInPredsMap = qStandardBuiltInPredsMap ns in
                  case lookupObject n sqlStandardBuiltInPredsMap of
                    Nothing -> error ("sqlStandardTrans: cannot find predicate " ++ show n)
                    Just pred1 -> pred1
sqlStandardTrans :: String -> [Pred] -> [SQLMapping] -> Maybe String -> SQLTrans
sqlStandardTrans ns preds mappings nextid =
  let builtins@(BuiltIn builtinsmap) = (sqlBuiltIn (lookupPred ns))
      ptablemap = (sqlMapping ns preds mappings)
      nextidpred = (nextidPred [ns] <$> nextid)
      total = map (setPredNamespace ns) preds ++ map lookupPredByName (keys builtinsmap) ++ case nextidpred of
                        Nothing -> []
                        Just pred1 -> [pred1] in
        SQLTrans
          builtins ptablemap nextidpred
              (constructPredTypeMap total)
