{-# LANGUAGE FlexibleContexts #-}
module SQL.ICAT where

import DB.GenericDatabase
import FO.Data
import SQL.SQL
import ICAT
import Data.Namespace.Namespace

import Data.Text (unpack)
import Data.Map.Strict (fromList, keys)
import Text.Read


makeICATSQLDBAdapter :: String -> [String] -> Maybe String -> a -> IO (GenericDatabase  SQLTrans a)
makeICATSQLDBAdapter ns [predsPath, mappingsPath] nextid conninfo = do
    preds0 <- loadPreds predsPath
    let preds =
          case nextid of
            Nothing -> preds0
            Just nextid -> Pred (UQPredName nextid) (PredType ObjectPred [Key "Text"]) : preds0
    mappings <- loadMappings mappingsPath
    let (BuiltIn builtin) = sqlBuiltIn ns
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

sqlBuiltIn :: String -> BuiltIn
sqlBuiltIn ns =
  let sqlStandardBuiltInPredsMap = qStandardBuiltInPredsMap ns
      lookupPred n = case lookupObject (QPredName ns [] n) sqlStandardBuiltInPredsMap of
              Nothing -> error ("sqlStandardTrans: cannot find predicate " ++ n)
              Just pred1 -> pred1 in
      BuiltIn ( fromList [
        (lookupPred "le", simpleBuildIn "le" (\args ->
            return (swhere (SQLCompCond "<=" (head args) (args !! 1))))),
        (lookupPred "lt", simpleBuildIn "lt" (\ args ->
            return (swhere (SQLCompCond "<" (head args) (args !! 1))))),
        (lookupPred "eq", \args -> do
                sqlExprs <- mapM sqlExprFromArg args
                case sqlExprs of
                    [Left a, Left b] ->
                        return (swhere (SQLCompCond "=" a b))
                    [Left a, Right v] -> case a of
                            SQLParamExpr _ ->
                                error "eq: unsupported arguments, the first argument cannot be a param"
                            _ -> do
                                addVarRep v a
                                return mempty
                    _ -> error "eq: unsupported arguments, only the second argument can be unbounded"),
        (lookupPred "ge", simpleBuildIn "le" (\args ->
            return (swhere (SQLCompCond ">=" (head args) (args !! 1))))),
        (lookupPred "gt", simpleBuildIn "lt" (\args ->
            return (swhere (SQLCompCond ">" (head args) (args !! 1))))),
        (lookupPred "ne", \args -> do
                sqlExprs <- mapM sqlExprFromArg args
                case sqlExprs of
                    [Left a, Left b] ->
                        return (swhere (SQLCompCond "<>" a b))
                    _ -> error "ne: unsupported arguments, no argument can be unbounded"),
        (lookupPred "like", simpleBuildIn "like" (\args ->
            return (swhere (SQLCompCond "LIKE" (head args) (args !! 1))))),
        (lookupPred "not_like", simpleBuildIn "not_like" (\args ->
            return (swhere (SQLCompCond "NOT LIKE" (head args) (args !! 1))))),
        (lookupPred "like_regex", simpleBuildIn "like_regex" (\ args ->
            return (swhere (SQLCompCond "~" (head args) (args !! 1))))),
        (lookupPred "not_like_regex", simpleBuildIn "not_like_regex" (\ args ->
            return (swhere (SQLCompCond "!~" (head args) (args !! 1))))),
        (lookupPred "in", simpleBuildIn "in" (\args ->
            let sql = swhere (SQLCompCond "in" (head args) (SQLExprText ("(" ++ (case args !! 1 of
                                                                                      SQLStringConstExpr str -> unpack str
                                                                                      _ -> error "the second argument of in is not a string") ++ ")"))) in
                return sql)),
        (lookupPred "add", repBuildIn (\ [Left a, Left b, Right v] -> [(v, SQLInfixFuncExpr "+" a b)]
            )),
        (lookupPred "sub", repBuildIn (\ [Left a, Left b, Right v] -> [(v, SQLInfixFuncExpr "-" a b)]
            )),
        (lookupPred "mul", repBuildIn (\ [Left a, Left b, Right v] -> [(v, SQLInfixFuncExpr "*" a b)]
            )),
        (lookupPred "div", repBuildIn (\ [Left a, Left b, Right v] -> [(v, SQLInfixFuncExpr "/" a b)]
            )),
        (lookupPred "mod", repBuildIn (\ [Left a, Left b, Right v] -> [(v, SQLInfixFuncExpr "%" a b)]
            )),
        (lookupPred "exp", repBuildIn (\ [Left a, Left b, Right v] -> [(v, SQLInfixFuncExpr "^" a b)]
            )),
        (lookupPred "concat", repBuildIn (\ [Left a, Left b, Right v] -> [(v, SQLFuncExpr "concat" [a, b])]
            )),
        (lookupPred "substr", repBuildIn (\ [Left a, Left b, Left c, Right v] -> [(v, SQLFuncExpr "substr" [a, b, c])]
            )),
        (lookupPred "replace", repBuildIn (\ [Left a, Left b, Left c, Right v] -> [(v, SQLFuncExpr "replace" [a, b, c])]
            )),
        (lookupPred "regex_replace", repBuildIn (\ [Left a, Left b, Left c, Right v] -> [(v, SQLFuncExpr "regexp_replace" [a, b, c])]
            )),
        (lookupPred "strlen", repBuildIn (\ [Left a, Right v] -> [(v, SQLFuncExpr "length" [a])]
            ))
      ])

sqlStandardTrans :: String -> [Pred] -> [(String, (Table, [SQLQualifiedCol]))] -> Maybe String -> SQLTrans
sqlStandardTrans ns preds mappings nextid =
        (SQLTrans
            (sqlBuiltIn ns)
            (sqlMapping ns preds mappings) nextid)
