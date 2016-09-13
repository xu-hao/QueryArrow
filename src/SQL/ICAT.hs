{-# LANGUAGE FlexibleContexts #-}
module SQL.ICAT where

import Prelude hiding (lookup)
import DB.GenericDatabase
import FO.Data
import SQL.SQL
import ICAT
import Data.Namespace.Namespace

import Data.Text (unpack)
import Data.Map.Strict (fromList)


makeICATSQLDBAdapter :: String -> [String] -> Maybe String -> a -> IO (GenericDatabase  SQLTrans a)
makeICATSQLDBAdapter ns [predsPath, mappingsPath] nextid conninfo = do
    preds <- loadPreds predsPath
    mappings <- loadMappings mappingsPath
    return (GenericDatabase (sqlStandardTrans ns preds mappings nextid) conninfo ns (qStandardPreds ns preds ++ qStandardBuiltInPreds ns))

loadMappings :: FilePath -> IO [(String, (Table, [SQLQualifiedCol]))]
loadMappings path = do
    content <- readFile path
    return (read content)

sqlMapping :: String -> [Pred] -> [(String, (Table, [SQLQualifiedCol]))] -> PredTableMap
sqlMapping ns preds mappings =
    let sqlStandardPredsMap = qStandardPredsMap ns preds
        lookupPred n = case lookupObject (QPredName ns [] n) sqlStandardPredsMap of
                Nothing -> error ("sqlMapping: cannot find predicate " ++ n)
                Just pred1 -> pred1 in
        fromList (map (\(n, m) -> (lookupPred n, m)) mappings)

sqlStandardTrans :: String -> [Pred] -> [(String, (Table, [SQLQualifiedCol]))] -> Maybe String -> SQLTrans
sqlStandardTrans ns preds mappings nextid =
    let sqlStandardBuiltInPredsMap = qStandardBuiltInPredsMap ns
        lookupPred n = case lookupObject (QPredName ns [] n) sqlStandardBuiltInPredsMap of
                Nothing -> error ("sqlStandardTrans: cannot find predicate " ++ n)
                Just pred1 -> pred1 in

        (SQLTrans
            (BuiltIn ( fromList [
                (lookupPred "le", simpleBuildIn "le" (\thesign args ->
                    return (swhere (SQLCompCond (case thesign of
                        Pos -> "<="
                        Neg -> ">") (head args) (args !! 1))))),
                (lookupPred "lt", simpleBuildIn "lt" (\thesign args ->
                    return (swhere (SQLCompCond (case thesign of
                        Pos -> "<"
                        Neg -> ">=") (head args) (args !! 1))))),
                (lookupPred "eq", simpleBuildIn "eq" (\thesign args ->
                    return (swhere (SQLCompCond (case thesign of
                        Pos -> "="
                        Neg -> "<>") (head args) (args !! 1))))),
                (lookupPred "like", simpleBuildIn "like" (\thesign args ->
                    return (swhere (SQLCompCond (case thesign of
                        Pos -> "LIKE"
                        Neg -> "NOT LIKE") (head args) (args !! 1))))),
                (lookupPred "like_regex", simpleBuildIn "like_regex" (\thesign args ->
                    return (swhere (SQLCompCond (case thesign of
                        Pos -> "~"
                        Neg -> "!~") (head args) (args !! 1))))),
                (lookupPred "in", simpleBuildIn "in" (\thesign args ->
                    let sql = swhere (SQLCompCond "in" (head args) (SQLExprText ("(" ++ (case args !! 1 of
                                                                                              SQLStringConstExpr str -> unpack str
                                                                                              _ -> error "the second argument of in is not a string") ++ ")"))) in
                        case thesign of
                            Pos -> return sql
                            Neg -> return (snot sql))),
                (lookupPred "add", repBuildIn (\ [Left a, Left b, Right v] -> [(v, SQLFuncExpr "add" [a, b])]
                    )),
                (lookupPred "concat", repBuildIn (\ [Left a, Left b, Right v] -> [(v, SQLFuncExpr "concat" [a, b])]
                    )),
                (lookupPred "substr", repBuildIn (\ [Left a, Left b, Left c, Right v] -> [(v, SQLFuncExpr "substr" [a, b, c])]
                    )),
                (lookupPred "replace", repBuildIn (\ [Left a, Left b, Left c, Right v] -> [(v, SQLFuncExpr "replace" [a, b, c])]
                    )),
                (lookupPred "regex_replace", repBuildIn (\ [Left a, Left b, Left c, Right v] -> [(v, SQLFuncExpr "regexp_replace" [a, b, c])]
                    )),
                (lookupPred "cast_int", repBuildIn (\ [Left a, Right v] -> [(v, SQLCastExpr a "integer")]
                    ))
            ]))
            (sqlMapping ns preds mappings) nextid)
