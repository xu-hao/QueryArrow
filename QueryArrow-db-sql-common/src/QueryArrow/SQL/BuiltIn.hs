{-# LANGUAGE FlexibleContexts, OverloadedStrings #-}
module QueryArrow.SQL.BuiltIn where

import QueryArrow.DB.GenericDatabase
import QueryArrow.FO.Data
import QueryArrow.SQL.SQL
import QueryArrow.ICAT
import Data.Namespace.Namespace

import Data.Text (unpack)
import qualified Data.Text as T
import Data.Map.Strict (fromList, keys)
import Text.Read

convertTextToSQLText :: T.Text -> SQLExpr
convertTextToSQLText str = SQLExprText (if str `T.index` 0 /= '(' then if str `T.index` 0 /= '\'' then "(\'" ++ unpack str ++ "\')" else "(" ++ unpack str ++ ")" else unpack str)

sqlBuiltIn :: (String -> PredName) -> BuiltIn
sqlBuiltIn lookupPred =
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
            let sql = swhere (case args !! 1 of
                                     SQLStringConstExpr str | T.length str == 0 || str `T.index` 0 /= '{' -> SQLCompCond "in" (head args) (convertTextToSQLText str)
                                     _ -> SQLCompCond "=" (head args) (SQLFuncExpr "ANY" [args !! 1])) 
            in
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
        (lookupPred "concat", repBuildIn (\ [Left a, Left b, Right v] -> [(v, SQLInfixFuncExpr "||" a b)]
            )),
        (lookupPred "substr", repBuildIn (\ [Left a, Left b, Left c, Right v] -> [(v, SQLFuncExpr "substr" [a, b, c])]
            )),
        (lookupPred "replace", repBuildIn (\ [Left a, Left b, Left c, Right v] -> [(v, SQLFuncExpr "replace" [a, b, c])]
            )),
        (lookupPred "regex_replace", repBuildIn (\ [Left a, Left b, Left c, Right v] -> [(v, SQLFuncExpr "regexp_replace" [a, b, c, SQLStringConstExpr "g"])]
            )),
        (lookupPred "strlen", repBuildIn (\ [Left a, Right v] -> [(v, SQLFuncExpr "length" [a])]
            ))
      ])
