{-# LANGUAGE FlexibleContexts, TemplateHaskell #-}
module SQL.ICAT where

import Prelude hiding (lookup)
import QueryPlan
import FO.Data
import SQL.SQL
import DBQuery
import ICAT
import SQL.ICATGen
import Data.Namespace.Namespace

import Data.Map.Strict (empty, fromList, insert, lookup)
import qualified Data.Text as T


makeICATSQLDBAdapter :: DBConnection conn   => String -> Maybe String -> conn -> GenericDB conn SQLTrans
makeICATSQLDBAdapter ns nextid conn = GenericDB conn ns (qStandardPreds ns ++ qStandardBuiltInPreds ns) (sqlStandardTrans ns nextid)




sqlMapping :: String -> PredTableMap
sqlMapping ns =
    let sqlStandardPredsMap = qStandardPredsMap ns
        lookupPred n = case lookupObject (QPredName ns [] n) sqlStandardPredsMap of
                Nothing -> error ("sqlMapping: cannot find predicate " ++ n)
                Just pred1 -> pred1 in
        fromList (map (\(n, m) -> (lookupPred n, m)) mappings)

sqlStandardTrans :: String -> Maybe String -> SQLTrans
sqlStandardTrans ns nextid =
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
                        Neg -> "!~") (head args) (args !! 1)))))
            ]))
            (sqlMapping ns) nextid)
