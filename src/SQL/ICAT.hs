{-# LANGUAGE FlexibleContexts, TemplateHaskell #-}
module SQL.ICAT where

import QueryPlan
import FO.Data
import SQL.SQL
import DBQuery
import ICAT
import SQL.ICATGen

import Data.Map.Strict (empty, fromList, insert, (!))
import qualified Data.Text as T

makeICATSQLDBAdapter :: DBConnection conn  SQLQuery   => conn -> GenericDB conn SQLTrans
makeICATSQLDBAdapter conn = GenericDB conn "ICAT" standardPreds sqlStandardTrans



sqlMapping = fromList (map (\(n, m) -> (standardPredMap ! n, m)) mappings)
sqlStandardTrans :: SQLTrans
sqlStandardTrans =
    (SQLTrans
        (BuiltIn ( fromList [
            (standardBuiltInPredsMap ! "le", simpleBuildIn "le" (\thesign args ->
                return (swhere (SQLCompCond (case thesign of
                    Pos -> "<="
                    Neg -> ">") (head args) (args !! 1))))),
            (standardBuiltInPredsMap ! "lt", simpleBuildIn "lt" (\thesign args ->
                return (swhere (SQLCompCond (case thesign of
                    Pos -> "<"
                    Neg -> ">=") (head args) (args !! 1))))),
            (standardBuiltInPredsMap ! "eq", simpleBuildIn "eq" (\thesign args ->
                return (swhere (SQLCompCond (case thesign of
                    Pos -> "="
                    Neg -> "<>") (head args) (args !! 1))))),
            (standardBuiltInPredsMap ! "like", simpleBuildIn "like" (\thesign args ->
                return (swhere (SQLCompCond (case thesign of
                    Pos -> "LIKE"
                    Neg -> "NOT LIKE") (head args) (args !! 1))))),
            (standardBuiltInPredsMap ! "like_regex", simpleBuildIn "like_regex" (\thesign args ->
                return (swhere (SQLCompCond (case thesign of
                    Pos -> "~"
                    Neg -> "!~") (head args) (args !! 1)))))
        ]))
        sqlMapping)
