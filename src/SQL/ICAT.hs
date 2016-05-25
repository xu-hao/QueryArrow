{-# LANGUAGE FlexibleContexts, TemplateHaskell #-}
module SQL.ICAT where

import QueryPlan
import FO.Data
import SQL.SQL
import DBQuery
import ICAT
import SQL.ICATGen

import Data.Map.Strict (empty, fromList, insert)
import qualified Data.Text as T

makeICATSQLDBAdapter :: DBConnection conn  SQLQuery   => conn -> GenericDB conn SQLTrans
makeICATSQLDBAdapter conn = GenericDB conn "ICAT" standardPreds sqlStandardTrans

sqlStandardTrans :: SQLTrans
sqlStandardTrans =
    (SQLTrans
        (fromList schemas)
        (BuiltIn ( fromList [
            ("le", simpleBuildIn "le" (\thesign args ->
                return (swhere (SQLCompCond (case thesign of
                    Pos -> "<="
                    Neg -> ">") (head args) (args !! 1))))),
            ("lt", simpleBuildIn "lt" (\thesign args ->
                return (swhere (SQLCompCond (case thesign of
                    Pos -> "<"
                    Neg -> ">=") (head args) (args !! 1))))),
            ("eq", simpleBuildIn "eq" (\thesign args ->
                return (swhere (SQLCompCond (case thesign of
                    Pos -> "="
                    Neg -> "<>") (head args) (args !! 1))))),
            ("like", simpleBuildIn "like" (\thesign args ->
                return (swhere (SQLCompCond (case thesign of
                    Pos -> "LIKE"
                    Neg -> "NOT LIKE") (head args) (args !! 1))))),
            ("like_regex", simpleBuildIn "like_regex" (\thesign args ->
                return (swhere (SQLCompCond (case thesign of
                    Pos -> "~"
                    Neg -> "!~") (head args) (args !! 1)))))
        ]))
        (fromList mappings))

standardPredMap = foldl (\map1 pred@(Pred name _) -> insert name pred map1) empty standardPreds