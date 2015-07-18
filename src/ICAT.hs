{-# LANGUAGE MonadComprehensions, FlexibleContexts #-}
module ICAT where

import FO
import Parser
import SQL.SQL
import SQL.HDBC
import SQL.HDBC.Sqlite3
import SQL.HDBC.PostgreSQL
import DBQuery

import Data.Map.Strict (empty, fromList)
import Text.Parsec (runParser)
import Data.Functor.Identity
import Control.Monad.Trans.State.Strict (evalStateT)
import Database.HDBC

makeICATSQLDBAdapter :: DBConnection conn stmt SQLQuery SQLExpr => conn -> SQLDBAdapter connInfo conn stmt 
makeICATSQLDBAdapter conn = SQLDBAdapter {
    sqlDBConn = conn,
    sqlDBName = "ICAT",
    sqlDBPreds = [
        Pred "DATA_NAME" ["ObjectId", "String"],
        Pred "DATA_COLL_ID" ["ObjectId", "CollId"],
        Pred "DATA_REPL_NUM" ["ObjectId", "ReplNum"],
        Pred "DATA_SIZE" ["ObjectId", "BigInt"],
        Pred "COLL_NAME" ["ObjectId", "String"],
        Pred "DATA_PATH" ["ObjectId", "Path"],
        Pred "DATA_COLL_NAME" ["ObjectId", "String"],
        Pred "DATA_CHECKSUM" ["ObjectId", "Checksum"],
        Pred "DATA_CREATE_TIME" ["ObjectId", "Time"],
        Pred "DATA_MODIFY_TIME" ["ObjectId", "Time"],
        Pred "COLL_CREATE_TIME" ["CollId", "Time"],
        Pred "COLL_MODIFY_TIME" ["CollId", "Time"],
        Pred "le" ["BigInt", "BigInt"],
        Pred "lt" ["BigInt", "BigInt"],
        Pred "eq" ["Any", "Any"],
        Pred "like" ["String", "Pattern" ],
        Pred "like_regex" ["String", "Pattern"] ],
    sqlTrans = SQLTrans
        (fromList [("r_data_main", (["data_id", "data_name", "data_repl_num", "coll_id", "data_size", "data_repl_num", "data_path", "data_checksum", "modify_ts", "create_ts"], ["data_id"])),
                   ("r_coll_main", (["coll_id", "coll_name", "parent_coll_name","create_ts", "modify_ts"], ["coll_id"])),
                   ("r_meta_main", (["meta_id", "meta_attr_name", "meta_attr_value","meta_attr_unit"], ["meta_id"])),
                   ("r_objt_metamap", (["meta_id", "object_id", "create_ts", "modify_ts"], ["meta_id", "object_id"]))
                   ])
        (BuiltIn ( fromList [
            ("le", \thesign args ->
                return ([], SQLCompCond (case thesign of 
                    Pos -> "<="
                    Neg -> ">") (head args) (args !! 1))),
            ("lt", \thesign args ->
                return ([], SQLCompCond (case thesign of 
                    Pos -> "<"
                    Neg -> ">=") (head args) (args !! 1))),
            ("eq", \thesign args ->
                return ([], SQLCompCond (case thesign of 
                    Pos -> "="
                    Neg -> "<>") (head args) (args !! 1))),
            ("like", \thesign args ->
                return ([], SQLCompCond (case thesign of 
                    Pos -> "LIKE"
                    Neg -> "NOT LIKE") (head args) (args !! 1))),
            ("like_regex", \thesign args ->
                return ([], SQLCompCond (case thesign of 
                    Pos -> "~"
                    Neg -> "!~") (head args) (args !! 1)))
        ]))
        (fromList [
            ("DATA_NAME", (OneTable "r_data_main" "1", [( "1", "data_id"), ( "1", "data_name")])),
            ("DATA_SIZE", (OneTable "r_data_main" "1", [( "1", "data_id"), ( "1", "data_size")])),
            ("COLL_NAME", (OneTable "r_coll_main" "1", [( "1", "coll_id"), ( "1", "coll_name")])),
            ("DATA_REPL_NUM", (OneTable "r_data_main" "1", [( "1", "data_id"), ( "1", "data_repl_num")])),
            ("COLL", (OneTable "r_data_main" "1", [( "1", "data_id"), ( "1", "coll_id")])),
            ("DATA_PATH", (OneTable "r_data_main" "1", [( "1", "data_id"), ( "1", "data_path")])),
            ("DATA_CHECKSUM", (OneTable "r_data_main" "1", [( "1", "data_id"), ( "1", "data_checksum")])),
            ("DATA_CREATE_TIME", (OneTable "r_data_main" "1", [( "1", "data_id"), ( "1", "create_ts")])),
            ("DATA_MODIFY_TIME", (OneTable "r_data_main" "1", [( "1", "data_id"), ( "1", "modify_ts")])),
            ("COLL_CREATE_TIME", (OneTable "r_coll_main" "1", [( "1", "coll_id"), ( "1", "create_ts")])),
            ("COLL_MODIFY_TIME", (OneTable "r_coll_main" "1", [( "1", "coll_id"), ( "1", "modify_ts")])),
            ("META", (OneTable "r_objt_metamap" "1", [("1", "object_id"), ("1", "meta_id")])),
            ("META_ATTR_NAME", (OneTable "r_meta_main" "1", [( "1", "meta_id"), ("1", "meta_attr_name")])),
            ("META_ATTR_VALUE", (OneTable "r_meta_main" "1", [( "1", "meta_id"), ("1", "meta_attr_value")])),
            ("META_ATTR_UNITS", (OneTable "r_meta_main" "1", [( "1", "meta_id"), ("1", "meta_attr_unit")]))
            ])
}
