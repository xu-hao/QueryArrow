{-# LANGUAGE FlexibleContexts, MultiParamTypeClasses, FlexibleInstances, OverloadedStrings, GADTs, ExistentialQuantification #-}

module QueryArrow.FFI.GenQuery.Translate where

import QueryArrow.FO.Data (Pred, Formula(..), Var(..), Expr(..), Atom(..), Aggregator(..), Summary(..), Lit(..), Sign(..))

import Prelude hiding (lookup)
import Data.Text (Text, pack)
import System.Log.Logger (errorM, infoM)
import Data.List (find, intercalate, nub)
import Data.Maybe (fromMaybe)
import Data.Char (toLower)
import Debug.Trace

import QueryArrow.Data.Abstract
import QueryArrow.Data.PredicatesGen
import QueryArrow.FFI.GenQuery.Data

prefixes :: [String]
prefixes = ["DATA_ACCESS", "COLL_ACCESS", "DATA", "COLL", "RESC", "ZONE", "USER", "META_DATA", "META_COLL", "META_RESC", "META_USER", "RULE_EXEC"]

extractPrefix :: String -> String
extractPrefix "COLUMN_NAME_NOT_FOUND_510" = "COLL"
extractPrefix "COLUMN_NAME_NOT_FOUND_511" = "COLL"
extractPrefix "COLUMN_NAME_NOT_FOUND_512" = "COLL"
extractPrefix "COLUMN_NAME_NOT_FOUND_421" = "DATA"
extractPrefix "COLUMN_NAME_NOT_FOUND_1300" = "USER"
extractPrefix "COLUMN_NAME_NOT_FOUND_1301" = "USER"
extractPrefix "COLL_TOKEN_NAMESPACE" = "COLL_ACCESS"
extractPrefix "DATA_TOKEN_NAMESPACE" = "DATA_ACCESS"
extractPrefix col =
  fromMaybe (error ("malformatted column name " ++ col)) (find (\p -> take (length p) col == p) prefixes)


toVariable :: String -> Var
toVariable "DATA_ACCESS_DATA_ID" = toVariable "DATA_ID"
toVariable col = Var ("x" ++ col)

toIdVariable :: String -> Var
toIdVariable col = Var ("x" ++ extractPrefix col ++ "_ID")

dataRescId = Var "xDATA_RESC_ID"

translateGenQueryColumnToPredicate :: String -> AbstractFormula Predicates
translateGenQueryColumnToPredicate  col =
  let
      collower = map toLower col
  in
      case collower of
          "resc_id" -> _resc_obj @@ [VarExpr (toVariable col)]
          "coll_id" -> _coll_obj @@ [VarExpr (toVariable col)]
          "data_id" -> _data_obj @@ [VarExpr (toVariable col), VarExpr dataRescId]
          "data_resc_id" -> _data_obj @@ [VarExpr (toIdVariable col), VarExpr dataRescId]
          "data_repl_num" -> _data_repl_num @@ [VarExpr (toIdVariable col), VarExpr dataRescId, VarExpr (toVariable col)]
          "data_coll_id" -> _data_coll_id @@ [VarExpr (toIdVariable col), VarExpr dataRescId, VarExpr (toVariable col)]
          "data_version" -> _data_version @@ [VarExpr (toIdVariable col), VarExpr dataRescId, VarExpr (toVariable col)]
          "data_status" -> _data_status @@ [VarExpr (toIdVariable col), VarExpr dataRescId, VarExpr (toVariable col)]
          "data_type_name" -> _data_type_name @@ [VarExpr (toIdVariable col), VarExpr dataRescId, VarExpr (toVariable col)]
          "data_map_id" -> _data_map_id @@ [VarExpr (toIdVariable col), VarExpr dataRescId, VarExpr (toVariable col)]
          "data_size" -> _data_size @@ [VarExpr (toIdVariable col), VarExpr dataRescId, VarExpr (toVariable col)]
          "data_path" -> _data_path @@ [VarExpr (toIdVariable col), VarExpr dataRescId, VarExpr (toVariable col)]
          "data_name" -> _data_name @@ [VarExpr (toIdVariable col), VarExpr dataRescId, VarExpr (toVariable col)]
          "data_owner_name" -> _data_owner_name @@ [VarExpr (toIdVariable col), VarExpr dataRescId, VarExpr (toVariable col)]
          "data_owner_zone" -> _data_owner_zone @@ [VarExpr (toIdVariable col), VarExpr dataRescId, VarExpr (toVariable col)]
          "data_is_dirty" -> _data_is_dirty @@ [VarExpr (toIdVariable col), VarExpr dataRescId, VarExpr (toVariable col)]
          "data_checksum" -> _data_checksum @@ [VarExpr (toIdVariable col), VarExpr dataRescId, VarExpr (toVariable col)]
          "data_expiry" -> _data_expiry_ts @@ [VarExpr (toIdVariable col), VarExpr dataRescId, VarExpr (toVariable col)]
          "data_comments" -> _data_comment @@ [VarExpr (toIdVariable col), VarExpr dataRescId, VarExpr (toVariable col)]
          "data_create_time" -> _data_create_ts @@ [VarExpr (toIdVariable col), VarExpr dataRescId, VarExpr (toVariable col)]
          "data_modify_time" -> _data_modify_ts @@ [VarExpr (toIdVariable col), VarExpr dataRescId, VarExpr (toVariable col)]
          "data_mode" -> _data_mode @@ [VarExpr (toIdVariable col), VarExpr dataRescId, VarExpr (toVariable col)]
          "data_resc_hier" -> _data_resc_hier @@ [VarExpr (toIdVariable col), VarExpr dataRescId, VarExpr (toVariable col)]
          "data_resc_name" -> _data_resc_name @@ [VarExpr (toIdVariable col), VarExpr dataRescId, VarExpr (toVariable col)]
          "data_repl_status" -> _data_status @@ [VarExpr (toIdVariable col), VarExpr dataRescId, VarExpr (toVariable col)]
          "column_name_not_found_421" -> _data_mode @@ [VarExpr (toIdVariable col), VarExpr dataRescId, VarExpr (toVariable col)]
          "zone_name" -> _zone_name @@ [VarExpr (toIdVariable col), VarExpr (toVariable col)]
          "zone_type" -> _zone_type_name @@ [VarExpr (toIdVariable col), VarExpr (toVariable col)]
          "zone_connection" -> _zone_conn_string @@ [VarExpr (toIdVariable col), VarExpr (toVariable col)]
          "zone_comment" -> _zone_comment @@ [VarExpr (toIdVariable col), VarExpr (toVariable col)]
          "resc_name" -> _resc_name @@ [VarExpr (toIdVariable col), VarExpr (toVariable col)]
          "resc_zone_name" -> _resc_zone_name @@ [VarExpr (toIdVariable col), VarExpr (toVariable col)]
          "resc_type_name" -> _resc_type_name @@ [VarExpr (toIdVariable col), VarExpr (toVariable col)]
          "resc_class_name" -> _resc_class_name @@ [VarExpr (toIdVariable col), VarExpr (toVariable col)]
          "resc_loc" -> _resc_net @@ [VarExpr (toIdVariable col), VarExpr (toVariable col)]
          "resc_vault_path" -> _resc_def_path @@ [VarExpr (toIdVariable col), VarExpr (toVariable col)]
          "resc_free_space" -> _resc_free_space @@ [VarExpr (toIdVariable col), VarExpr (toVariable col)]
          "resc_info" -> _resc_info @@ [VarExpr (toIdVariable col), VarExpr (toVariable col)]
          "resc_comment" -> _resc_comment @@ [VarExpr (toIdVariable col), VarExpr (toVariable col)]
          "resc_create_time" -> _resc_create_ts @@ [VarExpr (toIdVariable col), VarExpr (toVariable col)]
          "resc_modify_time" -> _resc_modify_ts @@ [VarExpr (toIdVariable col), VarExpr (toVariable col)]
          "resc_status" -> _resc_status @@ [VarExpr (toIdVariable col), VarExpr (toVariable col)]
          "resc_children" -> _resc_children @@ [VarExpr (toIdVariable col), VarExpr (toVariable col)]
          "resc_context" -> _resc_context @@ [VarExpr (toIdVariable col), VarExpr (toVariable col)]
          "resc_parent" -> _resc_parent @@ [VarExpr (toIdVariable col), VarExpr (toVariable col)]
          "resc_parent_context" -> _resc_parent_context @@ [VarExpr (toIdVariable col), VarExpr (toVariable col)]
          "coll_inheritance" -> _coll_inheritance @@ [VarExpr (toIdVariable col), VarExpr (toVariable col)]
          "coll_name" -> _coll_name @@ [VarExpr (toIdVariable col), VarExpr (toVariable col)]
          "coll_comments" -> _coll_comment @@ [VarExpr (toIdVariable col), VarExpr (toVariable col)]
          "coll_parent_name" -> _coll_parent_coll_name @@ [VarExpr (toIdVariable col), VarExpr (toVariable col)]
          "coll_owner_name" -> _coll_owner_name @@ [VarExpr (toIdVariable col), VarExpr (toVariable col)]
          "coll_owner_zone" -> _coll_owner_zone @@ [VarExpr (toIdVariable col), VarExpr (toVariable col)]
          "coll_create_time" -> _coll_create_ts @@ [VarExpr (toIdVariable col), VarExpr (toVariable col)]
          "coll_modify_time" -> _coll_modify_ts @@ [VarExpr (toIdVariable col), VarExpr (toVariable col)]
          "column_name_not_found_510" -> _coll_type @@ [VarExpr (toIdVariable col), VarExpr (toVariable col)]
          "column_name_not_found_511" -> _coll_info1 @@ [VarExpr (toIdVariable col), VarExpr (toVariable col)]
          "column_name_not_found_512" -> _coll_info2 @@ [VarExpr (toIdVariable col), VarExpr (toVariable col)]
          -- user (coll)
          "column_name_not_found_1300" -> _user_name @@ [VarExpr (toIdVariable col), VarExpr (toVariable col)]
          "column_name_not_found_1301" -> _user_zone_name @@ [VarExpr (toIdVariable col), VarExpr (toVariable col)]
          -- user
          "user_name" -> _user_name @@ [VarExpr (toIdVariable col), VarExpr (toVariable col)]
          "user_zone" -> _user_zone_name @@ [VarExpr (toIdVariable col), VarExpr (toVariable col)]
          "user_comment" -> _user_comment @@ [VarExpr (toIdVariable col), VarExpr (toVariable col)]
          -- access (coll)
          "coll_access_name" -> _tokn_token_name @@ [VarExpr (toIdVariable col), VarExpr (toVariable col)]
          "coll_token_namespace" -> _tokn_token_namespace @@ [VarExpr (toIdVariable col), VarExpr (toVariable col)]
          -- access (data)
          "data_access_name" -> _tokn_token_name @@ [VarExpr (toIdVariable col), VarExpr (toVariable col)]
          "data_token_namespace" -> _tokn_token_namespace @@ [VarExpr (toIdVariable col), VarExpr (toVariable col)]
          "data_access_data_id" -> _data_obj @@ [VarExpr (toVariable col), VarExpr dataRescId]
          -- meta (data)
          "meta_data_attr_name" -> _meta_attr_name @@ [VarExpr (toIdVariable col), VarExpr (toVariable col)]
          "meta_data_attr_value" -> _meta_attr_value @@ [VarExpr (toIdVariable col), VarExpr (toVariable col)]
          "meta_data_attr_units" -> _meta_attr_unit @@ [VarExpr (toIdVariable col), VarExpr (toVariable col)]
          -- meta (coll)
          "meta_coll_attr_name" -> _meta_attr_name @@ [VarExpr (toIdVariable col), VarExpr (toVariable col)]
          "meta_coll_attr_value" -> _meta_attr_value @@ [VarExpr (toIdVariable col), VarExpr (toVariable col)]
          "meta_coll_attr_units" -> _meta_attr_unit @@ [VarExpr (toIdVariable col), VarExpr (toVariable col)]
          -- meta (resc)
          "meta_resc_attr_name" -> _meta_attr_name @@ [VarExpr (toIdVariable col), VarExpr (toVariable col)]
          "meta_resc_attr_value" -> _meta_attr_value @@ [VarExpr (toIdVariable col), VarExpr (toVariable col)]
          "meta_resc_attr_units" -> _meta_attr_unit @@ [VarExpr (toIdVariable col), VarExpr (toVariable col)]
          -- meta (user)
          "meta_user_attr_name" -> _meta_attr_name @@ [VarExpr (toIdVariable col), VarExpr (toVariable col)]
          "meta_user_attr_value" -> _meta_attr_value @@ [VarExpr (toIdVariable col), VarExpr (toVariable col)]
          "meta_user_attr_units" -> _meta_attr_unit @@ [VarExpr (toIdVariable col), VarExpr (toVariable col)]
          -- rule
          "rule_exec_id" -> _rule_exec_obj @@ [VarExpr (toVariable col)]
          "rule_exec_name" -> _rule_exec_rule_name @@ [VarExpr (toIdVariable col), VarExpr (toVariable col)]
          "rule_exec_rei_file_path" -> _rule_exec_rei_file_path @@ [VarExpr (toIdVariable col), VarExpr (toVariable col)]
          "rule_exec_user_name" -> _rule_exec_user_name @@ [VarExpr (toIdVariable col), VarExpr (toVariable col)]
          "rule_exec_address" -> _rule_exec_exe_address @@ [VarExpr (toIdVariable col), VarExpr (toVariable col)]
          "rule_exec_time" -> _rule_exec_exe_time @@ [VarExpr (toIdVariable col), VarExpr (toVariable col)]
          "rule_exec_frequency" -> _rule_exec_exe_frequency @@ [VarExpr (toIdVariable col), VarExpr (toVariable col)]
          "rule_exec_priority" -> _rule_exec_priority @@ [VarExpr (toIdVariable col), VarExpr (toVariable col)]
          "rule_exec_estimated_exe_time" -> _rule_exec_estimated_exe_time @@ [VarExpr (toIdVariable col), VarExpr (toVariable col)]
          "rule_exec_notification_addr" -> _rule_exec_notification_addr @@ [VarExpr (toIdVariable col), VarExpr (toVariable col)]
          "rule_exec_last_exe_time" -> _rule_exec_last_exe_time @@ [VarExpr (toIdVariable col), VarExpr (toVariable col)]
          "rule_exec_status" -> _rule_exec_exe_status @@ [VarExpr (toIdVariable col), VarExpr (toVariable col)]
          _ -> error ("unsupported column " ++ col)

toCondPredicate2 :: [String] -> Cond ->  AbstractFormula Predicates
toCondPredicate2  cols cond@(Cond col _) =
  let form = toCondPredicate  cond in
  if col `elem` cols
    then form
    else  (translateGenQueryColumnToPredicate  col) .*. form

toCondPredicate :: Cond ->  AbstractFormula Predicates
toCondPredicate  (Cond col (EqString str)) =
  _eq @@ [VarExpr (toVariable col), StringExpr (pack str)]
toCondPredicate  (Cond col (EqInteger str)) =
  _eq @@ [VarExpr (toVariable col), IntExpr (fromIntegral str)]
toCondPredicate  (Cond col (NotEqString str)) =
  aggregate Not (_eq @@ [VarExpr (toVariable col), StringExpr (pack str)])
toCondPredicate  (Cond col (NotEqInteger str)) =
  aggregate Not (_eq @@ [VarExpr (toVariable col), IntExpr (fromIntegral str)])
toCondPredicate  (Cond col (LikeCond str)) =
  _like @@ [VarExpr (toVariable col), StringExpr (pack str)]
toCondPredicate  (Cond col (ParentOfCond str)) =
  _coll_name @@ [VarExpr (Var "cid"), StringExpr (pack str)] .*.
      _coll_parent_coll_name @@ [VarExpr (Var "cid"), VarExpr (toVariable col)]
toCondPredicate  (Cond col (AndCond a b)) =
  toCondPredicate  (Cond col a) .*.
      toCondPredicate  (Cond col b)
toCondPredicate  (Cond col (OrCond a b)) =
  toCondPredicate  (Cond col a) .+.
      toCondPredicate  (Cond col b)


toJoinPredicate :: [String] -> [Cond] -> [ AbstractFormula Predicates]
toJoinPredicate  cols conds =
    let
        tables = nub (map extractPrefix (cols ++ map (\(Cond col _) -> col) conds))
    in
        trace ("toJoinPredicate: tables = " ++ show tables) [
          if "COLL" `elem` tables && "DATA" `elem` tables
            then _data_coll_id @@ [VarExpr (Var "xDATA_ID"), VarExpr (Var "xDATA_RESC_ID"), VarExpr (Var "xCOLL_ID")]
            else return FOne,
          if "COLL_ACCESS" `elem` tables
            then _objt_access_obj @@ [VarExpr (Var "xCOLL_ID"), VarExpr (Var "xUSER_ID"), VarExpr (Var "xCOLL_ACCESS_ID")]
            else return FOne,
          if "DATA_ACCESS" `elem` tables
            then _objt_access_obj @@ [VarExpr (Var "xDATA_ID"), VarExpr (Var "xUSER_ID"), VarExpr (Var "xDATA_ACCESS_ID")]
            else return FOne,
          if "META_DATA" `elem` tables
            then _objt_metamap_obj @@ [VarExpr (Var "xDATA_ID"), VarExpr (Var "xMETA_DATA_ID")]
            else return FOne,
          if "META_COLL" `elem` tables
            then _objt_metamap_obj @@ [VarExpr (Var "xCOLL_ID"), VarExpr (Var "xMETA_COLL_ID")]
            else return FOne,
          if "META_RESC" `elem` tables
            then _objt_metamap_obj @@ [VarExpr (Var "xRESC_ID"), VarExpr (Var "xMETA_DATA_ID")]
            else return FOne,
          if "META_USER" `elem` tables
            then _objt_metamap_obj @@ [VarExpr (Var "xUSER_ID"), VarExpr (Var "xMETA_COLL_ID")]
            else return FOne
        ]

translateGenQueryToQAL :: GenQuery -> ([Var],  AbstractFormula Predicates)
translateGenQueryToQAL  (GenQuery sels conds) =
      if null sels
        then error "cannot translate genquery cols null"
        else
          let
              cols = map (\(Sel col _) -> col) sels
              vars = map toVariable cols
              form0 = foldr (.*.) (return (FReturn vars)) (map (translateGenQueryColumnToPredicate ) cols ++ toJoinPredicate  cols conds ++ map (toCondPredicate2  cols) conds)
              orders = map (\(Sel col _) -> col) (filter (\(Sel _ sel) -> case sel of
                                                                              Order -> True
                                                                              _ -> False) sels)
              form = foldr (\col form -> aggregate (OrderByAsc (toVariable col)) form) form0 orders
          in
              (vars, form)
