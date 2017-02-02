{-# LANGUAGE FlexibleContexts, MultiParamTypeClasses, FlexibleInstances, OverloadedStrings, GADTs, ExistentialQuantification #-}

module QueryArrow.FFI.GenQuery.Translate where

import QueryArrow.FO.Data (Pred, Formula(..), Var(..), Expr(..), Atom(..), Aggregator(..), Summary(..), Lit(..), Sign(..), (.+.), (.*.), (@@), CastType(..))

import Prelude hiding (lookup)
import Data.Text (Text, pack)
import System.Log.Logger (errorM, infoM)
import Data.List (find, intercalate, nub)
import Data.Maybe (fromMaybe)
import Data.Char (toLower)
import Debug.Trace

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

translateGenQueryColumnToPredicate :: String -> Formula
translateGenQueryColumnToPredicate  col =
      case col of
          "RESC_ID" -> "RESC_OBJ" @@ [VarExpr (toVariable col)]
          "COLL_ID" -> "COLL_OBJ" @@ [VarExpr (toVariable col)]
          "DATA_ID" -> "DATA_OBJ" @@ [VarExpr (toVariable col), VarExpr dataRescId]
          "DATA_RESC_ID" -> "DATA_OBJ" @@ [VarExpr (toIdVariable col), VarExpr dataRescId]
          "DATA_REPL_NUM" -> "DATA_REPL_NUM" @@ [VarExpr (toIdVariable col), VarExpr dataRescId, VarExpr (toVariable col)]
          "DATA_COLL_ID" -> "DATA_COLL_ID" @@ [VarExpr (toIdVariable col), VarExpr dataRescId, VarExpr (toVariable col)]
          "DATA_VERSION" -> "DATA_VERSION" @@ [VarExpr (toIdVariable col), VarExpr dataRescId, VarExpr (toVariable col)]
          "DATA_STATUS" -> "DATA_STATUS" @@ [VarExpr (toIdVariable col), VarExpr dataRescId, VarExpr (toVariable col)]
          "DATA_TYPE_NAME" -> "DATA_TYPE_NAME" @@ [VarExpr (toIdVariable col), VarExpr dataRescId, VarExpr (toVariable col)]
          "DATA_MAP_ID" -> "DATA_MAP_ID" @@ [VarExpr (toIdVariable col), VarExpr dataRescId, VarExpr (toVariable col)]
          "DATA_SIZE" -> "DATA_SIZE" @@ [VarExpr (toIdVariable col), VarExpr dataRescId, VarExpr (toVariable col)]
          "DATA_PATH" -> "DATA_PATH" @@ [VarExpr (toIdVariable col), VarExpr dataRescId, VarExpr (toVariable col)]
          "DATA_NAME" -> "DATA_NAME" @@ [VarExpr (toIdVariable col), VarExpr dataRescId, VarExpr (toVariable col)]
          "DATA_OWNER_NAME" -> "DATA_OWNER_NAME" @@ [VarExpr (toIdVariable col), VarExpr dataRescId, VarExpr (toVariable col)]
          "DATA_OWNER_ZONE" -> "DATA_OWNER_ZONE" @@ [VarExpr (toIdVariable col), VarExpr dataRescId, VarExpr (toVariable col)]
          "DATA_IS_DIRTY" -> "DATA_IS_DIRTY" @@ [VarExpr (toIdVariable col), VarExpr dataRescId, VarExpr (toVariable col)]
          "DATA_CHECKSUM" -> "DATA_CHECKSUM" @@ [VarExpr (toIdVariable col), VarExpr dataRescId, VarExpr (toVariable col)]
          "DATA_EXPIRY" -> "DATA_EXPIRY_TS" @@ [VarExpr (toIdVariable col), VarExpr dataRescId, VarExpr (toVariable col)]
          "DATA_COMMENTS" -> "DATA_COMMENT" @@ [VarExpr (toIdVariable col), VarExpr dataRescId, VarExpr (toVariable col)]
          "DATA_CREATE_TIME" -> "DATA_CREATE_TS" @@ [VarExpr (toIdVariable col), VarExpr dataRescId, VarExpr (toVariable col)]
          "DATA_MODIFY_TIME" -> "DATA_MODIFY_TS" @@ [VarExpr (toIdVariable col), VarExpr dataRescId, VarExpr (toVariable col)]
          "DATA_MODE" -> "DATA_MODE" @@ [VarExpr (toIdVariable col), VarExpr dataRescId, VarExpr (toVariable col)]
          "DATA_RESC_HIER" -> "DATA_RESC_HIER" @@ [VarExpr (toIdVariable col), VarExpr dataRescId, VarExpr (toVariable col)]
          "DATA_RESC_NAME" -> "DATA_RESC_NAME" @@ [VarExpr (toIdVariable col), VarExpr dataRescId, VarExpr (toVariable col)]
          "DATA_REPL_STATUS" -> "DATA_STATUS" @@ [VarExpr (toIdVariable col), VarExpr dataRescId, VarExpr (toVariable col)]
          "COLUMN_NAME_NOT_FOUND_421" -> "DATA_MODE" @@ [VarExpr (toIdVariable col), VarExpr dataRescId, VarExpr (toVariable col)]
          "ZONE_NAME" -> "ZONE_NAME" @@ [VarExpr (toIdVariable col), VarExpr (toVariable col)]
          "ZONE_TYPE" -> "ZONE_TYPE_NAME" @@ [VarExpr (toIdVariable col), VarExpr (toVariable col)]
          "ZONE_CONNECTION" -> "ZONE_CONN_STRING" @@ [VarExpr (toIdVariable col), VarExpr (toVariable col)]
          "ZONE_COMMENT" -> "ZONE_COMMENT" @@ [VarExpr (toIdVariable col), VarExpr (toVariable col)]
          "RESC_NAME" -> "RESC_NAME" @@ [VarExpr (toIdVariable col), VarExpr (toVariable col)]
          "RESC_ZONE_NAME" -> "RESC_ZONE_NAME" @@ [VarExpr (toIdVariable col), VarExpr (toVariable col)]
          "RESC_TYPE_NAME" -> "RESC_TYPE_NAME" @@ [VarExpr (toIdVariable col), VarExpr (toVariable col)]
          "RESC_CLASS_NAME" -> "RESC_CLASS_NAME" @@ [VarExpr (toIdVariable col), VarExpr (toVariable col)]
          "RESC_LOC" -> "RESC_NET" @@ [VarExpr (toIdVariable col), VarExpr (toVariable col)]
          "RESC_VAULT_PATH" -> "RESC_DEF_PATH" @@ [VarExpr (toIdVariable col), VarExpr (toVariable col)]
          "RESC_FREE_SPACE" -> "RESC_FREE_SPACE" @@ [VarExpr (toIdVariable col), VarExpr (toVariable col)]
          "RESC_INFO" -> "RESC_INFO" @@ [VarExpr (toIdVariable col), VarExpr (toVariable col)]
          "RESC_COMMENT" -> "RESC_COMMENT" @@ [VarExpr (toIdVariable col), VarExpr (toVariable col)]
          "RESC_CREATE_TIME" -> "RESC_CREATE_TS" @@ [VarExpr (toIdVariable col), VarExpr (toVariable col)]
          "RESC_MODIFY_TIME" -> "RESC_MODIFY_TS" @@ [VarExpr (toIdVariable col), VarExpr (toVariable col)]
          "RESC_STATUS" -> "RESC_STATUS" @@ [VarExpr (toIdVariable col), VarExpr (toVariable col)]
          "RESC_CHILDREN" -> "RESC_CHILDREN" @@ [VarExpr (toIdVariable col), VarExpr (toVariable col)]
          "RESC_CONTEXT" -> "RESC_CONTEXT" @@ [VarExpr (toIdVariable col), VarExpr (toVariable col)]
          "RESC_PARENT" -> "RESC_PARENT" @@ [VarExpr (toIdVariable col), VarExpr (toVariable col)]
          "RESC_PARENT_CONTEXT" -> "RESC_PARENT_CONTEXT" @@ [VarExpr (toIdVariable col), VarExpr (toVariable col)]
          "COLL_INHERITANCE" -> "COLL_INHERITANCE" @@ [VarExpr (toIdVariable col), VarExpr (toVariable col)]
          "COLL_NAME" -> "COLL_NAME" @@ [VarExpr (toIdVariable col), VarExpr (toVariable col)]
          "COLL_COMMENTS" -> "COLL_COMMENT" @@ [VarExpr (toIdVariable col), VarExpr (toVariable col)]
          "COLL_PARENT_NAME" -> "COLL_PARENT_COLL_NAME" @@ [VarExpr (toIdVariable col), VarExpr (toVariable col)]
          "COLL_OWNER_NAME" -> "COLL_OWNER_NAME" @@ [VarExpr (toIdVariable col), VarExpr (toVariable col)]
          "COLL_OWNER_ZONE" -> "COLL_OWNER_ZONE" @@ [VarExpr (toIdVariable col), VarExpr (toVariable col)]
          "COLL_CREATE_TIME" -> "COLL_CREATE_TS" @@ [VarExpr (toIdVariable col), VarExpr (toVariable col)]
          "COLL_MODIFY_TIME" -> "COLL_MODIFY_TS" @@ [VarExpr (toIdVariable col), VarExpr (toVariable col)]
          "COLUMN_NAME_NOT_FOUND_510" -> "COLL_TYPE" @@ [VarExpr (toIdVariable col), VarExpr (toVariable col)]
          "COLUMN_NAME_NOT_FOUND_511" -> "COLL_INFO1" @@ [VarExpr (toIdVariable col), VarExpr (toVariable col)]
          "COLUMN_NAME_NOT_FOUND_512" -> "COLL_INFO2" @@ [VarExpr (toIdVariable col), VarExpr (toVariable col)]
          -- USER (coll)
          "COLUMN_NAME_NOT_FOUND_1300" -> "USER_NAME" @@ [VarExpr (toIdVariable col), VarExpr (toVariable col)]
          "COLUMN_NAME_NOT_FOUND_1301" -> "USER_ZONE_NAME" @@ [VarExpr (toIdVariable col), VarExpr (toVariable col)]
          -- USER
          "USER_NAME" -> "USER_NAME" @@ [VarExpr (toIdVariable col), VarExpr (toVariable col)]
          "USER_ZONE" -> "USER_ZONE_NAME" @@ [VarExpr (toIdVariable col), VarExpr (toVariable col)]
          "USER_COMMENT" -> "USER_COMMENT" @@ [VarExpr (toIdVariable col), VarExpr (toVariable col)]
          "USER_TYPE" -> "USER_TYPE" @@ [VarExpr (toIdVariable col), VarExpr (toVariable col)]
          -- ACCESS (coll""
          "COLL_ACCESS_NAME" -> "TOKN_TOKEN_NAME" @@ [VarExpr (toIdVariable col), VarExpr (toVariable col)]
          "COLL_TOKEN_NAMESPACE" -> "TOKN_TOKEN_NAMESPACE" @@ [VarExpr (toIdVariable col), VarExpr (toVariable col)]
          -- ACCESS (data)
          "DATA_ACCESS_NAME" -> "TOKN_TOKEN_NAME" @@ [VarExpr (toIdVariable col), VarExpr (toVariable col)]
          "DATA_TOKEN_NAMESPACE" -> "TOKN_TOKEN_NAMESPACE" @@ [VarExpr (toIdVariable col), VarExpr (toVariable col)]
          "DATA_ACCESS_DATA_ID" -> "DATA_OBJ" @@ [VarExpr (toVariable col), VarExpr dataRescId]
          -- META (data)
          "META_DATA_ATTR_NAME" -> "META_ATTR_NAME" @@ [VarExpr (toIdVariable col), VarExpr (toVariable col)]
          "META_DATA_ATTR_VALUE" -> "META_ATTR_VALUE" @@ [VarExpr (toIdVariable col), VarExpr (toVariable col)]
          "META_DATA_ATTR_UNITS" -> "META_ATTR_UNIT" @@ [VarExpr (toIdVariable col), VarExpr (toVariable col)]
          -- META (coll)
          "META_COLL_ATTR_NAME" -> "META_ATTR_NAME" @@ [VarExpr (toIdVariable col), VarExpr (toVariable col)]
          "META_COLL_ATTR_VALUE" -> "META_ATTR_VALUE" @@ [VarExpr (toIdVariable col), VarExpr (toVariable col)]
          "META_COLL_ATTR_UNITS" -> "META_ATTR_UNIT" @@ [VarExpr (toIdVariable col), VarExpr (toVariable col)]
          -- META (resc)
          "META_RESC_ATTR_NAME" -> "META_ATTR_NAME" @@ [VarExpr (toIdVariable col), VarExpr (toVariable col)]
          "META_RESC_ATTR_VALUE" -> "META_ATTR_VALUE" @@ [VarExpr (toIdVariable col), VarExpr (toVariable col)]
          "META_RESC_ATTR_UNITS" -> "META_ATTR_UNIT" @@ [VarExpr (toIdVariable col), VarExpr (toVariable col)]
          -- META (user)
          "META_USER_ATTR_NAME" -> "META_ATTR_NAME" @@ [VarExpr (toIdVariable col), VarExpr (toVariable col)]
          "META_USER_ATTR_VALUE" -> "META_ATTR_VALUE" @@ [VarExpr (toIdVariable col), VarExpr (toVariable col)]
          "META_USER_ATTR_UNITS" -> "META_ATTR_UNIT" @@ [VarExpr (toIdVariable col), VarExpr (toVariable col)]
          -- RULE
          "RULE_EXEC_ID" -> "RULE_EXEC_OBJ" @@ [VarExpr (toVariable col)]
          "RULE_EXEC_NAME" -> "RULE_EXEC_RULE_NAME" @@ [VarExpr (toIdVariable col), VarExpr (toVariable col)]
          "RULE_EXEC_REI_FILE_PATH" -> "RULE_EXEC_REI_FILE_PATH" @@ [VarExpr (toIdVariable col), VarExpr (toVariable col)]
          "RULE_EXEC_USER_NAME" -> "RULE_EXEC_USER_NAME" @@ [VarExpr (toIdVariable col), VarExpr (toVariable col)]
          "RULE_EXEC_ADDRESS" -> "RULE_EXEC_EXE_ADDRESS" @@ [VarExpr (toIdVariable col), VarExpr (toVariable col)]
          "RULE_EXEC_TIME" -> "RULE_EXEC_EXE_TIME" @@ [VarExpr (toIdVariable col), VarExpr (toVariable col)]
          "RULE_EXEC_FREQUENCY" -> "RULE_EXEC_EXE_FREQUENCY" @@ [VarExpr (toIdVariable col), VarExpr (toVariable col)]
          "RULE_EXEC_PRIORITY" -> "RULE_EXEC_PRIORITY" @@ [VarExpr (toIdVariable col), VarExpr (toVariable col)]
          "RULE_EXEC_ESTIMATED_EXE_TIME" -> "RULE_EXEC_ESTIMATED_EXE_TIME" @@ [VarExpr (toIdVariable col), VarExpr (toVariable col)]
          "RULE_EXEC_NOTIFICATION_ADDR" -> "RULE_EXEC_NOTIFICATION_ADDR" @@ [VarExpr (toIdVariable col), VarExpr (toVariable col)]
          "RULE_EXEC_LAST_EXE_TIME" -> "RULE_EXEC_LAST_EXE_TIME" @@ [VarExpr (toIdVariable col), VarExpr (toVariable col)]
          "RULE_EXEC_STATUS" -> "RULE_EXEC_EXE_STATUS" @@ [VarExpr (toIdVariable col), VarExpr (toVariable col)]
          _ -> error ("unsupported column " ++ col)

toCondPredicate2 :: [String] -> Cond ->  Formula
toCondPredicate2  cols cond@(Cond col _) =
  let form = toCondPredicate  cond in
  if col `elem` cols
    then form
    else  (translateGenQueryColumnToPredicate  col) .*. form

toCondPredicate :: Cond ->  Formula
toCondPredicate  (Cond col (EqString str)) =
  "eq" @@ [CastExpr TextType (VarExpr (toVariable col)), StringExpr (pack str)]
toCondPredicate  (Cond col (EqInteger str)) =
  "eq" @@ [CastExpr NumberType (VarExpr (toVariable col)), IntExpr (fromIntegral str)]
toCondPredicate  (Cond col (NotEqString str)) =
  Aggregate Not ("eq" @@ [VarExpr (toVariable col), StringExpr (pack str)])
toCondPredicate  (Cond col (NotEqInteger str)) =
  Aggregate Not ("eq" @@ [VarExpr (toVariable col), IntExpr (fromIntegral str)])
toCondPredicate  (Cond col (LikeCond str)) =
  "like" @@ [VarExpr (toVariable col), StringExpr (pack str)]
toCondPredicate  (Cond col (ParentOfCond str)) =
  "COLL_NAME" @@ [VarExpr (Var "cid"), StringExpr (pack str)] .*.
      "COLL_PARENT_COLL_NAME" @@ [VarExpr (Var "cid"), VarExpr (toVariable col)]
toCondPredicate  (Cond col (AndCond a b)) =
  toCondPredicate  (Cond col a) .*.
      toCondPredicate  (Cond col b)
toCondPredicate  (Cond col (OrCond a b)) =
  toCondPredicate  (Cond col a) .+.
      toCondPredicate  (Cond col b)


toJoinPredicate :: [String] -> [Cond] -> [ Formula]
toJoinPredicate  cols conds =
    let
        tables = nub (map extractPrefix (cols ++ map (\(Cond col _) -> col) conds))
    in
        [
          if "COLL" `elem` tables && "DATA" `elem` tables
            then "DATA_COLL_ID" @@ [VarExpr (Var "xDATA_ID"), VarExpr (Var "xDATA_RESC_ID"), VarExpr (Var "xCOLL_ID")]
            else FOne,
          if "COLL_ACCESS" `elem` tables
            then "OBJT_ACCESS_OBJ" @@ [VarExpr (Var "xCOLL_ID"), VarExpr (Var "xUSER_ID"), VarExpr (Var "xCOLL_ACCESS_ID")]
            else FOne,
          if "DATA_ACCESS" `elem` tables
            then "OBJT_ACCESS_OBJ" @@ [VarExpr (Var "xDATA_ID"), VarExpr (Var "xUSER_ID"), VarExpr (Var "xDATA_ACCESS_ID")]
            else FOne,
          if "META_DATA" `elem` tables
            then "OBJT_METAMAP_OBJ" @@ [VarExpr (Var "xDATA_ID"), VarExpr (Var "xMETA_DATA_ID")]
            else FOne,
          if "META_COLL" `elem` tables
            then "OBJT_METAMAP_OBJ" @@ [VarExpr (Var "xCOLL_ID"), VarExpr (Var "xMETA_COLL_ID")]
            else FOne,
          if "META_RESC" `elem` tables
            then "OBJT_METAMAP_OBJ" @@ [VarExpr (Var "xRESC_ID"), VarExpr (Var "xMETA_DATA_ID")]
            else FOne,
          if "META_USER" `elem` tables
            then "OBJT_METAMAP_OBJ" @@ [VarExpr (Var "xUSER_ID"), VarExpr (Var "xMETA_COLL_ID")]
            else FOne
        ]

translateGenQueryToQAL :: GenQuery -> ([Var],  Formula)
translateGenQueryToQAL  (GenQuery sels conds) =
      if null sels
        then error "cannot translate genquery cols null"
        else
          let
              cols = map (\(Sel col _) -> col) sels
              vars = map toVariable cols
              form0 = Aggregate (FReturn vars) (foldr (.*.) FOne (map translateGenQueryColumnToPredicate cols ++ toJoinPredicate  cols conds ++ map (toCondPredicate2  cols) conds))
              orders = map (\(Sel col _) -> col) (filter (\(Sel _ sel) -> case sel of
                                                                              Order -> True
                                                                              _ -> False) sels)
              form = foldr (\col form -> Aggregate (OrderByAsc (toVariable col)) form) form0 orders
          in
              (vars, form)
