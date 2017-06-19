{-# LANGUAGE FlexibleContexts, MultiParamTypeClasses, FlexibleInstances, OverloadedStrings, GADTs, ExistentialQuantification #-}

module QueryArrow.FFI.GenQuery.Translate where

import QueryArrow.FO.Data
import QueryArrow.FO.Types

import Prelude hiding (lookup)
import Data.Text (Text, pack)
import System.Log.Logger (errorM, infoM)
import Data.List (find, intercalate, nub)
import Data.Maybe (fromMaybe)
import Data.Char (toLower)
import Debug.Trace

import QueryArrow.FFI.GenQuery.Data

prefixes :: [String]
prefixes = ["DATA_ACCESS", "COLL_ACCESS", "DATA", "COLL", "RESC", "ZONE", "USER", "META_DATA", "META_COLL", "META_RESC", "META_USER", "RULE_EXEC", "SERVER_LOAD_DIGEST", "SERVER_LOAD", "TOKEN", "TICKET_ALLOWED_HOST",  "TICKET_ALLOWED_USER",  "TICKET_ALLOWED_GROUP", "TICKET", "QUOTA"]

extractPrefix :: String -> String
extractPrefix "COLUMN_NAME_NOT_FOUND_510" = "COLL"
extractPrefix "COLUMN_NAME_NOT_FOUND_511" = "COLL"
extractPrefix "COLUMN_NAME_NOT_FOUND_512" = "COLL"
extractPrefix "COLUMN_NAME_NOT_FOUND_421" = "DATA"
extractPrefix "COLUMN_NAME_NOT_FOUND_1300" = "USER"
extractPrefix "COLUMN_NAME_NOT_FOUND_1301" = "USER"
extractPrefix "COLL_TOKEN_NAMESPACE" = "COLL_ACCESS"
extractPrefix "DATA_TOKEN_NAMESPACE" = "DATA_ACCESS"
extractPrefix ('S' : 'L' : 'D' : '_' : _) = "SERVER_LOAD_DIGEST"
extractPrefix ('S' : 'L' : '_' : _) = "SERVER_LOAD"
extractPrefix col =
  fromMaybe (error ("malformatted column name " ++ col)) (find (\p -> take (length p) col == p) prefixes)


toVariable :: String -> Var
toVariable "DATA_ACCESS_DATA_ID" = toVariable "DATA_ID"
toVariable "DATA_ACCESS_USER_ID" = toVariable "USER_ID"
toVariable col = Var ("x" ++ col)

toIdVariable :: String -> Var
toIdVariable col = Var ("x" ++ extractPrefix col ++ "_ID")

dataRescId = toVariable "DATA_RESC_ID"
userAuthName = toVariable "USER_DN"
dataId = toVariable "DATA_ID"
collId = toVariable "COLL_ID"
userId = toVariable "USER_ID"

translateGenQueryColumnToPredicate :: String -> Formula
translateGenQueryColumnToPredicate  col =
      case col of
          "RESC_ID" -> "RESC_OBJ" @@ [VarExpr (toVariable col)]
          "COLL_ID" -> "COLL_OBJ" @@ [VarExpr (toVariable col)]
          "DATA_ID" -> "DATA_OBJ" @@ [VarExpr (toVariable col), VarExpr dataRescId]
          "USER_ID" -> "USER_OBJ" @@ [VarExpr (toVariable col)]
          "USER_DN" -> "USER_AUTH_OBJ" @@ [VarExpr (toIdVariable col), VarExpr userAuthName]
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
          "DATA_RESC_NAME" -> "DATA_OBJ" @@ [VarExpr dataId, VarExpr dataRescId] .*. "RESC_NAME" @@ [VarExpr dataRescId, VarExpr (toVariable col)]
          "DATA_REPL_STATUS" -> "DATA_IS_DIRTY" @@ [VarExpr (toIdVariable col), VarExpr dataRescId, VarExpr (toVariable col)]
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
          "RESC_FREE_SPACE_TIME" -> "RESC_FREE_SPACE_TS" @@ [VarExpr (toIdVariable col), VarExpr (toVariable col)]
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
          "USER_TYPE" -> "USER_TYPE_NAME" @@ [VarExpr (toIdVariable col), VarExpr (toVariable col)]
          "USER_GROUP_NAME" -> "USER_GROUP_NAME" @@ [VarExpr (toIdVariable col), VarExpr (toVariable col)]
          "USER_INFO" -> "USER_INFO" @@ [VarExpr (toIdVariable col), VarExpr (toVariable col)]
          "USER_CREATE_TIME" -> "USER_CREATE_TS" @@ [VarExpr (toIdVariable col), VarExpr (toVariable col)]
          "USER_MODIFY_TIME" -> "USER_MODIFY_TS" @@ [VarExpr (toIdVariable col), VarExpr (toVariable col)]
          -- USER_AUTH
          "USER_AUTH_CREATE_TIME" -> "USER_AUTH_CREATE_TS" @@ [VarExpr (toIdVariable col), VarExpr userAuthName, VarExpr (toVariable col)]
          -- ACCESS (coll""
          "COLL_ACCESS_NAME" -> "TOKN_TOKEN_NAME" @@ [VarExpr (toIdVariable col), VarExpr (toVariable col)]
          "COLL_TOKEN_NAMESPACE" -> "TOKN_TOKEN_NAMESPACE" @@ [VarExpr (toIdVariable col), VarExpr (toVariable col)]
          -- ACCESS (data)
          "DATA_ACCESS_NAME" -> "TOKN_TOKEN_NAME" @@ [VarExpr (toIdVariable col), VarExpr (toVariable col)]
          "DATA_TOKEN_NAMESPACE" -> "TOKN_TOKEN_NAMESPACE" @@ [VarExpr (toIdVariable col), VarExpr (toVariable col)]
          "DATA_ACCESS_DATA_ID" -> "DATA_OBJ" @@ [VarExpr dataId, VarExpr dataRescId]
          "DATA_ACCESS_USER_ID" -> "USER_OBJ" @@ [VarExpr userId]
          "DATA_ACCESS_TYPE" -> FOne
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
          -- SL
          'S' : 'L' : '_' : _ -> "SERVER_LOAD_OBJ" @@ [VarExpr (toVariable "SL_HOST_NAME"), VarExpr (toVariable "SL_RESC_NAME"), VarExpr (toVariable "SL_CPU_USED"),VarExpr (toVariable "SL_MEM_USED"), VarExpr (toVariable "SL_SWAP_USED"), VarExpr (toVariable "SL_RUNQ_LOAD"), VarExpr (toVariable "SL_DISK_SPACE"), VarExpr (toVariable "SL_NET_INPUT"), VarExpr (toVariable "SL_NET_OUTPUT"), VarExpr (toVariable "SL_CREATE_TIME")] 
          -- SLD
          "SLD_LOAD_FACTOR" -> "SERVER_LOAD_DIGEST_OBJ" @@ [VarExpr (toVariable "SLD_RESC_NAME"), VarExpr (toVariable "SLD_LOAD_FACTOR"), VarExpr (toVariable "SLD_CREATE_TIME")]
          "SLD_RESC_NAME" -> "SERVER_LOAD_DIGEST_OBJ" @@ [VarExpr (toVariable "SLD_RESC_NAME"), VarExpr (toVariable "SLD_LOAD_FACTOR"), VarExpr (toVariable "SLD_CREATE_TIME")]
          "SLD_CREATE_TIME" -> "SERVER_LOAD_DIGEST_OBJ" @@ [VarExpr (toVariable "SLD_RESC_NAME"), VarExpr (toVariable "SLD_LOAD_FACTOR"), VarExpr (toVariable "SLD_CREATE_TIME")]
          -- TOKEN
          "TOKEN_ID" -> "TOKN_OBJ" @@ [VarExpr (toVariable col)]
          "TOKEN_NAME" -> "TOKN_TOKEN_NAME" @@ [VarExpr (toIdVariable col), VarExpr (toVariable col)]
          "TOKEN_NAMESPACE" -> "TOKN_TOKEN_NAMESPACE" @@ [VarExpr (toIdVariable col), VarExpr (toVariable col)]
          -- TICKET
          "TICKET_ID" -> "TICKET_OBJ" @@ [VarExpr (toVariable col)]
          "TICKET_STRING" -> "TICKET_STRING" @@ [VarExpr (toIdVariable col), VarExpr (toVariable col)]
          "TICKET_TYPE" -> "TICKET_TYPE" @@ [VarExpr (toIdVariable col), VarExpr (toVariable col)]
          "TICKET_OBJECT_TYPE" -> "TICKET_OBJECT_TYPE" @@ [VarExpr (toIdVariable col), VarExpr (toVariable col)]
          "TICKET_OWNER_NAME" -> "TICKET_USER_ID" @@ [VarExpr (toIdVariable col), VarExpr (toVariable "TICKET_USER_ID")] .*. "USER_NAME" @@ [VarExpr (toVariable "TICKET_USER_ID"), VarExpr (toVariable "TICKET_OWNER_NAME")]
          "TICKET_OWNER_ZONE" -> "TICKET_USER_ID" @@ [VarExpr (toIdVariable col), VarExpr (toVariable "TICKET_USER_ID")] .*. "USER_ZONE_NAME" @@ [VarExpr (toVariable "TICKET_USER_ID"), VarExpr (toVariable "TICKET_OWNER_ZONE")]
          "TICKET_USES_COUNT" -> "TICKET_USES_COUNT" @@ [VarExpr (toIdVariable col), VarExpr (toVariable col)]
          "TICKET_USES_LIMIT" -> "TICKET_USES_LIMIT" @@ [VarExpr (toIdVariable col), VarExpr (toVariable col)]
          "TICKET_WRITE_FILE_COUNT" -> "TICKET_WRITE_FILE_COUNT" @@ [VarExpr (toIdVariable col), VarExpr (toVariable col)]
          "TICKET_WRITE_FILE_LIMIT" -> "TICKET_WRITE_FILE_LIMIT" @@ [VarExpr (toIdVariable col), VarExpr (toVariable col)]
          "TICKET_WRITE_BYTE_COUNT" -> "TICKET_WRITE_BYTE_COUNT" @@ [VarExpr (toIdVariable col), VarExpr (toVariable col)]
          "TICKET_WRITE_BYTE_LIMIT" -> "TICKET_WRITE_BYTE_LIMIT" @@ [VarExpr (toIdVariable col), VarExpr (toVariable col)]
          "TICKET_EXPIRY" -> "TICKET_EXPIRY_TS" @@ [VarExpr (toIdVariable col), VarExpr (toVariable col)]
          "TICKET_DATA_NAME" -> "TICKET_OBJECT_ID" @@ [VarExpr (toIdVariable col), VarExpr (toVariable "TICKET_OBJECT_ID")] .*. "DATA_NAME" @@ [VarExpr (toVariable "TICKET_OBJECT_ID"), VarExpr (toVariable "TICKET_OBJECT_RESC_ID"), VarExpr (toVariable "TICKET_DATA_NAME")]
          "TICKET_DATA_COLL_NAME" -> "TICKET_OBJECT_ID" @@ [VarExpr (toIdVariable col), VarExpr (toVariable "TICKET_OBJECT_ID")] .*. "DATA_COLL_ID" @@ [VarExpr (toVariable "TICKET_OBJECT_ID"), VarExpr (toVariable "TICKET_OBJECT_RESC_ID"), VarExpr (toVariable "TICKET_OBJECT_COLL_ID")] .*. "COLL_NAME" @@ [VarExpr (toVariable "TICKET_OBJECT_COLL_ID"), VarExpr (toVariable "TICKET_DATA_COLL_NAME")]
          "TICKET_COLL_NAME" -> "TICKET_OBJECT_ID" @@ [VarExpr (toIdVariable col), VarExpr (toVariable "TICKET_OBJECT_ID")] .*. "COLL_NAME" @@ [VarExpr (toVariable "TICKET_OBJECT_ID"), VarExpr (toVariable "TICKET_COLL_NAME")]
          -- TICKET_ALLOWED_HOSTS
          "TICKET_ALLOWED_HOST_TICKET_ID" -> "TICKET_ALLOWED_HOSTS_OBJ" @@ [VarExpr (toVariable "TICKET_ALLOWED_HOST_TICKET_ID"), VarExpr (toVariable "TICKET_ALLOWED_HOST")]
          "TICKET_ALLOWED_HOST" -> "TICKET_ALLOWED_HOSTS_OBJ" @@ [VarExpr (toVariable "TICKET_ALLOWED_HOST_TICKET_ID"), VarExpr (toVariable "TICKET_ALLOWED_HOST")]
          -- TICKET_ALLOWED_USERS
          "TICKET_ALLOWED_USER_TICKET_ID" -> "TICKET_ALLOWED_USERS_OBJ" @@ [VarExpr (toVariable "TICKET_ALLOWED_USER_TICKET_ID"), VarExpr (toVariable "TICKET_ALLOWED_USER_NAME")]
          "TICKET_ALLOWED_USER_NAME" -> "TICKET_ALLOWED_USERS_OBJ" @@ [VarExpr (toVariable "TICKET_ALLOWED_USER_TICKET_ID"), VarExpr (toVariable "TICKET_ALLOWED_USER_NAME")]
          -- TICKET_ALLOWED_GROUPS
          "TICKET_ALLOWED_GROUP_TICKET_ID" -> "TICKET_ALLOWED_GROUPS_OBJ" @@ [VarExpr (toVariable "TICKET_ALLOWED_GROUP_TICKET_ID"), VarExpr (toVariable "TICKET_ALLOWED_GROUP_NAME")]
          "TICKET_ALLOWED_GROUP_NAME" -> "TICKET_ALLOWED_GROUPS_OBJ" @@ [VarExpr (toVariable "TICKET_ALLOWED_GROUP_TICKET_ID"), VarExpr (toVariable "TICKET_ALLOWED_GROUP_NAME")]
          -- QUOTA
          "QUOTA_RESC_NAME" -> "QUOTA_OBJ" @@ [VarExpr (toVariable "QUOTA_USER_ID"), VarExpr (toVariable "QUOTA_RESC_ID")] .*. "RESC_NAME" @@ [VarExpr (toVariable "QUOTA_RESC_ID"), VarExpr (toVariable col)]
          "QUOTA_USER_NAME" -> "QUOTA_OBJ" @@ [VarExpr (toVariable "QUOTA_USER_ID"), VarExpr (toVariable "QUOTA_RESC_ID")] .*. "USER_NAME" @@ [VarExpr (toVariable "QUOTA_USER_ID"), VarExpr (toVariable col)]
          "QUOTA_USER_ZONE" -> "QUOTA_OBJ" @@ [VarExpr (toVariable "QUOTA_USER_ID"), VarExpr (toVariable "QUOTA_RESC_ID")] .*. "USER_ZONE_NAME" @@ [VarExpr (toVariable "QUOTA_USER_ID"), VarExpr (toVariable col)]
          "QUOTA_USER_TYPE" -> "QUOTA_OBJ" @@ [VarExpr (toVariable "QUOTA_USER_ID"), VarExpr (toVariable "QUOTA_RESC_ID")] .*. "USER_TYPE_NAME" @@ [VarExpr (toVariable "QUOTA_USER_ID"), VarExpr (toVariable col)]
          "QUOTA_RESC_ID" -> "QUOTA_OBJ" @@ [VarExpr (toVariable "QUOTA_USER_ID"), VarExpr (toVariable "QUOTA_RESC_ID")]
          "QUOTA_USER_ID" -> "QUOTA_OBJ" @@ [VarExpr (toVariable "QUOTA_USER_ID"), VarExpr (toVariable "QUOTA_RESC_ID")]
          "QUOTA_LIMIT" -> "QUOTA_LIMIT" @@ [VarExpr (toVariable "QUOTA_USER_ID"), VarExpr (toVariable "QUOTA_RESC_ID"), VarExpr (toVariable col)]
          "QUOTA_OVER" -> "QUOTA_OVER" @@ [VarExpr (toVariable "QUOTA_USER_ID"), VarExpr (toVariable "QUOTA_RESC_ID"), VarExpr (toVariable col)]
          "QUOTA_MODIFY_TIME" -> "QUOTA_MODIFY_TS" @@ [VarExpr (toVariable "QUOTA_USER_ID"), VarExpr (toVariable "QUOTA_RESC_ID"), VarExpr (toVariable col)]
          "TICKET_OWNER_ZONE" -> "TICKET_USER_ID" @@ [VarExpr (toIdVariable col), VarExpr (toVariable "TICKET_USER_ID")] .*. "USER_ZONE_NAME" @@ [VarExpr (toVariable "TICKET_USER_ID"), VarExpr (toVariable "TICKET_OWNER_ZONE")]
          "TICKET_USES_COUNT" -> "TICKET_USES_COUNT" @@ [VarExpr (toIdVariable col), VarExpr (toVariable col)]
          "TICKET_USES_LIMIT" -> "TICKET_USES_LIMIT" @@ [VarExpr (toIdVariable col), VarExpr (toVariable col)]
          "TICKET_WRITE_FILE_COUNT" -> "TICKET_WRITE_FILE_COUNT" @@ [VarExpr (toIdVariable col), VarExpr (toVariable col)]
          "TICKET_WRITE_FILE_LIMIT" -> "TICKET_WRITE_FILE_LIMIT" @@ [VarExpr (toIdVariable col), VarExpr (toVariable col)]
          "TICKET_WRITE_BYTE_COUNT" -> "TICKET_WRITE_BYTE_COUNT" @@ [VarExpr (toIdVariable col), VarExpr (toVariable col)]
          "TICKET_WRITE_BYTE_LIMIT" -> "TICKET_WRITE_BYTE_LIMIT" @@ [VarExpr (toIdVariable col), VarExpr (toVariable col)]
          "TICKET_EXPIRY" -> "TICKET_EXPIRY_TS" @@ [VarExpr (toIdVariable col), VarExpr (toVariable col)]
          "TICKET_DATA_NAME" -> "TICKET_OBJECT_ID" @@ [VarExpr (toIdVariable col), VarExpr (toVariable "TICKET_OBJECT_ID")] .*. "DATA_NAME" @@ [VarExpr (toVariable "TICKET_OBJECT_ID"), VarExpr (toVariable "TICKET_OBJECT_RESC_ID"), VarExpr (toVariable "TICKET_DATA_NAME")]
          "TICKET_DATA_COLL_NAME" -> "TICKET_OBJECT_ID" @@ [VarExpr (toIdVariable col), VarExpr (toVariable "TICKET_OBJECT_ID")] .*. "DATA_COLL_ID" @@ [VarExpr (toVariable "TICKET_OBJECT_ID"), VarExpr (toVariable "TICKET_OBJECT_RESC_ID"), VarExpr (toVariable "TICKET_OBJECT_COLL_ID")] .*. "COLL_NAME" @@ [VarExpr (toVariable "TICKET_OBJECT_COLL_ID"), VarExpr (toVariable "TICKET_DATA_COLL_NAME")]
          "TICKET_COLL_NAME" -> "TICKET_OBJECT_ID" @@ [VarExpr (toIdVariable col), VarExpr (toVariable "TICKET_OBJECT_ID")] .*. "COLL_NAME" @@ [VarExpr (toVariable "TICKET_OBJECT_ID"), VarExpr (toVariable "TICKET_COLL_NAME")]

          _ -> error ("unsupported column " ++ col)

toCondPredicate2 :: [String] -> Cond ->  Formula
toCondPredicate2  cols cond@(Cond col _) =
  let form = toCondPredicate  cond in
  if col `elem` cols
    then form
    else  (translateGenQueryColumnToPredicate  col) .*. form

ancestors :: String -> [String]
ancestors = ancestor "" where
           ancestor "" ('/' : tl) = "/" : ancestor "/" tl
           ancestor a ('/' : tl) = (reverse a) : ancestor ('/' : a) tl
           ancestor a (hd : tl) = ancestor (hd : a) tl
           ancestor a "" = [reverse a]

escapeSQLTextArrayString :: String -> String
escapeSQLTextArrayString "" = ""
escapeSQLTextArrayString ('\"' : tl) = "\\\"" ++ escapeSQLTextArrayString tl
escapeSQLTextArrayString ('\\' : tl) = "\\\\" ++ escapeSQLTextArrayString tl
escapeSQLTextArrayString (hd : tl) = hd : escapeSQLTextArrayString tl

quote :: String -> String
quote a = "\"" ++ a ++ "\""


toCondPredicate :: Cond ->  Formula
toCondPredicate  (Cond col (EqString str)) =
  "eq" @@ [CastExpr TextType (VarExpr (toVariable col)), StringExpr (pack str)]
toCondPredicate  (Cond col (EqInteger str)) =
  "eq" @@ [CastExpr Int64Type (VarExpr (toVariable col)), IntExpr (fromIntegral str)]
toCondPredicate  (Cond col (NotEqString str)) =
  Aggregate Not ("eq" @@ [VarExpr (toVariable col), StringExpr (pack str)])
toCondPredicate  (Cond col (NotEqInteger str)) =
  Aggregate Not ("eq" @@ [VarExpr (toVariable col), IntExpr (fromIntegral str)])
toCondPredicate  (Cond col (GtString str)) =
  ("gt" @@ [CastExpr Int64Type (VarExpr (toVariable col)), CastExpr Int64Type (StringExpr (pack str))])
toCondPredicate  (Cond col (GtInteger str)) =
  ("gt" @@ [CastExpr Int64Type (VarExpr (toVariable col)), IntExpr (fromIntegral str)])
toCondPredicate  (Cond col (LtString str)) =
  ("lt" @@ [CastExpr Int64Type (VarExpr (toVariable col)), CastExpr Int64Type (StringExpr (pack str))])
toCondPredicate  (Cond col (LtInteger str)) =
  ("lt" @@ [CastExpr Int64Type (VarExpr (toVariable col)), IntExpr (fromIntegral str)])
toCondPredicate  (Cond col (GeString str)) =
  ("ge" @@ [CastExpr Int64Type (VarExpr (toVariable col)), CastExpr Int64Type (StringExpr (pack str))])
toCondPredicate  (Cond col (GeInteger str)) =
  ("ge" @@ [CastExpr Int64Type (VarExpr (toVariable col)), IntExpr (fromIntegral str)])
toCondPredicate  (Cond col (LeString str)) =
  ("le" @@ [CastExpr Int64Type (VarExpr (toVariable col)), CastExpr Int64Type (StringExpr (pack str))])
toCondPredicate  (Cond col (LeInteger str)) =
  ("le" @@ [CastExpr Int64Type (VarExpr (toVariable col)), IntExpr (fromIntegral str)])
toCondPredicate  (Cond col (LikeCond str)) =
  "like" @@ [VarExpr (toVariable col), StringExpr (pack str)]
toCondPredicate  (Cond col (InCond arrstr)) =
      "in" @@ [VarExpr (toVariable col), listExpr (map (StringExpr . pack) arrstr)]
toCondPredicate  (Cond col (ParentOfCond str)) =
  let arrstr = ancestors str in
      "in" @@ [VarExpr (toVariable col), listExpr (map (StringExpr . pack) arrstr)]
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
            then "OBJT_ACCESS_OBJ" @@ [VarExpr collId, VarExpr userId, VarExpr (Var "xCOLL_ACCESS_TYPE")]
            else FOne,
          if "DATA_ACCESS" `elem` tables
            then "OBJT_ACCESS_OBJ" @@ [VarExpr dataId, VarExpr userId, VarExpr (Var "xDATA_ACCESS_TYPE")]
            else FOne,
          if "META_DATA" `elem` tables
            then "OBJT_METAMAP_OBJ" @@ [VarExpr (Var "xDATA_ID"), VarExpr (Var "xMETA_DATA_ID")]
            else FOne,
          if "META_COLL" `elem` tables
            then "OBJT_METAMAP_OBJ" @@ [VarExpr (Var "xCOLL_ID"), VarExpr (Var "xMETA_COLL_ID")]
            else FOne,
          if "META_RESC" `elem` tables
            then "OBJT_METAMAP_OBJ" @@ [VarExpr (Var "xRESC_ID"), VarExpr (Var "xMETA_RESC_ID")]
            else FOne,
          if "META_USER" `elem` tables
            then "OBJT_METAMAP_OBJ" @@ [VarExpr (Var "xUSER_ID"), VarExpr (Var "xMETA_USER_ID")]
            else FOne
        ]
addAccessControl :: String -> String -> Var -> Formula -> Formula
addAccessControl uz un id form =
    form .*. "HAS_READ_PERMISSION" @@ [StringExpr (pack uz), StringExpr (pack un), VarExpr id]
 
addAccessControls :: String -> String -> [String] -> Formula -> Formula
addAccessControls _ _ [] form = form
addAccessControls uz un ("DATA" : tabs) form = addAccessControls uz un tabs (addAccessControl uz un dataId form)
addAccessControls uz un ("COLL" : tabs) form = addAccessControls uz un tabs (addAccessControl uz un collId form)
addAccessControls uz un (_ : tabs) form = addAccessControls uz un tabs form

addTicketAccessControlData :: String -> Var -> Formula -> Formula
addTicketAccessControlData ticket id form =
    form .*. "HAS_READ_PERMISSION_TICKET_DATA" @@ [StringExpr (pack ticket), VarExpr id]

addTicketAccessControlColl :: String -> Var -> Formula -> Formula
addTicketAccessControlColl ticket id form =
    form .*. "HAS_READ_PERMISSION_TICKET_COLL" @@ [StringExpr (pack ticket), VarExpr id]

addTicketAccessControls :: String -> [String] -> Formula -> Formula
addTicketAccessControls ticket tabs form = 
          if "DATA" `elem` tabs
              then addTicketAccessControlData ticket dataId form
              else if "COLL" `elem` tabs
                  then addTicketAccessControlColl ticket collId form
                  else form

addAccessControlTicket :: Formula -> Formula
addAccessControlTicket form =
    form .*. "TICKET_USER_ID" @@ [VarExpr (toVariable "TICKET_ID"), VarExpr (toVariable "USER_ID")]

addAccessControlsTicket :: [String] -> Formula -> Formula
addAccessControlsTicket [] form = form
addAccessControlsTicket ("TICKET" : tabs) form = addAccessControlsTicket tabs (addAccessControlTicket form)
addAccessControlsTicket (_ : tabs) form = addAccessControlsTicket tabs form

translateGenQueryToQAL :: Bool -> AccessControl -> GenQuery -> ([Var],  Formula)
translateGenQueryToQAL distinct addaccessctl gq@(GenQuery sels conds) =
      if null sels
        then error "cannot translate genquery cols null"
        else
          let
              agg = not (null (filter (\(Sel _ a) -> case a of 
                                                           GQCount -> True
                                                           _ -> False) sels))
              cols = map (\(Sel col _) -> col) sels
              vars = map toVariable cols
              tabs = nub (map extractPrefix cols) 
              form0 = foldr (.*.) FOne (nub (map translateGenQueryColumnToPredicate cols ++ toJoinPredicate  cols conds ++ map (toCondPredicate2  cols) conds))
              form1a = case addaccessctl of
                                             UserAccessControl uz un -> addAccessControls uz un tabs form0
                                             TicketAccessControl ticket -> addTicketAccessControls ticket tabs form0
                                             _ -> form0
              form1 = case addaccessctl of
                                             AccessControlTicket -> form1a
                                             _ -> addAccessControlsTicket tabs form1a
              form3 = if agg 
                         then
                             let colaggs = map (\(Sel col sel) -> case sel of
                                                                      GQCount -> if distinct
                                                                                     then CountDistinct                                                                                    
                                                                                     else const Count
                                                                      GQSum -> Sum
                                                                      GQMax -> Max
                                                                      GQMin -> Min
                                                                      None -> Random
                                                                      _ -> error ("translateGenQueryToQAL: unsupported selector: " ++ show sel)) sels
                                 aggs = zip vars (zipWith ($) colaggs vars)
                             in
                                 Aggregate (Summarize aggs []) form1
                         else 
                             let form2 = if distinct then Aggregate Distinct form1 else form1
                                 orders = concatMap (\(Sel col sel) -> case sel of
                                                                              GQOrderDesc -> [(col, OrderByDesc)]
                                                                              GQOrderAsc -> [(col, OrderByAsc)]
                                                                              _ -> []) sels ++ (if "COLL_NAME" `elem` cols then [("COLL_NAME", OrderByAsc)] else [])
                                                                                            ++ (if "DATA_NAME" `elem` cols then [("DATA_NAME", OrderByAsc)] else [])
                                                                                            ++ (if "DATA_REPL_NUM" `elem` cols then [("DATA_REPL_NUM", OrderByAsc)] else [])
                             in
                                 foldr (\(col, ord) form -> Aggregate (ord (toVariable col)) form) form2 orders
              form = Aggregate (FReturn vars) form3 
          in
              (vars, form)
