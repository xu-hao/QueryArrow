{-# LANGUAGE FlexibleContexts, MultiParamTypeClasses, FlexibleInstances, OverloadedStrings, GADTs, ExistentialQuantification, ForeignFunctionInterface #-}

module QueryArrow.C.GenQuery where

import FO.Data (Pred, Formula(..), Var(..), Expr(..), Atom(..), Aggregator(..), Summary(..), Lit(..), Sign(..))
import DB.DB
import QueryPlan
import DB.ResultStream
import Config
import DBMap(transDB)
import Utils(constructDBPredMap)

import Prelude hiding (lookup)
import Data.Set (Set, singleton, fromList, empty)
import Data.Text (Text, pack)
import qualified Data.Text as Text
import Data.Monoid ((<>))
import Control.Exception (catch, SomeException)
import Control.Monad.Reader
import Control.Applicative (liftA2, pure)
import Control.Monad.Trans.Resource (runResourceT)
import Data.Map.Strict (lookup)
import qualified Data.Map.Strict as Map
import Control.Monad.Error.Class (throwError)
import Control.Monad.IO.Class (liftIO)
import Foreign.StablePtr
import Foreign.Ptr
import Foreign.C.Types
import Foreign.C.String
import Foreign.Storable
import Foreign.Marshal.Array
import System.Log.Logger (errorM, infoM)
import Logging
import Data.List (find, intercalate, nub)
import Data.Maybe (fromMaybe)
import Data.Char (toLower)
import Control.Monad.Trans.Either
import Debug.Trace
import Text.Parsec (runParser)

import QueryArrow.C.PluginGen
import QueryArrow.C.GenQuery.Parser
import QueryArrow.C.GenQuery.Data
import QueryArrow.C.Template

prefixes :: [String]
prefixes = ["DATA_ACCESS", "COLL_ACCESS", "DATA", "COLL", "RESC", "ZONE", "USER"]

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

translateGenQueryColumnToPredicate :: Predicates -> String -> Formula
translateGenQueryColumnToPredicate predicates col =
  let
      collower = map toLower col
  in
      case collower of
          "resc_id" -> FAtomic (Atom (_resc_obj predicates) [VarExpr (toVariable col)])
          "coll_id" -> FAtomic (Atom (_coll_obj predicates) [VarExpr (toVariable col)])
          "data_id" -> FAtomic (Atom (_data_obj predicates) [VarExpr (toVariable col), VarExpr dataRescId])
          "data_resc_id" -> FAtomic (Atom (_data_obj predicates) [VarExpr (toIdVariable col), VarExpr dataRescId])
          "data_repl_num" -> FAtomic (Atom (_data_repl_num predicates) [VarExpr (toIdVariable col), VarExpr dataRescId, VarExpr (toVariable col)])
          "data_coll_id" -> FAtomic (Atom (_data_coll_id predicates) [VarExpr (toIdVariable col), VarExpr dataRescId, VarExpr (toVariable col)])
          "data_version" -> FAtomic (Atom (_data_version predicates) [VarExpr (toIdVariable col), VarExpr dataRescId, VarExpr (toVariable col)])
          "data_status" -> FAtomic (Atom (_data_status predicates) [VarExpr (toIdVariable col), VarExpr dataRescId, VarExpr (toVariable col)])
          "data_type_name" -> FAtomic (Atom (_data_type_name predicates) [VarExpr (toIdVariable col), VarExpr dataRescId, VarExpr (toVariable col)])
          "data_map_id" -> FAtomic (Atom (_data_map_id predicates) [VarExpr (toIdVariable col), VarExpr dataRescId, VarExpr (toVariable col)])
          "data_size" -> FAtomic (Atom (_data_size predicates) [VarExpr (toIdVariable col), VarExpr dataRescId, VarExpr (toVariable col)])
          "data_path" -> FAtomic (Atom (_data_path predicates) [VarExpr (toIdVariable col), VarExpr dataRescId, VarExpr (toVariable col)])
          "data_name" -> FAtomic (Atom (_data_name predicates) [VarExpr (toIdVariable col), VarExpr dataRescId, VarExpr (toVariable col)])
          "data_owner_name" -> FAtomic (Atom (_data_owner_name predicates) [VarExpr (toIdVariable col), VarExpr dataRescId, VarExpr (toVariable col)])
          "data_owner_zone" -> FAtomic (Atom (_data_owner_zone predicates) [VarExpr (toIdVariable col), VarExpr dataRescId, VarExpr (toVariable col)])
          "data_is_dirty" -> FAtomic (Atom (_data_is_dirty predicates) [VarExpr (toIdVariable col), VarExpr dataRescId, VarExpr (toVariable col)])
          "data_checksum" -> FAtomic (Atom (_data_checksum predicates) [VarExpr (toIdVariable col), VarExpr dataRescId, VarExpr (toVariable col)])
          "data_expiry" -> FAtomic (Atom (_data_expiry_ts predicates) [VarExpr (toIdVariable col), VarExpr dataRescId, VarExpr (toVariable col)])
          "data_comments" -> FAtomic (Atom (_data_comment predicates) [VarExpr (toIdVariable col), VarExpr dataRescId, VarExpr (toVariable col)])
          "data_create_time" -> FAtomic (Atom (_data_create_ts predicates) [VarExpr (toIdVariable col), VarExpr dataRescId, VarExpr (toVariable col)])
          "data_modify_time" -> FAtomic (Atom (_data_modify_ts predicates) [VarExpr (toIdVariable col), VarExpr dataRescId, VarExpr (toVariable col)])
          "data_mode" -> FAtomic (Atom (_data_mode predicates) [VarExpr (toIdVariable col), VarExpr dataRescId, VarExpr (toVariable col)])
          "data_resc_hier" -> FAtomic (Atom (_data_resc_hier predicates) [VarExpr (toIdVariable col), VarExpr dataRescId, VarExpr (toVariable col)])
          "data_resc_name" -> FAtomic (Atom (_data_resc_name predicates) [VarExpr (toIdVariable col), VarExpr dataRescId, VarExpr (toVariable col)])
          "data_repl_status" -> FAtomic (Atom (_data_status predicates) [VarExpr (toIdVariable col), VarExpr dataRescId, VarExpr (toVariable col)])
          "column_name_not_found_421" -> FAtomic (Atom (_data_mode predicates) [VarExpr (toIdVariable col), VarExpr dataRescId, VarExpr (toVariable col)])
          "zone_name" -> FAtomic (Atom (_zone_name predicates) [VarExpr (toIdVariable col), VarExpr (toVariable col)])
          "zone_type" -> FAtomic (Atom (_zone_type_name predicates) [VarExpr (toIdVariable col), VarExpr (toVariable col)])
          "zone_connection" -> FAtomic (Atom (_zone_conn_string predicates) [VarExpr (toIdVariable col), VarExpr (toVariable col)])
          "zone_comment" -> FAtomic (Atom (_zone_comment predicates) [VarExpr (toIdVariable col), VarExpr (toVariable col)])
          "resc_name" -> FAtomic (Atom (_resc_name predicates) [VarExpr (toIdVariable col), VarExpr (toVariable col)])
          "resc_zone_name" -> FAtomic (Atom (_resc_zone_name predicates) [VarExpr (toIdVariable col), VarExpr (toVariable col)])
          "resc_type_name" -> FAtomic (Atom (_resc_type_name predicates) [VarExpr (toIdVariable col), VarExpr (toVariable col)])
          "resc_class_name" -> FAtomic (Atom (_resc_class_name predicates) [VarExpr (toIdVariable col), VarExpr (toVariable col)])
          "resc_loc" -> FAtomic (Atom (_resc_net predicates) [VarExpr (toIdVariable col), VarExpr (toVariable col)])
          "resc_vault_path" -> FAtomic (Atom (_resc_def_path predicates) [VarExpr (toIdVariable col), VarExpr (toVariable col)])
          "resc_free_space" -> FAtomic (Atom (_resc_free_space predicates) [VarExpr (toIdVariable col), VarExpr (toVariable col)])
          "resc_info" -> FAtomic (Atom (_resc_info predicates) [VarExpr (toIdVariable col), VarExpr (toVariable col)])
          "resc_comment" -> FAtomic (Atom (_resc_comment predicates) [VarExpr (toIdVariable col), VarExpr (toVariable col)])
          "resc_create_time" -> FAtomic (Atom (_resc_create_ts predicates) [VarExpr (toIdVariable col), VarExpr (toVariable col)])
          "resc_modify_time" -> FAtomic (Atom (_resc_modify_ts predicates) [VarExpr (toIdVariable col), VarExpr (toVariable col)])
          "resc_status" -> FAtomic (Atom (_resc_status predicates) [VarExpr (toIdVariable col), VarExpr (toVariable col)])
          "resc_children" -> FAtomic (Atom (_resc_children predicates) [VarExpr (toIdVariable col), VarExpr (toVariable col)])
          "resc_context" -> FAtomic (Atom (_resc_context predicates) [VarExpr (toIdVariable col), VarExpr (toVariable col)])
          "resc_parent" -> FAtomic (Atom (_resc_parent predicates) [VarExpr (toIdVariable col), VarExpr (toVariable col)])
          "resc_parent_context" -> FAtomic (Atom (_resc_parent_context predicates) [VarExpr (toIdVariable col), VarExpr (toVariable col)])
          "coll_inheritance" -> FAtomic (Atom (_coll_inheritance predicates) [VarExpr (toIdVariable col), VarExpr (toVariable col)])
          "coll_name" -> FAtomic (Atom (_coll_name predicates) [VarExpr (toIdVariable col), VarExpr (toVariable col)])
          "coll_parent_name" -> FAtomic (Atom (_coll_parent_coll_name predicates) [VarExpr (toIdVariable col), VarExpr (toVariable col)])
          "coll_owner_name" -> FAtomic (Atom (_coll_owner_name predicates) [VarExpr (toIdVariable col), VarExpr (toVariable col)])
          "coll_owner_zone" -> FAtomic (Atom (_coll_owner_zone predicates) [VarExpr (toIdVariable col), VarExpr (toVariable col)])
          "coll_create_time" -> FAtomic (Atom (_coll_create_ts predicates) [VarExpr (toIdVariable col), VarExpr (toVariable col)])
          "coll_modify_time" -> FAtomic (Atom (_coll_modify_ts predicates) [VarExpr (toIdVariable col), VarExpr (toVariable col)])
          "column_name_not_found_510" -> FAtomic (Atom (_coll_type predicates) [VarExpr (toIdVariable col), VarExpr (toVariable col)])
          "column_name_not_found_511" -> FAtomic (Atom (_coll_info1 predicates) [VarExpr (toIdVariable col), VarExpr (toVariable col)])
          "column_name_not_found_512" -> FAtomic (Atom (_coll_info2 predicates) [VarExpr (toIdVariable col), VarExpr (toVariable col)])
          -- user (coll)
          "column_name_not_found_1300" -> FAtomic (Atom (_user_name predicates) [VarExpr (toIdVariable col), VarExpr (toVariable col)])
          "column_name_not_found_1301" -> FAtomic (Atom (_user_zone_name predicates) [VarExpr (toIdVariable col), VarExpr (toVariable col)])
          -- user
          "user_name" -> FAtomic (Atom (_user_name predicates) [VarExpr (toIdVariable col), VarExpr (toVariable col)])
          "user_zone" -> FAtomic (Atom (_user_zone_name predicates) [VarExpr (toIdVariable col), VarExpr (toVariable col)])
          -- access (coll)
          "coll_access_name" -> FAtomic (Atom (_tokn_token_name predicates) [VarExpr (toIdVariable col), VarExpr (toVariable col)])
          "coll_token_namespace" -> FAtomic (Atom (_tokn_token_namespace predicates) [VarExpr (toIdVariable col), VarExpr (toVariable col)])
          -- access (data)
          "data_access_name" -> FAtomic (Atom (_tokn_token_name predicates) [VarExpr (toIdVariable col), VarExpr (toVariable col)])
          "data_token_namespace" -> FAtomic (Atom (_tokn_token_namespace predicates) [VarExpr (toIdVariable col), VarExpr (toVariable col)])
          "data_access_data_id" -> FAtomic (Atom (_data_obj predicates) [VarExpr (toVariable col), VarExpr dataRescId])
          _ -> error ("unsupported column " ++ col)

toCondPredicate2 :: Predicates -> [String] -> Cond -> Formula
toCondPredicate2 predicates cols cond@(Cond col _) =
  let form = toCondPredicate predicates cond in
  if col `elem` cols
    then form
    else FSequencing (translateGenQueryColumnToPredicate predicates col) form

toCondPredicate :: Predicates -> Cond -> Formula
toCondPredicate predicates (Cond col (EqString str)) =
  FAtomic (Atom (_eq predicates) [VarExpr (toVariable col), StringExpr (pack str)])
toCondPredicate predicates (Cond col (EqInteger str)) =
  FAtomic (Atom (_eq predicates) [VarExpr (toVariable col), IntExpr (fromIntegral str)])
toCondPredicate predicates (Cond col (NotEqString str)) =
  Aggregate Not (FAtomic (Atom (_eq predicates) [VarExpr (toVariable col), StringExpr (pack str)]))
toCondPredicate predicates (Cond col (NotEqInteger str)) =
  Aggregate Not (FAtomic (Atom (_eq predicates) [VarExpr (toVariable col), IntExpr (fromIntegral str)]))
toCondPredicate predicates (Cond col (LikeCond str)) =
  FAtomic (Atom (_like predicates) [VarExpr (toVariable col), StringExpr (pack str)])
toCondPredicate predicates (Cond col (ParentOfCond str)) =
  foldl1 FSequencing [
      FAtomic (Atom (_coll_name predicates) [VarExpr (Var "cid"), StringExpr (pack str)]),
      FAtomic (Atom (_coll_parent_coll_name predicates) [VarExpr (Var "cid"), VarExpr (toVariable col)])
  ]
toCondPredicate predicates (Cond col (AndCond a b)) =
  foldl1 FSequencing [
      toCondPredicate predicates (Cond col a),
      toCondPredicate predicates (Cond col b)
  ]
toCondPredicate predicates (Cond col (OrCond a b)) =
  foldl1 FChoice [
      toCondPredicate predicates (Cond col a),
      toCondPredicate predicates (Cond col b)
  ]


toJoinPredicate :: Predicates -> [String] -> [Cond] -> [Formula]
toJoinPredicate predicates cols conds =
    let
        tables = nub (map extractPrefix cols ++ map (\(Cond col _) -> col) conds)
    in
        [
          if "COLL" `elem` tables && "DATA" `elem` tables
            then FAtomic (Atom (_data_coll_id predicates) [VarExpr (Var "xDATA_ID"), VarExpr (Var "xDATA_RESC_ID"), VarExpr (Var "xCOLL_ID")])
            else FOne,
          if "COLL_ACCESS" `elem` tables
            then foldl1 FSequencing [
              FAtomic (Atom (_objt_access_obj predicates) [VarExpr (Var "xCOLL_ID"), VarExpr (Var "xUSER_ID"), VarExpr (Var "xCOLL_ACCESS_ID")])
            ]
            else FOne,
          if "DATA_ACCESS" `elem` tables
            then foldl1 FSequencing [
              FAtomic (Atom (_objt_access_obj predicates) [VarExpr (Var "xDATA_ID"), VarExpr (Var "xUSER_ID"), VarExpr (Var "xDATA_ACCESS_ID")])
            ]
            else FOne
        ]

translateGenQueryToQAL :: Predicates -> GenQuery -> ([Var], Formula)
translateGenQueryToQAL predicates (GenQuery sels conds) =
      if null sels
        then error "cannot translate genquery cols null"
        else
          let
              cols = map (\(Sel col _) -> col) sels
              vars = map toVariable cols
              form0 = foldr FSequencing (FReturn vars) (map (translateGenQueryColumnToPredicate predicates) cols ++ toJoinPredicate predicates cols conds ++ map (toCondPredicate2 predicates cols) conds)
              orders = map (\(Sel col _) -> col) (filter (\(Sel _ sel) -> case sel of
                                                                              Order -> True
                                                                              _ -> False) sels)
              form = foldr (\col form -> Aggregate (OrderByAsc (toVariable col)) form) form0 orders
          in
              (vars, form)

foreign export ccall hs_gen_query :: StablePtr (Session Predicates) -> CString -> Ptr (Ptr CString) -> Ptr CInt -> Ptr CInt -> IO Int
hs_gen_query :: StablePtr (Session Predicates) -> CString -> Ptr (Ptr CString) -> Ptr CInt -> Ptr CInt -> IO Int
hs_gen_query sessionptr cqu cout ccol crow = do
  session@(Session _ _ predicates) <- deRefStablePtr sessionptr
  qu <- peekCString cqu
  putStrLn ("genquery = " ++ qu)
  let (vars, form) = case runParser genQueryP () "" qu of
                Left err -> error (show err)
                Right gq -> translateGenQueryToQAL predicates gq
  res <- runEitherT (getResults session vars form mempty)
  case res of
    Left err -> error (show err)
    Right res -> do
      let col = length vars
      let row = length res
      if row == 0
        then return eCAT_NO_ROWS_FOUND
        else do
          poke ccol (fromIntegral col)
          poke crow (fromIntegral row)
          infoM "Plugin" ("GenQuery " ++ show res)
          arrelems <- mapM newCString (concatMap (\r -> map (\v -> Text.unpack (resultValueToString (fromMaybe (error ("cannot find column " ++ show v ++ show r)) (lookup v r)))) vars ) res)
          arr <- newArray arrelems
          poke cout arr
          return 0
