{-# LANGUAGE FlexibleContexts, MultiParamTypeClasses, FlexibleInstances, OverloadedStrings, GADTs, ExistentialQuantification, ForeignFunctionInterface #-}

module QueryArrow.FFI.C.Plugin where

import QueryArrow.FO.Data
import QueryArrow.FO.Utils
import QueryArrow.DB.DB
import QueryArrow.QueryPlan
import QueryArrow.DB.ResultStream
import QueryArrow.FFI.C.Template
import QueryArrow.Config
import QueryArrow.Utils(constructDBPredMap)

import Prelude hiding (lookup)
import Data.Set (Set, singleton, fromList, empty)
import Data.Text (Text, pack, unpack)
import qualified Data.Text as Text
import Data.Monoid ((<>))
import Control.Exception (catch, SomeException)
import Control.Monad.Reader
import Control.Applicative (liftA2, pure)
import Control.Monad.Trans.Resource (runResourceT)
import Data.Map.Strict (lookup)
import qualified Data.Map.Strict as Map
import Control.Monad.Trans.Either (EitherT, runEitherT)
import Control.Monad.Error.Class (throwError)
import Control.Monad.IO.Class (liftIO)
import Foreign.StablePtr
import Foreign.Ptr
import Foreign.C.Types
import Foreign.C.String
import Foreign.Storable
import Foreign.Marshal.Array
import System.Log.Logger (errorM, infoM, Priority(..))
import Data.Maybe (fromMaybe)
import Control.Monad (foldM)
import QueryArrow.Logging
import QueryArrow.FFI.Service
import QueryArrow.FFI.Auxiliary

foreign export ccall hs_setup :: IO ()
hs_setup :: IO ()
hs_setup = setup INFO

foreign export ccall hs_connect :: StablePtr (QueryArrowService b) -> CString -> Ptr (StablePtr b) -> IO Int
hs_connect :: StablePtr (QueryArrowService b) -> CString -> Ptr (StablePtr b) -> IO Int
hs_connect svcptr cpath ptr = do
    svc <- deRefStablePtr svcptr
    path <- peekCString cpath
    res <- runEitherT (qasConnect svc path)
    case res of
        Right session -> do
                sessionptr <- newStablePtr session
                poke ptr sessionptr
                return 0
        Left e -> do
                errorM "C Api" (show e)
                return (0-1)

foreign export ccall hs_disconnect :: StablePtr (QueryArrowService b) -> StablePtr b -> IO Int
hs_disconnect :: StablePtr (QueryArrowService b) -> StablePtr b -> IO Int
hs_disconnect svcptr stptr = do
    svc <- deRefStablePtr svcptr
    session <- deRefStablePtr stptr
    res <- runEitherT (qasDisconnect svc session)
    case res of
        Right _ ->
            return 0
        Left e -> do
            errorM "C Api" (show e)
            return (0-1)

foreign export ccall hs_commit :: StablePtr (QueryArrowService b) -> StablePtr b -> IO Int
hs_commit :: StablePtr (QueryArrowService b) -> StablePtr b -> IO Int
hs_commit svcptr sessionptr = do
  svc <- deRefStablePtr svcptr
  session <- deRefStablePtr sessionptr
  res <- runEitherT (qasCommit svc session)
  case res of
      Right _ ->
          return 0
      Left e -> do
          errorM "C Api" (show e)
          return (0-1)

foreign export ccall hs_rollback :: StablePtr (QueryArrowService b) -> StablePtr b -> IO Int
hs_rollback :: StablePtr (QueryArrowService b) -> StablePtr b -> IO Int
hs_rollback svcptr sessionptr = do
  svc <- deRefStablePtr svcptr
  session <- deRefStablePtr sessionptr
  res <- runEitherT (qasRollback svc session)
  case res of
      Right _ ->
          return 0
      Left e -> do
          errorM "C Api" (show e)
          return (0-1)

foreign export ccall hs_modify_data :: StablePtr (QueryArrowService b) -> StablePtr b -> Ptr CString -> Ptr CString -> Ptr CString -> Ptr CString -> Int -> Int -> IO Int
hs_modify_data :: StablePtr (QueryArrowService b) -> StablePtr b -> Ptr CString -> Ptr CString -> Ptr CString -> Ptr CString -> Int -> Int -> IO Int
hs_modify_data svcptr sessionptr cupdatecols cupdatevals cwherecolsandops cwherevals cupcols cnumwheres = do
    svc <- deRefStablePtr svcptr
    session <- deRefStablePtr sessionptr
    let upcols = fromIntegral cupcols
    let numwheres = fromIntegral cnumwheres
    cupdatecols2 <- peekArray upcols cupdatecols
    updatecols <- mapM peekCString cupdatecols2
    cupdatevals2 <- peekArray upcols cupdatevals
    updatevals <- mapM peekCString cupdatevals2
    cwherecolsandops2 <- peekArray numwheres cwherecolsandops
    wherecolsandops <- mapM peekCString cwherecolsandops2
    cwherevals2 <- peekArray numwheres cwherevals
    wherevals <- mapM peekCString cwherevals2
    let parse wherecolandcond =
            if drop (length wherecolandcond - 2) wherecolandcond == "!="
                then (take (length wherecolandcond - 2) wherecolandcond, "!=")
                else (take (length wherecolandcond - 1) wherecolandcond, "=")
    let (wherecols, whereops) = unzip (map parse wherecolsandops)
    let wherepredicate wherecol0 = do
            case wherecol0 of
                    "data_repl_num" -> "DATA_REPL_NUM"
                    "data_type_name" -> "DATA_TYPE_NAME"
                    "data_size" -> "DATA_SIZE"
                    -- "resc_name" ">" _data_resc_name
                    "data_path" -> "DATA_PATH"
                    "data_owner_name" -> "DATA_OWNER_NAME"
                    "data_owner_zone" -> "DATA_OWNER_ZONE"
                    "data_is_dirty" -> "DATA_IS_DIRTY"
                    "data_checksum" -> "DATA_CHECKSUM"
                    "data_expiry_ts" -> "DATA_EXPIRY_TS"
                    "r_comment" -> "DATA_COMMENT"
                    "create_ts" -> "DATA_CREATE_TS"
                    "modify_ts" -> "DATA_MODIFY_TS"
                    "data_mode" -> "DATA_MODE"
                    "resc_hier" -> "DATA_RESC_HIER"
                    _ -> error ("unsupported column " ++ wherecol0)
    let colval wherecol0 val = do
            case wherecol0 of
                    "data_repl_num" -> AbstractResultValue (Int64Value (read val))
                    "data_type_name" -> AbstractResultValue (StringValue (pack val))
                    "data_size" -> AbstractResultValue (Int64Value (read val))
                    "data_id" -> AbstractResultValue (Int64Value (read val))
                    "resc_id" -> AbstractResultValue (Int64Value (read val))
                    "data_path" -> AbstractResultValue (StringValue (pack val))
                    "data_owner_name" -> AbstractResultValue (StringValue (pack val))
                    "data_owner_zone" -> AbstractResultValue (StringValue (pack val))
                    "data_is_dirty" -> AbstractResultValue (Int64Value (read val))
                    "data_checksum" -> AbstractResultValue (StringValue (pack val))
                    "data_expiry_ts" -> AbstractResultValue (StringValue (pack val))
                    "r_comment" -> AbstractResultValue (StringValue (pack val))
                    "create_ts" -> AbstractResultValue (StringValue (pack val))
                    "modify_ts" -> AbstractResultValue (StringValue (pack val))
                    "data_mode" -> AbstractResultValue (StringValue (pack val))
                    "resc_hier" -> AbstractResultValue (StringValue (pack val))
                    _ -> error ("unsupported column " ++ wherecol0)
    let whereatom pre wherecol0 =
          let wherecol = var (pre ++ wherecol0)
              p = wherepredicate wherecol0 in
                  p @@ [var "w_data_id", var "w_resc_id", wherecol]
    let updateatom pre wherecol0 =
          let wherecol = var (pre ++ wherecol0)
              p = wherepredicate wherecol0 in
                  p @@+ [var "w_data_id", var "w_resc_id", wherecol]
    let whereform wherecol | wherecol /= "data_id" && wherecol /= "resc_id" = whereatom "w_" wherecol
                           | otherwise = FOne
    let wherelit wherecol "!=" =
            Aggregate Not (whereform wherecol)
        wherelit wherecol "=" =
            whereform wherecol
    let cond = foldl (.*.) ("DATA_OBJ" @@ [var "w_data_id", var "w_resc_id"]) (zipWith wherelit wherecols whereops )
    let updateform wherecol | wherecol /= "data_id" && wherecol /= "resc_id" = updateatom "u_" wherecol
                            | wherecol == "resc_id" = FOne
                            | otherwise = error ("cannot update " ++ wherecol)
    let update = foldl (.*.) cond (map updateform updatecols) .*. 
                     if "resc_id" `elem` updatecols
                         then "UPDATE_RESC_ID" @@ [var "w_data_id", var "w_resc_id", var "u_resc_id"]
                         else FOne
    let whereparam col val = (Var ("w_" ++ col), colval col val)
    let updateparam col val = (Var ("u_" ++ col), colval col val)
    let params = Map.fromList (zipWith whereparam wherecols wherevals) <> Map.fromList (zipWith updateparam updatecols updatevals)
    processRes (execAbstract svc session ( update) params) (const (return ()))

foreign export ccall hs_modify_coll :: StablePtr (QueryArrowService a) -> StablePtr a -> Ptr CString -> Ptr CString -> Int -> CString -> IO Int
hs_modify_coll :: StablePtr (QueryArrowService a) -> StablePtr a -> Ptr CString -> Ptr CString -> Int -> CString -> IO Int
hs_modify_coll svcptr sessionptr cupdatecols cupdatevals cupcols ccn = do
    svc <- deRefStablePtr svcptr
    let upcols = fromIntegral cupcols
    session <- deRefStablePtr sessionptr
    cupdatecols2 <- peekArray upcols cupdatecols
    updatecols <- mapM peekCString cupdatecols2
    cupdatevals2 <- peekArray upcols cupdatevals
    updatevals <- mapM peekCString cupdatevals2
    cn <- peekCString ccn
    let cond = "COLL_OBJ" @@ [var "cid"] .*. "COLL_NAME" @@ [var "cid", var "w_coll_name"]
    let wherepredicate wherecol0 =
            case wherecol0 of
                    "coll_type" -> "COLL_TYPE"
                    "coll_info1" -> "COLL_INFO1"
                    "coll_info2" -> "COLL_INFO2"
                    "modify_ts" -> "COLL_MODIFY_TS"
    let colval wherecol0 val =
            case wherecol0 of
                    "coll_id" -> AbstractResultValue (Int64Value (read val))
                    "coll_type" -> AbstractResultValue (StringValue (pack val))
                    "coll_info1" -> AbstractResultValue (StringValue (pack val))
                    "coll_info2" -> AbstractResultValue (StringValue (pack val))
                    "modify_ts" -> AbstractResultValue (StringValue (pack val))
    let updateatom wherecol0 =
          let wherecol = var ("u_" ++ wherecol0)
              p = wherepredicate wherecol0 in
              p @@+ [var "cid", wherecol]
    let updateform updatecol = updateatom updatecol
    let update = foldl (.*.) cond (map updateform updatecols)
    let updateparam col val = (Var ("u_" ++ col), colval col val)
    let params = Map.singleton (Var "w_coll_name") (AbstractResultValue (StringValue (pack cn))) <> Map.fromList (zipWith updateparam updatecols updatevals)
    processRes (execAbstract svc session ( update) params) (const (return ()))

foreign export ccall hs_modify_rule_exec :: StablePtr (QueryArrowService a) -> StablePtr a -> Ptr CString -> Ptr CString -> Int -> CInt -> IO Int
hs_modify_rule_exec :: StablePtr (QueryArrowService a) -> StablePtr a -> Ptr CString -> Ptr CString -> Int -> CInt -> IO Int
hs_modify_rule_exec svcptr sessionptr cupdatecols cupdatevals cupcols crexeid= do
    svc <- deRefStablePtr svcptr
    let upcols = fromIntegral cupcols
    session <- deRefStablePtr sessionptr
    cupdatecols2 <- peekArray upcols cupdatecols
    updatecols <- mapM peekCString cupdatecols2
    cupdatevals2 <- peekArray upcols cupdatevals
    updatevals <- mapM peekCString cupdatevals2
    let ruleexecid = fromIntegral crexeid
    let cond = "RULE_EXEC_OBJ" @@ [var "cid"] 
    let wherepredicate wherecol0 =
            case wherecol0 of
                "rule_name" -> "RULE_EXEC_RULE_NAME"
                "rei_file_path" -> "RULE_EXEC_REI_FILE_PATH"
                "user_name" -> "RULE_EXEC_USER_NAME"
                "exe_address" -> "RULE_EXEC_EXE_ADDRESS"
                "exe_time" -> "RULE_EXEC_EXE_TIME"
                "exe_frequency" -> "RULE_EXEC_EXE_FREQUENCY"
                "priority" -> "RULE_EXEC_PRIORITY"
                "estimated_exe_time" -> "RULE_EXEC_ESTIMATED_EXE_TIME"
                "notification_addr" -> "RULE_EXEC_NOTIFICATION_ADDR"
                "last_exe_time" -> "RULE_EXEC_LAST_EXEC_TIME"
                "exe_status" -> "RULE_EXEC_EXE_STATUS"
                "create_ts" -> "RULE_EXEC_CREATE_TS"
                "modify_ts" -> "RULE_EXEC_MODIFY_TS"
    let colval wherecol0 val =
            case wherecol0 of
                    _ -> AbstractResultValue (StringValue (pack val))
    let updateatom wherecol0 =
          let wherecol = var ("u_" ++ wherecol0)
              p = wherepredicate wherecol0 in
              p @@+ [var "cid", wherecol]
    let updateform updatecol = updateatom updatecol
    let update = foldl (.*.) cond (map updateform updatecols)
    let updateparam col val = (Var ("u_" ++ col), colval col val)
    let params = Map.singleton (Var "cid") (AbstractResultValue (Int64Value ruleexecid)) <> Map.fromList (zipWith updateparam updatecols updatevals)
    processRes (execAbstract svc session ( update) params) (const (return ()))

mapResultRowToList :: [Var] -> MapResultRow -> [AbstractResultValue]
mapResultRowToList vars r =
    map (\v -> fromMaybe (error ("mapResultRowToList: cannot find var " ++ show v ++ show r)) (lookup v r)) vars

-- foreign export ccall hs_command :: StablePtr (Session) -> Ptr CString -> CInt -> CString -> Ptr CString -> Ptr CString -> CInt -> Ptr (Ptr (Ptr CString)) -> IO Int
-- hs_command :: StablePtr (Session) -> Ptr CString -> CInt -> CString -> Ptr CString -> Ptr CString -> CInt -> Ptr (Ptr (Ptr CString)) -> IO Int
-- hs_command sessionptr cvararr cnvars cqu cparamnamearr cparamarr cnparams cout = do
--     session@(Session _ _ predicates) <- deRefStablePtr sessionptr
--     qu <- peekCString cqu
--     let nvars = fromIntegral cnvars
--     vars <- peekArray nvars cvararr >>= mapM peekCString
--     let quvars = map Var vars
--     let nparams = fromIntegral cnparams
--     paramnames <- peekArray nparams cparamnamearr >>= mapM peekCString
--     params <- peekArray nparams cparamarr >>= mapM peekCString
--     let env = Map.fromList (zip (map Var paramnames) (map (StringValue . pack) params))
--     case runParser progp (mempty, , mempty) "" qu of
--         Left err -> error (show err)
--         Right (Execute qu, _) ->
--             processRes2 (getAllResult session quvars qu env) (\res -> do
--                     let resvals = map (map (unpack . resultValueToString) . mapResultRowToList quvars) res
--                     cvalarray <- mapM (mapM newCString >=> newArray) resvals >>= newArray
--                     poke cout cvalarray)
--         Right p -> error ("unsupported commands: " ++ show p)
