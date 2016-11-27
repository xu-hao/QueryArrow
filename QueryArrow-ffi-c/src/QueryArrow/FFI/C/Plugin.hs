{-# LANGUAGE FlexibleContexts, MultiParamTypeClasses, FlexibleInstances, OverloadedStrings, GADTs, ExistentialQuantification, TemplateHaskell , ForeignFunctionInterface #-}

module QueryArrow.FFI.C.Plugin where

import FO.Data (Pred, Formula(..), Var(..), Expr(..), Atom(..), Aggregator(..), Summary(..), Lit(..), Sign(..))
import DB.DB
import QueryPlan
import DB.ResultStream
import QueryArrow.FFI.C.Template
import Config
import Utils(constructDBPredMap)

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
import System.Log.Logger (errorM, infoM)
import Data.Maybe (fromMaybe)
import Control.Monad (foldM)
import Logging
import QueryArrow.FFI.Service
import QueryArrow.Data.PredicatesGen
import QueryArrow.Data.Abstract
import QueryArrow.FFI.Auxiliary

foreign export ccall hs_setup :: IO ()
hs_setup :: IO ()
hs_setup = setup

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
                    "data_repl_num" -> _data_repl_num
                    "data_type_name" -> _data_type_name
                    "data_size" -> _data_size
                    -- "resc_name" -> _data_resc_name
                    "data_path" -> _data_path
                    "data_owner_name" -> _data_owner_name
                    "data_owner_zone" -> _data_owner_zone
                    "data_is_dirty" -> _data_is_dirty
                    "data_checksum" -> _data_checksum
                    "data_expiry_ts" -> _data_expiry_ts
                    "r_comment" -> _data_comment
                    "create_ts" -> _data_create_ts
                    "modify_ts" -> _data_modify_ts
                    "data_mode" -> _data_mode
                    "resc_hier" -> _data_resc_hier
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
                           | otherwise = return FOne
    let wherelit wherecol "!=" =
            aggregate Not (whereform wherecol)
        wherelit wherecol "=" =
            whereform wherecol
    let cond = foldl (.*.) (return FOne) (zipWith wherelit wherecols whereops )
    let updateform wherecol | wherecol /= "data_id" && wherecol /= "resc_id" = updateatom "u_" wherecol
                            | otherwise = error ("cannot update " ++ wherecol)
    let update = foldl (.*.) cond (map updateform updatecols)
    let whereparam col val = (Var ("w_" ++ col), StringValue (pack val))
    let updateparam col val = (Var ("u_" ++ col), StringValue (pack val))
    let params = Map.fromList (zipWith whereparam wherecols wherevals) <> Map.fromList (zipWith updateparam updatecols updatevals)
    processRes (execQuery svc session update params) (const (return ()))

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
    let cond = _coll_name @@ [var "cid", var "w_coll_name"]
    let wherepredicate wherecol0 =
            case wherecol0 of
                    "coll_type" -> _coll_type
                    "coll_info1" -> _coll_info1
                    "coll_info2" -> _coll_info2
                    "modify_ts" -> _coll_modify_ts
    let updateatom wherecol0 =
          let wherecol = var ("u_" ++ wherecol0)
              p = wherepredicate wherecol0 in
              p @@+ [var "cid", wherecol]
    let updateform updatecol = updateatom updatecol
    let update = foldl (.*.) cond (map updateform updatecols)
    let updateparam col val = (Var ("u_" ++ col), StringValue (pack val))
    let params = Map.singleton (Var "w_coll_name") (StringValue (pack cn)) <> Map.fromList (zipWith updateparam updatecols updatevals)
    processRes (execQuery svc session update params) (const (return ()))

mapResultRowToList :: [Var] -> MapResultRow -> [ResultValue]
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
