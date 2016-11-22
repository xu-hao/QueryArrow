{-# LANGUAGE FlexibleContexts, MultiParamTypeClasses, FlexibleInstances, OverloadedStrings, GADTs, ExistentialQuantification, TemplateHaskell , ForeignFunctionInterface #-}

module QueryArrow.C.Plugin where

import FO.Data (Pred, Formula(..), Var(..), Expr(..), Atom(..), Aggregator(..), Summary(..), Lit(..), Sign(..))
import DB.DB
import QueryPlan
import DB.ResultStream
import QueryArrow.C.Template
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
import Control.Monad.Trans.Either (EitherT)
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
import QueryArrow.C.PluginGen

foreign export ccall hs_setup :: IO ()
hs_setup :: IO ()
hs_setup = setup

foreign export ccall hs_connect :: CString -> Ptr (StablePtr (Session Predicates)) -> IO Int
hs_connect :: CString -> Ptr (StablePtr (Session Predicates)) -> IO Int
hs_connect cpath ptr = do
    path <- peekCString cpath
    infoM "Plugin" ("loading configuration from " ++ path)
    ps <- getConfig path
    infoM "Plugin" ("configuration: " ++ show ps)
    db <- transDB "plugin" ps
    case db of
        AbstractDatabase db ->
            catch (do
                conn <- dbOpen db
                let pm = constructDBPredMap db
                sessionptr <- newStablePtr (Session db conn (predicates pm))
                poke ptr sessionptr
                return 0) (\e -> do
                    errorM "C Api" (show (e :: SomeException))
                    return (0-1))

foreign export ccall hs_disconnect :: StablePtr (Session Predicates) -> IO Int
hs_disconnect :: StablePtr (Session Predicates) -> IO Int
hs_disconnect stptr = do
    Session _ conn _ <- deRefStablePtr stptr
    catch (do
        dbClose conn
        return 0) (\e -> do
            errorM "C Api" (show (e :: SomeException))
            return (0-1))

foreign export ccall hs_commit :: StablePtr (Session Predicates) -> IO Int
hs_commit :: StablePtr (Session Predicates) -> IO Int
hs_commit sessionptr = do
    (Session _ conn _) <- deRefStablePtr sessionptr
    b <- dbCommit conn
    return (if b then 0 else -1)

foreign export ccall hs_rollback :: StablePtr (Session Predicates) -> IO Int
hs_rollback :: StablePtr (Session Predicates) -> IO Int
hs_rollback sessionptr = do
    (Session _ conn _) <- deRefStablePtr sessionptr
    dbRollback conn
    return 0

foreign export ccall hs_modify_data :: StablePtr (Session Predicates) -> Ptr CString -> Ptr CString -> Ptr CString -> Ptr CString -> Int -> Int -> IO Int
hs_modify_data :: StablePtr (Session Predicates) -> Ptr CString -> Ptr CString -> Ptr CString -> Ptr CString -> Int -> Int -> IO Int
hs_modify_data sessionptr cupdatecols cupdatevals cwherecolsandops cwherevals cupcols cnumwheres = do
    session@(Session _ _ predicates) <- deRefStablePtr sessionptr
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
    let whereatom pre wherecol0 =
          let wherecol = var (pre ++ wherecol0)
              p = case wherecol0 of
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
                      "data_r_comment" -> _data_comment
                      "data_create_ts" -> _data_create_ts
                      "data_modify_ts" -> _data_modify_ts
                      "data_mode" -> _data_mode
                      "data_resc_hier" -> _data_resc_hier
                      _ -> error ("unsupported " ++ wherecol0) in
               Atom (p predicates) [var "w_data_id", var "w_resc_id", wherecol]
    let whereform wherecol | wherecol /= "data_id" && wherecol /= "resc_id" = FAtomic (whereatom "w_" wherecol)
                           | otherwise = FOne
    let wherelit :: String -> String -> Formula
        wherelit wherecol "!=" =
            Aggregate Not (whereform wherecol)
        wherelit wherecol "=" =
            whereform wherecol
    let cond = foldl FSequencing FOne (zipWith wherelit wherecols whereops )
    let updateatom wherecol | wherecol /= "data_id" && wherecol /= "resc_id" = whereatom "u_" wherecol
                            | otherwise = error ("cannot update " ++ wherecol)
    let updateform updatecol =
            FInsert (Lit Pos (updateatom updatecol))
    let update = foldl FSequencing cond (map updateform updatecols)
    let whereparam col val = (Var ("w_" ++ col), StringValue (pack val))
    let updateparam col val = (Var ("u_" ++ col), StringValue (pack val))
    let params = Map.fromList (zipWith whereparam wherecols wherevals) <> Map.fromList (zipWith updateparam updatecols updatevals)
    processRes (execQuery session update params) (const (return ()))

foreign export ccall hs_modify_coll :: StablePtr (Session Predicates) -> Ptr CString -> Ptr CString -> Int -> CString -> IO Int
hs_modify_coll :: StablePtr (Session Predicates) -> Ptr CString -> Ptr CString -> Int -> CString -> IO Int
hs_modify_coll sessionptr cupdatecols cupdatevals cupcols ccn = do
    session@(Session _ _ predicates) <- deRefStablePtr sessionptr
    let upcols = fromIntegral cupcols
    cupdatecols2 <- peekArray upcols cupdatecols
    updatecols <- mapM peekCString cupdatecols2
    cupdatevals2 <- peekArray upcols cupdatevals
    updatevals <- mapM peekCString cupdatevals2
    cn <- peekCString ccn
    let cond = formula predicates (_coll_name @@ [var "cid", var "w_coll_name"])
    let updateatom wherecol0 =
          let wherecol = var ("u_" ++ wherecol0)
              p = case wherecol0 of
                      "coll_type" -> _coll_type
                      "coll_info1" -> _coll_info1
                      "coll_info2" -> _coll_info2
                      "modify_ts" -> _coll_modify_ts in
              Atom (p predicates) [var "cid", wherecol]
    let updateform updatecol =
            FInsert (Lit Pos (updateatom updatecol))
    let update = foldl FSequencing cond (map updateform updatecols)
    let updateparam col val = (Var ("u_" ++ col), StringValue (pack val))
    let params = Map.singleton (Var "w_coll_name") (StringValue (pack cn)) <> Map.fromList (zipWith updateparam updatecols updatevals)
    processRes (execQuery session update params) (const (return ()))
