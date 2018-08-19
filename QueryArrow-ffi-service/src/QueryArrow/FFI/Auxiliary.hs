{-# LANGUAGE FlexibleContexts, MultiParamTypeClasses, FlexibleInstances, OverloadedStrings, GADTs, ExistentialQuantification, TypeApplications, ScopedTypeVariables #-}

module QueryArrow.FFI.Auxiliary where

import QueryArrow.FO.Data
import QueryArrow.Syntax.Type
import QueryArrow.FO.TypeChecker
import QueryArrow.Semantics.Value
import QueryArrow.FO.Serialize
import QueryArrow.DB.DB

import Prelude hiding (lookup)
import Data.Text (Text, pack)
import qualified Data.Text as Text
import Data.Map.Strict (lookup)
import Control.Monad.Trans.Except (ExceptT, runExceptT)
import Control.Monad.Error.Class (throwError)
import Control.Monad.IO.Class (liftIO)
import Data.Int
import QueryArrow.FFI.Service
import Data.Text.Encoding
import qualified Data.ByteString.UTF8 as BSUTF8
import Control.Exception
import Database.HDBC
import System.Log.Logger (debugM, errorM)
import Debug.Trace
import QueryArrow.Utils
import QueryArrow.Serialization

eCAT_NO_ROWS_FOUND :: Int
eCAT_NO_ROWS_FOUND = -808000

eNULL :: Int
eNULL = -1095000

eCATALOG_ALREADY_HAS_ITEM_BY_THAT_NAME :: Int
eCATALOG_ALREADY_HAS_ITEM_BY_THAT_NAME = -809000

catchErrors :: IO (Either Error a) -> IO (Either Error a)
catchErrors a = a
        `catch` (\e -> return (Left (convertException (e :: SomeException))))

convertException :: SomeException -> Error
convertException e =
                case fromException e of
                    Just (SqlError state nativeerror errormsg) ->
                        let errstr = show e in
                            (case state of
                                            "23505" -> eCATALOG_ALREADY_HAS_ITEM_BY_THAT_NAME
                                            _ -> - nativeerror, Text.pack("catchErrors: SqlError = " ++ errstr))
                    Nothing ->
                        let errstr = show e in
                            (-1, Text.pack("catchErrors: SomeException = " ++ errstr))

execAbstract :: QueryArrowService b -> b -> Formula -> MapResultRow -> ExceptT Error IO ()
execAbstract svc session form params = do
  liftIO $ debugM "FFI" ("execAbstract: form " ++ serialize form)
  r <- liftIO $ catchErrors (runExceptT (execQuery svc session form params))
  case r of
    Left err -> do
      liftIO $ errorM "FFI" ("execAbstract: ++++++++++++++++++++++++++++ caught error " ++ show err)
      throwError err
    Right a -> return a

getAllResultAbstract :: QueryArrowService b -> b -> [Var] -> Formula -> MapResultRow -> ExceptT Error IO [MapResultRow]
getAllResultAbstract svc session vars form params = do
  liftIO $ debugM "FFI" ("getAllResultValues: form " ++ serialize form)
  r <- liftIO $ catchErrors (runExceptT (getAllResult svc session vars form params))
  case r of
    Left err -> do
      liftIO $ errorM "FFI" ("getAllResultValues: caught error " ++ show err)
      throwError err
    Right a -> return a

getSomeResults :: QueryArrowService b -> b -> [Var] -> Formula -> MapResultRow -> Int -> ExceptT Error IO [MapResultRow]
getSomeResults svc session vars form params n =
  getAllResultAbstract svc session vars (Aggregate (Limit n) form) params

getResultValues :: QueryArrowService b -> b -> [Var] -> Formula -> MapResultRow -> ExceptT Error IO [ResultValue]
getResultValues svc session vars form params = do
  count <- getSomeResults svc session vars form params 1
  case count of
      row : _ -> case mapM (\var -> lookup var row) vars of
                  Just r -> return r
                  Nothing -> do
                    liftIO $ errorM "FFI" ("cannot find var " ++ show vars ++ " in map " ++ show row)
                    throwError (eNULL, pack ("cannot find var " ++ show vars ++ " in map " ++ show row))
      _ -> throwError (eCAT_NO_ROWS_FOUND, "error 2")

getAllResultValues :: QueryArrowService b -> b -> [Var] -> Formula  -> MapResultRow -> ExceptT Error IO [[ResultValue]]
getAllResultValues svc session vars form params = do
  count <- getAllResultAbstract svc session vars form params
  case count of
      [] -> throwError (eCAT_NO_ROWS_FOUND, "error 2")
      rows ->
          case mapM (\row -> mapM (\var -> lookup var row) vars) rows of
              Just r -> return r
              Nothing -> do
                liftIO $ errorM "FFI" ("cannot find var " ++ show vars ++ " in maps " ++ show rows)
                throwError (eNULL, pack ("cannot find var " ++ show vars ++ " in maps " ++ show rows))

getSomeResultValues :: QueryArrowService b -> b -> Int -> [Var] -> Formula  -> MapResultRow -> ExceptT Error IO [[ResultValue]]
getSomeResultValues svc session n vars form params = do
  count <- getSomeResults svc session vars form params (n `div` length vars)
  case count of
      [] -> throwError (eCAT_NO_ROWS_FOUND, "error 2")
      rows ->
          case mapM (\row -> mapM (\var -> lookup var row) vars) rows of
              Just r -> return r
              Nothing -> do
                liftIO $ errorM "FFI" ("cannot find var " ++ show vars ++ " in maps " ++ show rows)
                throwError (eNULL, pack ("cannot find var " ++ show vars ++ " in maps " ++ show rows))

getIntResult :: QueryArrowService b -> b -> [Var] -> Formula  -> MapResultRow -> ExceptT Error IO Int64
getIntResult svc session vars form params = do
    r:_ <- getResultValues svc session vars form params
    return (resultValueToInt r)

getStringResult :: QueryArrowService b -> b -> [Var] -> Formula  -> MapResultRow -> ExceptT Error IO Text
getStringResult svc session vars form params = do
    r:_ <- getResultValues svc session vars form params
    return (resultValueToString r)

getStringArrayResult :: QueryArrowService b -> b -> [Var] -> Formula -> MapResultRow -> ExceptT Error IO [Text]
getStringArrayResult svc session vars form params = do
    r <- getResultValues svc session vars form params
    return (map resultValueToString r)

getSomeStringArrayResult :: QueryArrowService b -> b -> Int -> [Var] -> Formula -> MapResultRow -> ExceptT Error IO [[Text]]
getSomeStringArrayResult svc session n vars form params = do
    r <- getSomeResultValues svc session n vars form params
    return (map (map resultValueToString) r)

getAllStringArrayResult :: QueryArrowService b -> b -> [Var] -> Formula -> MapResultRow -> ExceptT Error IO [[Text]]
getAllStringArrayResult svc session vars form params = do
    r <- getAllResultValues svc session vars form params
    return (map (map resultValueToString) r)

resultValueToInt :: ResultValue -> Int64
resultValueToInt rv = case rv of
    (Int64Value i) -> fromIntegral i
    (StringValue i) -> read (Text.unpack i)
    (ByteStringValue i) -> read (BSUTF8.toString i)
    _ -> error ("resultValueToInt: cannot convert to int " ++ show rv)

resultValueToString :: ResultValue -> Text
resultValueToString rv = case rv of
    (Int64Value i) -> Text.pack (show i)
    (StringValue i) -> i
    (ByteStringValue i) -> decodeUtf8 i
    (AppValue a b) ->  "(" `Text.append` resultValueToString a `Text.append` " " `Text.append` resultValueToString b `Text.append` ")"
    Null -> Text.pack ""


processRes :: ExceptT Error IO a -> (a -> IO ()) -> IO Int
processRes a f = do
    res <- runExceptT a
    case res of
        Left (ec, _) ->
            return (fromIntegral ec)
        Right a -> do
            f a
            return 0

processRes2 :: ExceptT Error IO [a] -> ([a] -> IO ()) -> IO Int
processRes2 a f = do
    res <- runExceptT a
    case res of
        Left (ec, _) ->
            return (fromIntegral ec)
        Right a -> do
            f a
            return (length a)
