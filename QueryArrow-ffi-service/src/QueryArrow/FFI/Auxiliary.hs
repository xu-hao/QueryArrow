{-# LANGUAGE FlexibleContexts, MultiParamTypeClasses, FlexibleInstances, OverloadedStrings, GADTs, ExistentialQuantification, TypeApplications, ScopedTypeVariables #-}

module QueryArrow.FFI.Auxiliary where

import QueryArrow.Syntax.Data
import QueryArrow.Syntax.Types
import QueryArrow.Semantics.ResultRow.VectorResultRow
import QueryArrow.Semantics.ResultValue.AbstractResultValue
import QueryArrow.Semantics.ResultValue
import QueryArrow.Semantics.ResultHeader.VectorResultHeader
import QueryArrow.Data.Some
import QueryArrow.DB.DB

import Prelude hiding (lookup)
import Data.Text (Text)
import qualified Data.Text as Text
import Data.Map.Strict (lookup)
import Control.Monad.Trans.Either (EitherT, runEitherT)
import Control.Monad.Error.Class (throwError)
import Control.Monad.IO.Class (liftIO)
import Data.Int
import QueryArrow.FFI.Service
import Data.Text.Encoding (encodeUtf8, decodeUtf8)
import qualified Data.ByteString.UTF8 as BSUTF8
import qualified Data.Vector as V

eCAT_NO_ROWS_FOUND :: Int
eCAT_NO_ROWS_FOUND = -808000

eNULL :: Int
eNULL = -1095000

getSomeResults :: QueryArrowService b -> b -> [Var] -> Formula -> ResultHeader -> VectorResultRow AbstractResultValue -> Int -> EitherT Error IO [VectorResultRow AbstractResultValue]
getSomeResults svc session vars form hdr params n =
  getAllResult svc session vars (Aggregate (Limit n) form) hdr params

execAbstract :: QueryArrowService b -> b -> Formula -> ResultHeader -> VectorResultRow AbstractResultValue -> EitherT Error IO ()
execAbstract svc session form hdr params =
  execQuery svc session form hdr params

getResultValues :: QueryArrowService b -> b -> [Var] -> Formula -> ResultHeader -> VectorResultRow AbstractResultValue -> EitherT Error IO (VectorResultRow AbstractResultValue)
getResultValues svc session vars form hdr params = do
  count <- getSomeResults svc session vars form hdr params 1
  case count of
      row : _ -> return row
      _ -> throwError (eCAT_NO_ROWS_FOUND, "error 2")

getAllResultValues :: QueryArrowService b -> b -> [Var] -> Formula  -> ResultHeader -> VectorResultRow AbstractResultValue -> EitherT Error IO [VectorResultRow AbstractResultValue]
getAllResultValues svc session vars form hdr params = do
  count <- getAllResult svc session vars form hdr params
  case count of
      [] -> throwError (eCAT_NO_ROWS_FOUND, "error 2")
      rows -> return rows

getSomeResultValues :: QueryArrowService b -> b -> Int -> [Var] -> Formula  -> ResultHeader -> VectorResultRow AbstractResultValue -> EitherT Error IO [VectorResultRow AbstractResultValue]
getSomeResultValues svc session n vars form hdr params = do
  count <- getSomeResults svc session vars form hdr params (n `div` length vars)
  case count of
      [] -> throwError (eCAT_NO_ROWS_FOUND, "error 2")
      rows -> return rows

getIntResult :: QueryArrowService b -> b ->[ Var ]-> Formula  -> ResultHeader -> VectorResultRow AbstractResultValue -> EitherT Error IO Int64
getIntResult svc session vars form hdr params = do
    row <- getResultValues svc session vars form hdr params
    let r = V.head row
    return (resultValueToInt r)

getStringResult :: QueryArrowService b -> b -> [Var ]-> Formula  -> ResultHeader -> VectorResultRow AbstractResultValue -> EitherT Error IO Text
getStringResult svc session vars form hdr params = do
    row <- getResultValues svc session vars form hdr params
    let r = V.head row
    return (resultValueToString r)

getStringArrayResult :: QueryArrowService b -> b -> [Var] -> Formula  -> ResultHeader -> VectorResultRow AbstractResultValue -> EitherT Error IO (V.Vector Text)
getStringArrayResult svc session vars form hdr params = do
    row <- getResultValues svc session vars form hdr params
    return (V.map resultValueToString row)

getSomeStringArrayResult :: QueryArrowService b -> b -> Int -> [Var] -> Formula -> ResultHeader -> VectorResultRow AbstractResultValue -> EitherT Error IO [V.Vector Text]
getSomeStringArrayResult svc session n vars form hdr params = do
    rows <- getSomeResultValues svc session n vars form hdr params
    return (map (V.map resultValueToString) rows)

getAllStringArrayResult :: QueryArrowService b -> b -> [Var] -> Formula -> ResultHeader -> VectorResultRow AbstractResultValue -> EitherT Error IO [V.Vector Text]
getAllStringArrayResult svc session vars form hdr params = do
    rows <- getAllResultValues svc session vars form hdr params
    return (map (V.map resultValueToString) rows)

resultValueToInt :: AbstractResultValue -> Int64
resultValueToInt rv = case rv of
  Some arv -> case toConcreteResultValue arv of
    (Int64Value i) -> fromIntegral i
    (StringValue i) -> read (Text.unpack i)
    (ByteStringValue i) -> read (BSUTF8.toString i)

resultValueToString :: AbstractResultValue -> Text
resultValueToString rv = case rv of
  Some arv -> case toConcreteResultValue arv of
    (Int64Value i) -> Text.pack (show i)
    (StringValue i) -> i
    (ByteStringValue i) -> decodeUtf8 i
    (RefValue ty loc path) -> Text.pack (show loc ++ "/" ++ show path)
    Null -> Text.pack ""


processRes :: EitherT Error IO a -> (a -> IO ()) -> IO Int
processRes a f = do
    res <- runEitherT a
    case res of
        Left (ec, _) ->
            return (fromIntegral ec)
        Right a -> do
            f a
            return 0

processRes2 :: EitherT Error IO [a] -> ([a] -> IO ()) -> IO Int
processRes2 a f = do
    res <- runEitherT a
    case res of
        Left (ec, _) ->
            return (fromIntegral ec)
        Right a -> do
            f a
            return (length a)
