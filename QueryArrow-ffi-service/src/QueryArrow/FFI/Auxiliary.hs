{-# LANGUAGE FlexibleContexts, MultiParamTypeClasses, FlexibleInstances, OverloadedStrings, GADTs, ExistentialQuantification, TypeApplications, ScopedTypeVariables #-}

module QueryArrow.FFI.Auxiliary where

import QueryArrow.FO.Data
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
import QueryArrow.Data.Abstract
import QueryArrow.Data.PredicatesGen

eCAT_NO_ROWS_FOUND :: Int
eCAT_NO_ROWS_FOUND = -808000

eNULL :: Int
eNULL = -1095000

execAbstract :: QueryArrowService b -> b -> AbstractFormula Predicates -> MapResultRow -> EitherT Error IO ()
execAbstract svc session form params =
  execQuery svc session  (formula (getPredicates svc session) form) params

getResultValues :: QueryArrowService b -> b -> [Var] -> AbstractFormula Predicates -> MapResultRow -> EitherT Error IO [ResultValue]
getResultValues svc session vars form params = do
  count <- getSomeResults svc session vars (formula (getPredicates svc session) form) params 1
  case count of
      row : _ -> case mapM (\var -> lookup var row) vars of
                  Just r -> return r
                  Nothing -> do
                    liftIO $ putStrLn ("cannot find var " ++ show vars ++ " in map " ++ show row)
                    throwError (eNULL, "error 1")
      _ -> throwError (eCAT_NO_ROWS_FOUND, "error 2")

getAllResultValues :: QueryArrowService b -> b -> [Var] -> AbstractFormula Predicates -> MapResultRow -> EitherT Error IO [[ResultValue]]
getAllResultValues svc session vars form params = do
  count <- getAllResult svc session vars (formula (getPredicates svc session) form) params
  case count of
      [] -> throwError (eCAT_NO_ROWS_FOUND, "error 2")
      rows ->
          case mapM (\row -> mapM (\var -> lookup var row) vars) rows of
              Just r -> return r
              Nothing -> do
                liftIO $ putStrLn ("cannot find var " ++ show vars ++ " in maps " ++ show rows)
                throwError (eNULL, "error 1")

getSomeResultValues :: QueryArrowService b -> b -> Int -> [Var] -> AbstractFormula Predicates -> MapResultRow -> EitherT Error IO [[ResultValue]]
getSomeResultValues svc session n vars form params = do
  count <- getSomeResults svc session vars (formula (getPredicates svc session) form) params (n `div` length vars)
  case count of
      [] -> throwError (eCAT_NO_ROWS_FOUND, "error 2")
      rows ->
          case mapM (\row -> mapM (\var -> lookup var row) vars) rows of
              Just r -> return r
              Nothing -> do
                liftIO $ putStrLn ("cannot find var " ++ show vars ++ " in maps " ++ show rows)
                throwError (eNULL, "error 1")

getIntResult :: QueryArrowService b -> b ->[ Var ]-> AbstractFormula Predicates -> MapResultRow -> EitherT Error IO Int64
getIntResult svc session vars form params = do
    r:_ <- getResultValues svc session vars form params
    return (resultValueToInt r)

getStringResult :: QueryArrowService b -> b -> [Var ]-> AbstractFormula Predicates -> MapResultRow -> EitherT Error IO Text
getStringResult svc session vars form params = do
    r:_ <- getResultValues svc session vars form params
    return (resultValueToString r)

getStringArrayResult :: QueryArrowService b -> b -> [Var] -> AbstractFormula Predicates -> MapResultRow -> EitherT Error IO [Text]
getStringArrayResult svc session vars form params = do
    r <- getResultValues svc session vars form params
    return (map resultValueToString r)

getSomeStringArrayResult :: QueryArrowService b -> b -> Int -> [Var] -> AbstractFormula Predicates -> MapResultRow -> EitherT Error IO [[Text]]
getSomeStringArrayResult svc session n vars form params = do
    r <- getSomeResultValues svc session n vars form params
    return (map (map resultValueToString) r)

getAllStringArrayResult :: QueryArrowService b -> b -> [Var] -> AbstractFormula Predicates -> MapResultRow -> EitherT Error IO [[Text]]
getAllStringArrayResult svc session vars form params = do
    r <- getAllResultValues svc session vars form params
    return (map (map resultValueToString) r)

resultValueToInt :: ResultValue -> Int64
resultValueToInt (IntValue i) = fromIntegral i
resultValueToInt (StringValue i) = read (Text.unpack i)

resultValueToString :: ResultValue -> Text
resultValueToString (IntValue i) = Text.pack (show i)
resultValueToString (StringValue i) = i
resultValueToString Null = Text.pack ""


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
