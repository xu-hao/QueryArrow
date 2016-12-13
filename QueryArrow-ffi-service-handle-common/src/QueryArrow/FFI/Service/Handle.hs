{-# LANGUAGE FlexibleContexts, MultiParamTypeClasses, FlexibleInstances, OverloadedStrings, GADTs, ExistentialQuantification, TypeApplications, ScopedTypeVariables, DeriveGeneric #-}

module QueryArrow.FFI.Service.Handle where

import QueryArrow.FO.Data
import QueryArrow.DB.DB

import Prelude hiding (lookup)
import Data.Set (fromList)
import Data.Text (pack)
import Control.Exception (SomeException, try)
import Control.Monad.Error.Class (throwError)
import Control.Monad.IO.Class (liftIO)
import System.Log.Logger (infoM, errorM)
import Data.Aeson
import QueryArrow.FFI.Service
import QueryArrow.RPC.Message
import QueryArrow.Config
import QueryArrow.DBMap
import QueryArrow.Serialization
import GHC.Generics
import System.IO (Handle)
import qualified Data.ByteString.Lazy.Char8 as BS
import Control.Monad.Trans.Either (EitherT)
import Network

data HandleSession = HandleSession Handle

handleService :: (String -> EitherT Error IO Handle) -> QueryArrowService HandleSession
handleService connect =
  let   getAllResult0 = \(HandleSession  handle) vars form params -> do
            let name = QuerySet {
                          qsquery = Static [Execute form],
                          qsheaders = fromList vars,
                          qsparams = params
                          }
            liftIO $ sendMsg handle name
            rep <- liftIO $ receiveMsg handle
            case rep of
                Just (ResultSet err results) ->
                    if null err
                      then
                          return results
                      else
                          throwError (-1, pack err)
                Nothing ->
                    throwError (-1, "cannot parse response")
  in
          QueryArrowService {
            execQuery =  \(HandleSession  handle) form params -> do
              let name = QuerySet {
                            qsquery = Static [Execute (form )],
                            qsheaders = mempty,
                            qsparams = params
                            }
              liftIO $ sendMsg handle name
              rep <- liftIO $ receiveMsg handle
              case rep of
                  Just (ResultSet err results) ->
                      if null err
                        then
                            return ()
                        else
                            throwError (-1, pack err)
                  Nothing ->
                      throwError (-1, "cannot parse response"),
            getAllResult = getAllResult0,
            qasConnect = \path -> do
                          handle <- connect path
                          return (HandleSession  handle ),
            qasDisconnect = \ session@(HandleSession handle) -> do
              let name2 = QuerySet {
                            qsquery = Quit,
                            qsheaders = mempty,
                            qsparams = mempty
                            }
              liftIO $ sendMsg handle name2,
            qasCommit = \ (HandleSession handle ) -> do
              let name2 = QuerySet {
                            qsquery = Static [Commit],
                            qsheaders = mempty,
                            qsparams = mempty
                            }
              liftIO $ sendMsg handle name2
              rep <- liftIO $ receiveMsg handle
              case rep of
                  Just (ResultSet err results) ->
                      if null err
                        then
                            return ()
                        else
                            throwError (-1, pack err)
                  Nothing ->
                      throwError (-1, "cannot parse response"),

            qasRollback = \ (HandleSession handle ) -> do
              let name2 = QuerySet {
                            qsquery = Static [Rollback],
                            qsheaders = mempty,
                            qsparams = mempty
                            }
              liftIO $ sendMsg handle name2
              rep <- liftIO $ receiveMsg handle
              case rep of
                  Just (ResultSet err results) ->
                      if null err
                        then
                            return ()
                        else
                            throwError (-1, pack err)
                  Nothing ->
                      throwError (-1, "cannot parse response")
          }