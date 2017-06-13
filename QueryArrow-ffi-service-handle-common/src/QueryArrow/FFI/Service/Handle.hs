{-# LANGUAGE FlexibleContexts, MultiParamTypeClasses, FlexibleInstances, OverloadedStrings, GADTs, ExistentialQuantification, TypeApplications, ScopedTypeVariables, DeriveGeneric #-}

module QueryArrow.FFI.Service.Handle where

import QueryArrow.DB.DB

import Prelude hiding (lookup)
import Data.Set (fromList)
import Data.Text (pack)
import Control.Monad.Error.Class (throwError)
import Control.Monad.IO.Class (liftIO)
import QueryArrow.FFI.Service
import QueryArrow.RPC.Message
import QueryArrow.Serialization
import System.IO (Handle)
import Control.Monad.Trans.Either (EitherT)

data HandleSession = HandleSession Handle

handleService :: (String -> EitherT Error IO Handle) -> QueryArrowService HandleSession
handleService connect =
  let   getAllResult0 = \(HandleSession  handle) vars form params -> do
            let name = QuerySet {
                          qsquery = Static [Execute form],
                          qsheaders = fromList vars,
                          qsparams = params
                          }
            liftIO $ sendMsgPack handle name
            rep <- liftIO $ receiveMsgPack handle
            case rep of
                Just (ResultSetNormal results) ->
                          return results
                Just (ResultSetError err) ->
                          throwError err
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
              liftIO $ sendMsgPack handle name
              rep <- liftIO $ receiveMsgPack handle
              case rep of
                  Just (ResultSetNormal  results) ->
                            return ()
                  Just (ResultSetError err) ->
                            throwError err
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
              liftIO $ sendMsgPack handle name2,
            qasCommit = \ (HandleSession handle ) -> do
              let name2 = QuerySet {
                            qsquery = Static [Commit],
                            qsheaders = mempty,
                            qsparams = mempty
                            }
              liftIO $ sendMsgPack handle name2
              rep <- liftIO $ receiveMsgPack handle
              case rep of
                  Just (ResultSetNormal results) ->
                            return ()
                  Just (ResultSetError err) ->
                            throwError err
                  Nothing ->
                      throwError (-1, "cannot parse response"),

            qasRollback = \ (HandleSession handle ) -> do
              let name2 = QuerySet {
                            qsquery = Static [Rollback],
                            qsheaders = mempty,
                            qsparams = mempty
                            }
              liftIO $ sendMsgPack handle name2
              rep <- liftIO $ receiveMsgPack handle
              case rep of
                  Just (ResultSetNormal results) ->
                            return ()
                  Just (ResultSetError err) ->
                            throwError err
                  Nothing ->
                      throwError (-1, "cannot parse response")
          }
