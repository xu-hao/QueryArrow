{-# LANGUAGE OverloadedStrings, TypeFamilies #-}
module QueryArrow.RPC.Service.Service.Common where

import QueryArrow.DB.DB

import Data.Time
import QueryArrow.Serialization
import System.Log.Logger
import QueryArrow.Control.Monad.Logger.HSLogger ()
import QueryArrow.RPC.Message
import Control.Monad.Trans.Except
import QueryArrow.RPC.DB
import System.IO (Handle)
import QueryArrow.Syntax.Type
import QueryArrow.Semantics.TypeChecker
import QueryArrow.FFI.Auxiliary

worker :: (IDatabase db, DBFormulaType db ~ FormulaT, RowType (StatementType (ConnectionType db)) ~ MapResultRow) => Handle -> db -> ConnectionType db -> IO ()
worker handle tdb conn = do
                t0 <- getCurrentTime
                req <- receiveMsgPack handle
                infoM "RPC_TCP_SERVER" ("received message " ++ show req)
                (t2, t3, b) <- case req of
                    Nothing -> do
                        sendMsgPack handle (errorSet (-1, "cannot parser request"))
                        t2 <- getCurrentTime
                        t3 <- getCurrentTime
                        return (t2, t3, False)
                    Just qs -> do
                        let qu = qsquery qs
                            hdr = qsheaders qs
                            par = qsparams qs
                        case qu of
                            Quit -> do -- disconnect when qu field is null
                                t2 <- getCurrentTime
                                t3 <- getCurrentTime
                                return (t2, t3, True)
                            Static qu -> do
                                t2 <- getCurrentTime
                                ret <- runExceptT (run3 hdr qu par tdb conn)
                                t3 <- getCurrentTime
                                case ret of
                                    Left e ->
                                        sendMsgPack handle (errorSet (convertException e))
                                    Right rep ->
                                        sendMsgPack handle (resultSet rep)
                                return (t2, t3, False)
                            Dynamic qu -> do
                                t2 <- getCurrentTime
                                infoM "RPC_TCP_SERVER" "**************"
                                ret <- runExceptT (run hdr qu par tdb conn)
                                infoM "RPC_TCP_SERVER" "**************"
                                t3 <- getCurrentTime
                                case ret of
                                    Left e -> do
                                        infoM "RPC_TCP_SERVER" "**************1"
                                        sendMsgPack handle (errorSet (convertException e))
                                        infoM "RPC_TCP_SERVER" "**************2"
                                    Right rep -> do
                                        infoM "RPC_TCP_SERVER" "**************3"
                                        sendMsgPack handle (resultSet rep)
                                        infoM "RPC_TCP_SERVER" "**************4"
                                return (t2, t3, False)
                t1 <- getCurrentTime
                infoM "RPC_TCP_SERVER" (show (diffUTCTime t1 t0) ++ "\npre: " ++ show (diffUTCTime t2 t0) ++ "\nquery: " ++ show (diffUTCTime t3 t2) ++ "\npost: " ++ show (diffUTCTime t1 t3))
                if b
                    then return ()
                    else worker handle tdb conn
