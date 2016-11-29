{-# LANGUAGE OverloadedStrings, TypeFamilies #-}

module QueryArrow.RPC.DB where

import DB.ResultStream
import DB.DB hiding (Null)
import FO.Data
import Utils
import Parser

import Prelude hiding (lookup, length)
import Data.Map.Strict (fromList, keysSet)
import Control.Monad.IO.Class (liftIO)
import Control.Monad.Trans.Resource
import Control.Monad.Except
import System.Log.Logger
import Control.Monad.Logger.HSLogger ()
import Control.Monad.Trans.Either
import Data.Set (Set)
import Text.Parsec

run3 :: (IDatabase db, DBFormulaType db ~ Formula, RowType (StatementType (ConnectionType db)) ~ MapResultRow) => Set Var -> [Command] -> MapResultRow -> db -> ConnectionType db -> EitherT String IO [MapResultRow]
run3 hdr commands params tdb conn = do
    r <- liftIO $ runResourceT $ dbCatch $ do
                                concat <$> mapM (\command -> case command of
                                    Begin -> do
                                        liftIO $ dbBegin conn
                                        return []
                                    Prepare -> do
                                        b <- liftIO $ dbPrepare conn
                                        if b
                                            then return []
                                            else do
                                                liftIO $ errorM "QA" "prepare failed, cannot commit"
                                                error "prepare failed, cannot commit"
                                    Commit -> do
                                        b <- liftIO $ dbCommit conn
                                        if b
                                            then return []
                                            else do
                                                liftIO $ errorM "QA" "commit failed"
                                                error "commit failed"
                                    Rollback -> do
                                        liftIO $ dbRollback conn
                                        return []
                                    Execute qu ->
                                        case runExcept (checkQuery qu) of
                                              Right _ -> getAllResultsInStream ( doQueryWithConn tdb conn hdr qu (keysSet params) (pure params))
                                              Left e -> error e) commands
                            -- Right (Right Commit) -> do
                            --     b <- liftIO $ dbPrepare tdb
                            --     if b
                            --         then do
                            --             b <- liftIO $ dbCommit tdb
                            --             if b
                            --                 then return [mempty]
                            --                 else do
                            --                     liftIO $ dbRollback tdb
                            --                     liftIO $ errorM "QA" "prepare succeeded but cannot commit"
                            --         else do
                            --             liftIO $ dbRollback tdb
                            --             )
    case r of
        Right rows ->  right rows
        Left e ->  left (show e)

run :: (IDatabase db, DBFormulaType db ~ Formula, RowType (StatementType (ConnectionType db)) ~ MapResultRow) => Set Var -> String -> MapResultRow -> db -> ConnectionType db -> EitherT String IO [MapResultRow]
run hdr query par tdb conn  = do
    let predmap = constructDBPredMap tdb
    case runParser progp (mempty, predmap, mempty) "" query of
                            Left err -> throwError (show err)
                            Right (commands, _) -> run3 hdr commands par tdb conn
