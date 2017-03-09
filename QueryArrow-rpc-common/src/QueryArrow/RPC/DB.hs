{-# LANGUAGE OverloadedStrings, TypeFamilies #-}

module QueryArrow.RPC.DB where

import QueryArrow.DB.ResultStream
import QueryArrow.DB.DB hiding (Null)
import QueryArrow.FO.Data
import QueryArrow.FO.Types
import QueryArrow.Utils
import QueryArrow.Parser

import Prelude hiding (lookup, length, map)
import Data.Map.Strict (keysSet, map)
import Control.Monad.IO.Class (liftIO)
import Control.Monad.Trans.Resource
import Control.Monad.Except
import QueryArrow.Control.Monad.Logger.HSLogger ()
import Control.Monad.Trans.Either
import Data.Set (Set)
import Text.Parsec

run3 :: (IDatabase db, DBFormulaType db ~ FormulaT, RowType (StatementType (ConnectionType db)) ~ MapResultRow) => Set Var -> [Command] -> MapResultRow -> db -> ConnectionType db -> EitherT String IO [MapResultRow]
run3 hdr commands params tdb conn = do
    r <- liftIO $ runResourceT $ dbCatch $ do
                                concat <$> mapM (\command -> case command of
                                    Begin -> do
                                        liftIO $ dbBegin conn
                                        return []
                                    Prepare -> do
                                        liftIO $ dbPrepare conn
                                        return []
                                    Commit -> do
                                        liftIO $ dbCommit conn
                                        return []
                                    Rollback -> do
                                        liftIO $ dbRollback conn
                                        return []
                                    Execute qu -> do
                                        let (varstinp, varstout) = setToMap2 (map (\(AbstractResultValue arv) -> castTypeOf arv) params) hdr
                                        getAllResultsInStream ( doQueryWithConn tdb conn varstout qu varstinp (pure params))) commands
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

run :: (IDatabase db, DBFormulaType db ~ FormulaT, RowType (StatementType (ConnectionType db)) ~ MapResultRow) => Set Var -> String -> MapResultRow -> db -> ConnectionType db -> EitherT String IO [MapResultRow]
run hdr query par tdb conn =
    case runParser progp () "" query of
                            Left err -> throwError (show err)
                            Right commands -> run3 hdr commands par tdb conn
