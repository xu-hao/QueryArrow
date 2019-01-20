{-# LANGUAGE DeriveGeneric, OverloadedStrings, TypeFamilies #-}

module QueryArrow.RPC.DB where

import QueryArrow.DB.ResultStream
import QueryArrow.DB.DB
import QueryArrow.Syntax.Term
import QueryArrow.Semantics.Value
import QueryArrow.Semantics.TypeChecker
import QueryArrow.Utils
import QueryArrow.Parser
import QueryArrow.RPC.Parser
import QueryArrow.RPC.Data

import Prelude hiding (lookup, length, map)
import Data.Map.Strict (map)
import Control.Monad.IO.Class (liftIO)
import Control.Monad.Trans.Resource
import Control.Monad.Except
import QueryArrow.Control.Monad.Logger.HSLogger ()
import Control.Monad.Trans.Except
import Control.Exception (SomeException(..))
import System.IO.Error(userError)
import Data.Set (Set)
import Data.Text (Text)
import Data.Conduit
import Text.Parsec
import GHC.Generics
import Debug.Trace

run3 :: (IDatabase db, DBFormulaType db ~ FormulaT, RowType (StatementType (ConnectionType db)) ~ MapResultRow) => Set Var -> [Command] -> MapResultRow -> db -> ConnectionType db -> ExceptT SomeException IO [MapResultRow]
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
                                        let (varstinp, varstout) = setToMap2 (map castTypeOf params) hdr
                                        -- trace ("varstinp = " ++ show varstinp ++ ", varstout" ++ show varstout) $
                                        getAllResultsInStream ( doQueryWithConn tdb conn varstout qu varstinp (yield params))) commands
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
        Right rows ->  return rows
        Left e ->  throwE e

run :: (IDatabase db, DBFormulaType db ~ FormulaT, RowType (StatementType (ConnectionType db)) ~ MapResultRow) => Set Var -> String -> MapResultRow -> db -> ConnectionType db -> ExceptT SomeException IO [MapResultRow]
run hdr query par tdb conn =
    case runParser progp () "" query of
                            Left err -> throwError (SomeException (userError ("run: " ++ show err)))
                            Right commands -> run3 hdr commands par tdb conn
