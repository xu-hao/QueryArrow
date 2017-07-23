{-# LANGUAGE OverloadedStrings, TypeFamilies #-}

module QueryArrow.RPC.DB where

import QueryArrow.Semantics.ResultStream
import QueryArrow.Semantics.ResultRow.VectorResultRow
import QueryArrow.Semantics.ResultValue.AbstractResultValue
import QueryArrow.Semantics.ResultValue
import QueryArrow.Semantics.ResultHeader.VectorResultHeader
import QueryArrow.Semantics.ResultSet.ResultStreamResultSet
import QueryArrow.Semantics.ResultSet
import QueryArrow.Semantics.ResultSet.VectorResultSetTransformer
import QueryArrow.DB.DB hiding (Null)
import QueryArrow.Syntax.Data
import QueryArrow.Syntax.Types
import QueryArrow.Utils
import QueryArrow.Parser
import QueryArrow.Data.Some

import Prelude hiding (lookup, length, map)
import Data.Map.Strict (keysSet, map, fromList)
import Control.Monad.IO.Class (liftIO)
import Control.Monad.Trans.Resource
import Control.Monad.Except
import QueryArrow.Control.Monad.Logger.HSLogger ()
import Control.Monad.Trans.Either
import Control.Exception (SomeException(..))
import System.IO.Error(userError)
import Data.Set (Set)
import Text.Parsec
import qualified Data.Vector as V

run3 :: (IDatabase db, DBFormulaType db ~ FormulaT,
        ResultSetRowType (ResultSetType (StatementType (ConnectionType db))) ~ VectorResultRow AbstractResultValue,
        ResultSetTransType (ResultSetType (StatementType (ConnectionType db))) ~ ResultSetTransformer AbstractResultValue,
        InputRowType (StatementType (ConnectionType db)) ~ VectorResultRow AbstractResultValue) => Set Var -> [Command] -> ResultHeader -> VectorResultRow AbstractResultValue -> db -> ConnectionType db -> EitherT String IO [VectorResultRow AbstractResultValue]
run3 ret commands hdr params tdb conn = do
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
                                        let (varstinp, varstout) = setToMap2 (fromList (zipWith (\var (Some arv) -> (var, castTypeOf arv)) (V.toList hdr) (V.toList params))) ret
                                        (_, rs) <- liftIO $ doQueryWithConn tdb conn varstout qu varstinp (ResultStreamResultSet RSId hdr (listResultStream [params]))
                                        getAllResultsInStream (rs :: DBResultStream (VectorResultRow AbstractResultValue))) commands
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
        Left e ->  left e

run :: (IDatabase db, DBFormulaType db ~ FormulaT, ResultSetRowType (ResultSetType (StatementType (ConnectionType db))) ~ VectorResultRow AbstractResultValue,
        ResultSetTransType (ResultSetType (StatementType (ConnectionType db))) ~ ResultSetTransformer AbstractResultValue,
        InputRowType (StatementType (ConnectionType db)) ~ VectorResultRow AbstractResultValue) => Set Var -> String -> ResultHeader -> VectorResultRow AbstractResultValue -> db -> ConnectionType db -> EitherT String IO [VectorResultRow AbstractResultValue]
run ret query hdr par tdb conn =
    case runParser progp () "" query of
                            Left err -> throwError (SomeException (userError ("run: " ++ show err)))
                            Right commands -> run3 ret commands hdr par tdb conn
