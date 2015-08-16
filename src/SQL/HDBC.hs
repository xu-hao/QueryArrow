{-# LANGUAGE TypeFamilies, TypeSynonymInstances, MultiParamTypeClasses, FlexibleInstances #-}
module SQL.HDBC where

import DBQuery
import SQL.SQL
import FO
import ResultStream
import FO.Data

import Database.HDBC
import Control.Monad.IO.Class (liftIO)
import Control.Applicative ((<$>))
import Data.Map.Strict (empty, insert)

data HDBCQueryStatement = HDBCQueryStatement [Var] Statement
data HDBCInsertStatement = HDBCInsertStatement [Statement]

convertExprToSQL :: Expr -> SqlValue
convertExprToSQL (IntExpr i) = toSql i
convertExprToSQL (StringExpr s) = toSql s
convertExprToSQL e = error ("unsupported sql expr type: " ++ show e)

convertSQLToResult :: [Var] -> [SqlValue] -> MapResultRow
convertSQLToResult vars sqlvalues = foldl (\row (var, sqlvalue) ->
                insert var (case sqlvalue of
                        SqlInt32 _ -> IntValue (fromSql sqlvalue)
                        SqlInt64 _ -> IntValue (fromSql sqlvalue)
                        SqlInteger _ -> IntValue (fromSql sqlvalue)
                        SqlString _ -> StringValue (fromSql sqlvalue)
                        SqlByteString _ -> StringValue (fromSql sqlvalue)
                        _ -> error ("unsupported sql value: " ++ show sqlvalue)) row) empty (zip vars sqlvalues)

class HDBCConnection conn where
        showSQLQuery :: conn -> SQLQuery -> String
        showSQLInsert :: conn -> SQLInsert -> String

instance PreparedStatement_ HDBCQueryStatement where
        execWithParams (HDBCQueryStatement vars stmt) args = ResultStream (\iteratee seed -> do
                liftIO $ execute stmt (map convertExprToSQL args)
                rows <- liftIO $ fetchAllRows stmt
                foldlM2 iteratee (liftIO $ finish stmt) seed (map (convertSQLToResult vars) rows)
                )
                    -- keep this type signature, otherwise it won't compile
        closePreparedStatement _ = return ()

instance PreparedStatement_ HDBCInsertStatement where
        execWithParams (HDBCInsertStatement stmts) args = do
            let exprs = map convertExprToSQL args
            totals <- liftIO $ mapM (\stmt -> do
                res <- execute stmt exprs
                return (fromIntegral res)) stmts
            intResultStream (sum totals)
        closePreparedStatement _ = return ()

prepareHDBCQueryStatement :: (HDBCConnection conn, IConnection conn) => conn -> SQLQuery -> IO HDBCQueryStatement
prepareHDBCQueryStatement conn sqlquery@(vars, query) = HDBCQueryStatement vars <$> prepare conn (showSQLQuery conn sqlquery)

prepareHDBCInsertStatement :: (HDBCConnection conn, IConnection conn) => conn -> SQLInserts -> IO HDBCInsertStatement
prepareHDBCInsertStatement conn query = HDBCInsertStatement <$> mapM (prepare conn) (map (showSQLInsert conn) query)

instance (HDBCConnection conn, IConnection conn) => DBConnection conn SQLQuery SQLInserts where
        execQueryStatement conn query = do
                stmt <- liftIO $ prepareHDBCQueryStatement conn query
                execWithParams stmt []
        execInsertStatement conn query = do
                stmt <- liftIO $ prepareHDBCInsertStatement conn query
                rs <- execWithParams stmt []
                return rs

        prepareQueryStatement conn query = PreparedStatement <$> prepareHDBCQueryStatement conn query
        prepareInsertStatement conn query = PreparedStatement <$> prepareHDBCInsertStatement conn query
        connBegin _ = return ()
        connCommit = commit
        connRollback = rollback
        connClose = disconnect

-- the QueryDB instance is provided for each DB type
