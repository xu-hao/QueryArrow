{-# LANGUAGE TypeFamilies, TypeSynonymInstances, MultiParamTypeClasses, FlexibleInstances #-}
module SQL.HDBC where

import DBQuery
import SQL.SQL
import QueryPlan
import ResultStream
import FO.Data

import Database.HDBC
import Control.Monad.IO.Class (liftIO)
import Control.Applicative ((<$>))
import Data.Map.Strict (empty, insert, (!))

data HDBCQueryStatement = HDBCQueryStatement [Var] Statement [Var] -- return vars stmt param vars
data HDBCInsertStatement = HDBCInsertStatement Statement [Var] -- stmt param vars

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
        execWithParams (HDBCQueryStatement vars stmt params) args = resultStream2 (do
                execute stmt (map (\v -> convertExprToSQL (args ! v)) params)
                rows <- fetchAllRows stmt
                return (map (convertSQLToResult vars) rows)) (finish stmt)
        closePreparedStatement _ = return ()

instance PreparedStatement_ HDBCInsertStatement where
        execWithParams (HDBCInsertStatement stmt params) args = do
            let exprs = (map (\v -> convertExprToSQL (args ! v)) params)
            total <- liftIO $ do
                res <- execute stmt exprs
                return (fromIntegral res)
            return empty
        closePreparedStatement _ = return ()

prepareHDBCQueryStatement :: (HDBCConnection conn, IConnection conn) => conn -> SQLQuery -> IO HDBCQueryStatement
prepareHDBCQueryStatement conn sqlquery@(vars, query, params) = HDBCQueryStatement vars <$> prepare conn (showSQLQuery conn sqlquery) <*> pure params

prepareHDBCInsertStatement :: (HDBCConnection conn, IConnection conn) => conn -> SQLInsert -> IO HDBCInsertStatement
prepareHDBCInsertStatement conn sqlquery@(query, params) = HDBCInsertStatement <$> prepare conn ( showSQLInsert conn sqlquery) <*> pure params

instance (HDBCConnection conn, IConnection conn) => DBConnection conn SQLQuery SQLInsert where
        prepareQueryStatement conn query = PreparedStatement <$> prepareHDBCQueryStatement conn query
        prepareInsertStatement conn query = PreparedStatement <$> prepareHDBCInsertStatement conn query
        connBegin _ = return ()
        connCommit = commit
        connRollback = rollback
        connClose = disconnect

-- the QueryDB instance is provided for each DB type
