{-# LANGUAGE TypeFamilies, TypeSynonymInstances, MultiParamTypeClasses, FlexibleInstances #-}
module SQL.HDBC where

import Prelude hiding (lookup)
import DBQuery
import SQL.SQL
import QueryPlan
import ResultStream
import FO.Data

import Database.HDBC
import Control.Monad.IO.Class (liftIO)
import Control.Applicative ((<$>))
import Data.Map.Strict (empty, insert, (!), lookup)
import System.Log.Logger

data HDBCQueryStatement = HDBCQueryStatement Bool [Var] Statement [Var] -- return vars stmt param vars

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
        hdbcPrepare :: conn -> DBAdapterMonad Bool
        hdbcCommit :: conn -> DBAdapterMonad Bool
        hdbcRollback :: conn -> DBAdapterMonad ()
        showSQLQuery :: conn -> SQLQuery -> String

instance PreparedStatement_ HDBCQueryStatement where
        execWithParams (HDBCQueryStatement ret vars stmt params) args = resultStream2 (do
                infoM "SQL" ("execute stmt")
                rcode <- execute stmt (map (\v -> convertExprToSQL (case lookup v args of
                    Just e -> e
                    Nothing -> error ("execWithParams: (all vars " ++ show params ++ ") " ++ show v ++ " is not found in " ++ show args))) params)
                if rcode == -1
                    then do
                        infoM "SQL" ("execute stmt: error ")
                        error ("execWithParams: error")
                    else if ret
                        then do
                            rows <- fetchAllRows stmt
                            infoM "SQL" ("returns " ++ show (length rows) ++ " rows")
                            return (map (convertSQLToResult vars) rows)
                        else do
                            infoM "SQL" ("updates " ++ show rcode ++ " rows")
                            return [mempty]
                ) (finish stmt)
        closePreparedStatement _ = return ()


prepareHDBCQueryStatement :: (HDBCConnection conn, IConnection conn) => conn -> SQLQuery -> IO HDBCQueryStatement
prepareHDBCQueryStatement conn sqlquery@(vars, query, params) = HDBCQueryStatement (case query of
                                                                                        SQLQ _ -> True
                                                                                        _ -> False) vars <$> prepare conn (showSQLQuery conn sqlquery) <*> pure params


instance (HDBCConnection conn, IConnection conn) => DBConnection conn SQLQuery  where
        prepareQueryStatement conn query = liftIO $ PreparedStatement <$> prepareHDBCQueryStatement conn query
        connBegin _ = return ()
        connCommit = hdbcCommit
        connPrepare = hdbcPrepare
        connRollback = hdbcRollback
        connClose conn = liftIO $ disconnect conn

-- the QueryDB instance is provided for each DB type
