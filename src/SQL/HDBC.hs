{-# LANGUAGE TypeFamilies, TypeSynonymInstances, MultiParamTypeClasses, FlexibleInstances #-}
module SQL.HDBC where

import DBQuery
import SQL.SQL
import FO


import Database.HDBC
import Control.Monad.IO.Class (liftIO)
import Control.Applicative ((<$>))
import Data.Map.Strict (empty, insert)


data HDBCStatement expr = HDBCStatement [Var] Statement

convertSQLExprToSQL :: SQLExpr -> SqlValue
convertSQLExprToSQL (SQLIntConstExpr i) = toSql i
convertSQLExprToSQL (SQLStringConstExpr s) = toSql s
convertSQLExprToSQL e = error ("unsupported sql expr type: " ++ show e)

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
        extractHDBCConnection :: conn -> ConnWrapper

instance PreparedStatement HDBCStatement SQLExpr where
        execWithParams (HDBCStatement vars stmt) args = ResultStream (\iteratee seed -> do
                liftIO $ execute stmt (map convertSQLExprToSQL args)
                let rows = fetchAllRows stmt
                foldlM2 iteratee seed (map (convertSQLToResult vars) <$> rows)
                ) where 
                        foldlM2 iteratee seed mrows = do 
                                rows <- liftIO $ mrows
                                case rows of
                                        [] -> return seed
                                        (row : rest) -> do
                                                seednew <- iteratee row seed
                                                case seednew of
                                                        Left seednew2 -> do
                                                                liftIO $ finish stmt
                                                                return seednew2
                                                        Right seednew2 ->
                                                                foldlM2 iteratee seednew2 (return rest)
        closePreparedStatement _ = return ()

instance HDBCConnection conn => DBConnection conn HDBCStatement SQLQuery SQLExpr where
        execStatement conn query = do
                stmt <- liftIO $ prepareStatement conn query
                execWithParams stmt []
        prepareStatement conn (vars, query) = HDBCStatement vars <$> prepare ( (extractHDBCConnection conn)) (show query)
        dbClose = disconnect . extractHDBCConnection

-- the QueryDB instance is provided for each DB type