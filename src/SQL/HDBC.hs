{-# LANGUAGE TypeFamilies, TypeSynonymInstances, MultiParamTypeClasses, FlexibleInstances #-}
module SQL.HDBC where

import DBQuery
import SQL.SQL
import FO


import Database.HDBC
import Control.Monad.IO.Class (liftIO)
import Control.Applicative ((<$>))
import Data.Map.Strict (empty, insert)


data HDBCStatement = HDBCStatement [Var] Statement

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
        extractHDBCConnection :: conn -> ConnWrapper
        showSQL :: conn -> SQL -> String

instance PreparedStatement_ HDBCStatement where
        execWithParams (HDBCStatement vars stmt) args = ResultStream (\iteratee seed -> do
                liftIO $ execute stmt (map convertExprToSQL args)
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

prepareHDBCStatement :: HDBCConnection conn => conn -> SQLQuery -> IO HDBCStatement
prepareHDBCStatement conn (vars, query) = HDBCStatement vars <$> prepare ( (extractHDBCConnection conn)) (showSQL conn query)

instance HDBCConnection conn => DBConnection conn SQLQuery where
        execStatement conn query = do
                stmt <- liftIO $ prepareHDBCStatement conn query
                execWithParams stmt []
        prepareStatement conn query = PreparedStatement <$> prepareHDBCStatement conn query
        dbClose = disconnect . extractHDBCConnection

-- the QueryDB instance is provided for each DB type