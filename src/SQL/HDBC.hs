{-# LANGUAGE TypeFamilies, TypeSynonymInstances, MultiParamTypeClasses, FlexibleInstances, RankNTypes, GADTs, TypeFamilies #-}
module SQL.HDBC where

import Prelude hiding (lookup)
import DBQuery
import SQL.SQL
import DB
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


instance PreparedStatement HDBCQueryStatement where
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


prepareHDBCQueryStatement :: (IConnection conn) => conn -> (Bool, [Var], String, [Var]) -> IO HDBCQueryStatement
prepareHDBCQueryStatement conn (ret, retvars, query, params) = HDBCQueryStatement ret retvars <$> prepare conn query <*> pure params

data HDBCConnectionDBConnection where
    HDBCConnectionDBConnection :: forall conn. (IConnection conn) => conn -> HDBCConnectionDBConnection

instance DBConnection0 HDBCConnectionDBConnection  where
        dbBegin _ = return ()
        dbCommit (HDBCConnectionDBConnection conn) =  do
            commit conn
            return True
        dbPrepare (HDBCConnectionDBConnection conn) =
            return True
        dbRollback (HDBCConnectionDBConnection conn) = rollback conn
        dbClose (HDBCConnectionDBConnection conn) = disconnect conn

instance DBConnection HDBCConnectionDBConnection where
        type StatementType HDBCConnectionDBConnection = PreparedDBStatement HDBCQueryStatement
        type QueryType HDBCConnectionDBConnection = (Bool, [Var], String, [Var])
        prepareQuery (HDBCConnectionDBConnection conn) _ query _ = PreparedDBStatement <$> prepareHDBCQueryStatement conn query

-- the QueryDB instance is provided for each DB type
