{-# LANGUAGE TypeFamilies, TypeSynonymInstances, MultiParamTypeClasses, FlexibleInstances, RankNTypes, GADTs #-}
module QueryArrow.SQL.HDBC where

import Prelude hiding (lookup)
import QueryArrow.DB.ParametrizedStatement
import QueryArrow.DB.DB
import QueryArrow.DB.ResultStream
import QueryArrow.FO.Data

import Database.HDBC
import Control.Monad.IO.Class (liftIO)
import Control.Applicative ((<$>))
import Data.Map.Strict (empty, insert, lookup)
import System.Log.Logger
import Data.Convertible

data HDBCStatement = HDBCStatement Bool [Var] Statement [Var] -- return vars stmt param vars

convertResultValueToSQL :: ResultValue -> SqlValue
convertResultValueToSQL (IntValue i) = toSql i
convertResultValueToSQL (StringValue s) = toSql s
convertResultValueToSQL (ByteStringValue s) = toSql s
convertResultValueToSQL e = error ("unsupported sql expr type: " ++ show e)

convertSQLToResult :: [Var] -> [SqlValue] -> MapResultRow
convertSQLToResult vars sqlvalues = foldl (\row (var0, sqlvalue) ->
                insert var0 (case sqlvalue of
                        SqlInt32 _ -> IntValue (fromSql sqlvalue)
                        SqlInt64 _ -> IntValue (fromSql sqlvalue)
                        SqlInteger _ -> IntValue (fromSql sqlvalue)
                        SqlString _ -> StringValue (fromSql sqlvalue)
                        SqlByteString _ -> StringValue (fromSql sqlvalue)
                        -- Currently the generated predicates only contains string, not bytestring. This must match the type of the predicates
                        -- SqlByteString _ -> ByteStringValue (fromSql sqlvalue)
                        SqlNull -> Null
                        _ -> error ("unsupported sql value: " ++ show sqlvalue)) row) empty (zip vars sqlvalues)

instance IPSDBStatement HDBCStatement where
        type ParameterType HDBCStatement = MapResultRow
        type PSRowType HDBCStatement = MapResultRow
        execWithParams (HDBCStatement ret vars stmt params) args = do
                liftIO $ infoM "SQL" ("execute stmt")
                -- liftIO $ putStrLn ("execHDBCStatement: params = " ++ show args)
                rcode <- liftIO $ execute stmt (map (\v -> convertResultValueToSQL (case lookup v args of
                    Just e -> e
                    Nothing -> error ("execWithParams: (all vars " ++ show params ++ ") " ++ show v ++ " is not found in " ++ show args))) params)
                if rcode == -1
                    then do
                        liftIO $ infoM "SQL" ("execute stmt: error ")
                        error ("execWithParams: error")
                    else if ret
                        then do
                            rows <- liftIO $ fetchAllRows stmt
                            liftIO $ infoM "SQL" ("returns " ++ show (length rows) ++ " rows")
                            listResultStream (map (convertSQLToResult vars) rows)
                        else do
                            liftIO $ infoM "SQL" ("updates " ++ show rcode ++ " rows")
                            return mempty
        psdbStmtClose (HDBCStatement ret vars stmt params) = finish stmt


prepareHDBCStatement :: (IConnection conn) => conn -> (Bool, [Var], [CastType], String, [Var]) -> IO HDBCStatement
prepareHDBCStatement conn (ret, retvars, retvartypes, query, params) = do
  -- putStrLn ("prepareHDBCStatement: " ++ query)
  HDBCStatement ret retvars <$> prepare conn query <*> pure params

data HDBCDBConnection where
    HDBCDBConnection :: forall conn. (IConnection conn) => conn -> HDBCDBConnection

instance IDBConnection0 HDBCDBConnection  where
        dbBegin _ = return ()
        dbCommit (HDBCDBConnection conn) =  do
            commit conn
        dbPrepare (HDBCDBConnection conn) =
            return ()
        dbRollback (HDBCDBConnection conn) = rollback conn
        dbClose (HDBCDBConnection conn) = disconnect conn

instance IDBConnection HDBCDBConnection where
        type StatementType HDBCDBConnection = PSDBStatement HDBCStatement
        type QueryType HDBCDBConnection = (Bool, [Var], [CastType], String, [Var])
        prepareQuery (HDBCDBConnection conn) query = do
            infoM "SQL" ("prepare statement " ++ show query)
            PSDBStatement <$> prepareHDBCStatement conn query

-- the QueryDB instance is provided for each DB type
