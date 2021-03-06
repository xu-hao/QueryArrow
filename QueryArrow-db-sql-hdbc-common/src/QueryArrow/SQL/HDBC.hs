{-# LANGUAGE TypeFamilies, TypeSynonymInstances, MultiParamTypeClasses, FlexibleInstances, RankNTypes, GADTs #-}
module QueryArrow.SQL.HDBC where

import Prelude hiding (lookup)
import QueryArrow.DB.ParametrizedStatement
import QueryArrow.DB.DB
import QueryArrow.DB.ResultStream
import QueryArrow.Syntax.Term
import QueryArrow.Syntax.Type
import QueryArrow.Semantics.Value

import Database.HDBC
import Control.Monad.IO.Class (liftIO)
import Control.Applicative ((<$>))
import Data.Map.Strict (empty, insert, lookup)
import System.Log.Logger
import Data.Convertible
import Data.Ratio
import Data.Text (unpack)
import Data.List (intercalate)

data HDBCStatement = HDBCStatement Bool [Var] Statement [Var] -- return vars stmt param vars

escapeSQLTextArrayString :: String -> String
escapeSQLTextArrayString "" = ""
escapeSQLTextArrayString ('\"' : tl) = "\\\"" ++ escapeSQLTextArrayString tl
escapeSQLTextArrayString ('\\' : tl) = "\\\\" ++ escapeSQLTextArrayString tl
escapeSQLTextArrayString (hd : tl) = hd : escapeSQLTextArrayString tl

quote :: String -> String
quote a = "\"" ++ a ++ "\""

convertResultValueToSQLText :: ResultValue -> String
convertResultValueToSQLText (Int64Value i) = show i
convertResultValueToSQLText (StringValue s) = quote (escapeSQLTextArrayString (unpack s))
convertResultValueToSQLText crv@ListNilValue = listToSQLText crv
convertResultValueToSQLText crv@(ListConsValue _ _) = listToSQLText crv
convertResultValueToSQLText e = error ("unsupported sql value type: " ++ show e)

listToSQLText :: ResultValue -> String
listToSQLText crv = "{" ++ (intercalate "," (map convertResultValueToSQLText (valueListFromValue crv))) ++ "}"

listToSql :: ResultValue -> SqlValue
listToSql crv = toSql (listToSQLText crv)

convertResultValueToSQL :: ResultValue -> SqlValue
convertResultValueToSQL (Int64Value i) = toSql i
convertResultValueToSQL (StringValue s) = toSql s
convertResultValueToSQL (ByteStringValue s) = toSql s
convertResultValueToSQL crv@ListNilValue = listToSql crv
convertResultValueToSQL crv@(ListConsValue _ _) = listToSql crv
convertResultValueToSQL e = error ("unsupported sql expr type: " ++ show e)

convertSQLToResult :: [Var] -> [SqlValue] -> MapResultRow
convertSQLToResult vars sqlvalues = foldl (\row (var0, sqlvalue) ->
                insert var0 (case sqlvalue of
                        SqlInt32 _ -> Int64Value (fromSql sqlvalue)
                        SqlInt64 _ -> Int64Value (fromSql sqlvalue)
                        SqlInteger _ -> Int64Value (fromSql sqlvalue)
                        SqlString _ -> StringValue (fromSql sqlvalue)
                        SqlByteString _ -> StringValue (fromSql sqlvalue)
                        -- Currently the generated predicates only contains string, not bytestring. This must match the type of the predicates
                        -- SqlByteString _ -> ByteStringValue (fromSql sqlvalue)
                        SqlRational r -> Int64Value (fromIntegral (numerator r `div` denominator r))
                        SqlNull -> Null
                        _ -> error ("unsupported sql value: " ++ show sqlvalue)) row) empty (zip vars sqlvalues)

instance IPSDBStatement HDBCStatement where
        type ParameterType HDBCStatement = MapResultRow
        type PSRowType HDBCStatement = MapResultRow
        execWithParams (HDBCStatement ret vars stmt params) args = do
                liftIO $ infoM "SQL" ("execute stmt")
                liftIO $ infoM "SQL" ("execHDBCStatement: params = " ++ show args)
                let sargs = (map (\v -> convertResultValueToSQL (case lookup v args of
                        Just e -> e
                        Nothing -> error ("execWithParams: (all vars " ++ show params ++ ") " ++ show v ++ " is not found in " ++ show args))) params)
                liftIO $ infoM "SQL" ("execHDBCStatement: params = " ++ show sargs)
                rcode <- liftIO $ execute stmt sargs
                if rcode == -1
                    then do
                        liftIO $ infoM "SQL" ("execute stmt: error ")
                        error ("execWithParams: error")
                    else if ret
                        then do
                            rows <- liftIO $ fetchAllRows stmt
                            liftIO $ infoM "SQL" ("returns " ++ show (length rows) ++ " rows\n" ++ show rows )
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
