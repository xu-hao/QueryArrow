{-# LANGUAGE TypeFamilies, TypeSynonymInstances, MultiParamTypeClasses, FlexibleInstances, RankNTypes, GADTs #-}
module QueryArrow.SQL.HDBC where

import Prelude hiding (lookup)
import QueryArrow.DB.ParametrizedStatement
import QueryArrow.DB.DB
import QueryArrow.Semantics.ResultStream
import QueryArrow.Semantics.ResultValue
import QueryArrow.Semantics.ResultSet
import QueryArrow.Semantics.ResultRow.VectorResultRow
import QueryArrow.Semantics.ResultHeader.VectorResultHeader
import QueryArrow.Semantics.ResultValue.AbstractResultValue
import QueryArrow.Semantics.ResultSet.VectorResultSetTransformer
import QueryArrow.Semantics.ResultSet.ResultStreamResultSet
import QueryArrow.Semantics.Sendable
import QueryArrow.Syntax.Data
import QueryArrow.Data.Monoid.Action
import QueryArrow.Data.Some

import Database.HDBC
import Control.Applicative ((<$>))
import System.Log.Logger
import qualified Data.Vector as V
import Data.Monoid
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

convertResultValueToSQLText :: ConcreteResultValue -> String
convertResultValueToSQLText (Int64Value i) = show i
convertResultValueToSQLText (StringValue s) = quote (escapeSQLTextArrayString (unpack s))
convertResultValueToSQLText crv@NilValue = listToSQLText crv
convertResultValueToSQLText crv@(ListConsValue _ _) = listToSQLText crv
convertResultValueToSQLText e = error ("unsupported sql value type: " ++ show e)

listToSQLText :: ConcreteResultValue -> String
listToSQLText crv = "{" ++ (intercalate "," (map convertResultValueToSQLText (valueListFromValue crv))) ++ "}"

listToSql :: ConcreteResultValue -> SqlValue
listToSql crv = toSql (listToSQLText crv)

convertResultValueToSQL :: ConcreteResultValue -> SqlValue
convertResultValueToSQL (Int64Value i) = toSql i
convertResultValueToSQL (StringValue s) = toSql s
convertResultValueToSQL (ByteStringValue s) = toSql s
convertResultValueToSQL crv@NilValue = listToSql crv
convertResultValueToSQL crv@(ListConsValue _ _) = listToSql crv
convertResultValueToSQL e = error ("unsupported sql expr type: " ++ show e)


convertSQLToResult :: SqlValue -> AbstractResultValue
convertSQLToResult sqlvalue = Some (case sqlvalue of
                        SqlInt32 _ -> Int64Value (fromSql sqlvalue)
                        SqlInt64 _ -> Int64Value (fromSql sqlvalue)
                        SqlInteger _ -> Int64Value (fromSql sqlvalue)
                        SqlString _ -> StringValue (fromSql sqlvalue)
                        SqlByteString _ -> StringValue (fromSql sqlvalue)
                        -- Currently the generated predicates only contains string, not bytestring. This must match the type of the predicates
                        -- SqlByteString _ -> ByteStringValue (fromSql sqlvalue)
                        SqlRational r -> Int64Value (fromIntegral (numerator r `div` denominator r))
                        SqlNull -> Null
                        _ -> error ("unsupported sql value: " ++ show sqlvalue))

convertSQLToResultRow :: [SqlValue] -> VectorResultRow AbstractResultValue
convertSQLToResultRow sqlvalues = V.map convertSQLToResult (V.fromList sqlvalues)

convertSQLToResultHeader :: [Var] -> ResultHeader
convertSQLToResultHeader vars = V.fromList vars

data HDBCResultSet = HDBCResultSet (ResultSetTransformer AbstractResultValue) ResultHeader [VectorResultRow AbstractResultValue]

instance Sendable HDBCResultSet where
  send h a =
    send h (ResultStreamResultSet (RSId :: ResultSetTransformer AbstractResultValue) (getHeader a) (toResultStream a))

instance Sendable (Partial HDBCResultSet) where
  send h (Partial a) =
    send h (toResultStream a)

instance Action (ResultSetTransformer AbstractResultValue) HDBCResultSet where
  trans `act` HDBCResultSet trans2 hdr rows = HDBCResultSet (trans <> trans2) hdr rows

instance ResultSet HDBCResultSet where
  type ResultSetRowType HDBCResultSet = VectorResultRow AbstractResultValue
  type ResultSetTransType HDBCResultSet = ResultSetTransformer AbstractResultValue
  getHeader (HDBCResultSet trans hdr _) = (tconvert trans :: ResultHeaderTransformer) `act` hdr
  toResultStream (HDBCResultSet trans _ rows) = listResultStream (map ((tconvert trans :: ResultRowTransformer AbstractResultValue) `act`) rows)

instance IPSDBStatement HDBCStatement where
        type ParameterType HDBCStatement = (ResultHeader, VectorResultRow AbstractResultValue)
        type PSResultSetType HDBCStatement = HDBCResultSet
        execWithParams (HDBCStatement ret vars stmt params) (hdr, args) = do
                infoM "SQL" ("execute stmt")
                -- liftIO $ putStrLn ("execHDBCStatement: params = " ++ show args)
                rcode <- execute stmt (map (\v -> convertResultValueToSQL (case args V.! case V.findIndex (== v) hdr of
                    Just e -> e
                    Nothing -> error ("execWithParams: (all vars " ++ show params ++ ") " ++ show v ++ " is not found in " ++ show (hdr, args)) of
                                        Some arv -> toConcreteResultValue arv)) params)
                if rcode == -1
                    then do
                        infoM "SQL" ("execute stmt: error ")
                        error ("execWithParams: error")
                    else if ret
                        then do
                            rows <- fetchAllRows stmt
                            infoM "SQL" ("returns " ++ show (length rows) ++ " rows")
                            let hdrret = convertSQLToResultHeader vars
                            return ((combineRow hdr args hdrret :: ResultSetTransformer AbstractResultValue) `act` HDBCResultSet RSId hdrret (map convertSQLToResultRow rows))
                        else do
                            infoM "SQL" ("updates " ++ show rcode ++ " rows")
                            return (HDBCResultSet RSId hdr [args])
        psdbStmtClose (HDBCStatement ret vars stmt params) = finish stmt
        psdbGetHeader (HDBCStatement _ vars _ _) = convertSQLToResultHeader vars


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

instance Convertible (ResultHeader, VectorResultRow AbstractResultValue)
                     (ResultHeader, VectorResultRow AbstractResultValue) where
    safeConvert = return

instance IDBConnection HDBCDBConnection where
        type StatementType HDBCDBConnection = PSDBStatement HDBCStatement
        type QueryType HDBCDBConnection = (Bool, [Var], [CastType], String, [Var])
        prepareQuery (HDBCDBConnection conn) query = do
            infoM "SQL" ("prepare statement " ++ show query)
            PSDBStatement <$> prepareHDBCStatement conn query

-- the QueryDB instance is provided for each DB type
