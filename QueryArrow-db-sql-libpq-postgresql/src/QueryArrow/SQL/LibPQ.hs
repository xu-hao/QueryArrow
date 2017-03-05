{-# LANGUAGE TypeFamilies, TypeSynonymInstances, MultiParamTypeClasses, FlexibleInstances, RankNTypes, GADTs, OverloadedStrings #-}
module QueryArrow.SQL.LibPQ where

import Prelude hiding (lookup)
import QueryArrow.DB.ParametrizedStatement
import QueryArrow.DB.DB
import QueryArrow.DB.ResultStream
import QueryArrow.FO.Data

import Database.PostgreSQL.LibPQ
import Control.Monad.IO.Class (liftIO)
import Control.Applicative ((<$>), (<|>))
import Control.Monad (unless, when)
import Data.Map.Strict (empty, insert, lookup)
import System.Log.Logger
import Data.IORef
import Data.ByteString (ByteString)
import Data.Binary.Put
import Data.Binary.Get
import Data.Text.Encoding
import Data.ByteString.Char8 (pack, unpack)
import Data.ByteString.Lazy (toStrict, fromStrict)
import Data.Monoid ((<>))

data LibPQStatement = LibPQStatement Connection Bool [Var] [Oid] ByteString [Var] -- return vars stmt param vars

int8oid :: Oid
int8oid = Oid 20

int4oid :: Oid
int4oid = Oid 23

varcharoid :: Oid
varcharoid = Oid 1043

byteaoid :: Oid
byteaoid = Oid 17

convertResultValueToSQL :: ResultValue -> (ByteString, Format)
convertResultValueToSQL (IntValue i) = (toStrict (runPut (putWord64be (fromIntegral i))), Binary) -- int8oid
convertResultValueToSQL (StringValue s) = (encodeUtf8 s, Binary) -- varcharoid
convertResultValueToSQL (ByteStringValue s) = (s, Binary) -- byteaoid
convertResultValueToSQL e = error ("unsupported expr type: " ++ show e)

convertSQLToResultValue :: Oid -> ByteString -> ResultValue
convertSQLToResultValue oid bs =
  if oid == int8oid
    then IntValue (fromIntegral (runGet getWord64be (fromStrict bs)))
    else if oid == int4oid
      then IntValue (fromIntegral (runGet getWord32be (fromStrict bs)))
      else if oid == varcharoid
        then StringValue (decodeUtf8 bs)
        else error ("unsupported sql expr type: " ++ show oid)
        -- Currently the generated predicates only contains string, not bytestring. This must match the type of the predicates
        -- SqlByteString _ -> ByteStringValue (fromSql sqlvalue)

convertSQLToResult :: [Var] -> [Oid] -> [Maybe ByteString] -> MapResultRow
convertSQLToResult vars oids sqlvalues = foldl (\row (var0, oid, sqlvalue) ->
                insert var0 (
                  case sqlvalue of
                    Nothing  -> Null
                    Just sqlvalue -> convertSQLToResultValue oid sqlvalue) row) empty (zip3 vars oids sqlvalues)

allRowsResultStream :: [Var] -> [Oid] -> Result -> DBResultStream MapResultRow
allRowsResultStream vars oids res = do
  Row nrows <- liftIO $ ntuples res
  Col ncols <- liftIO $ nfields res
  unless (fromIntegral ncols == length vars) $ error ("number of vars doesn't match number of columns")
  let allRowsResultStreamOffset offset =
          if nrows == offset
            then emptyResultStream
            else (do
              sqlvals <- liftIO $ mapM (\i -> getvalue res (fromIntegral offset) (fromIntegral i)) [0..ncols-1]
              return (convertSQLToResult vars oids sqlvals)) <|> allRowsResultStreamOffset (offset + 1)
  allRowsResultStreamOffset 0

instance IPSDBStatement LibPQStatement where
        type ParameterType LibPQStatement = MapResultRow
        type PSRowType LibPQStatement = MapResultRow
        execWithParams (LibPQStatement conn ret vars oids stmt params) args = do
                liftIO $ infoM "SQL" ("execute stmt")
                -- liftIO $ putStrLn ("execLibPQStatement: params = " ++ show args)
                res <- liftIO $ execPrepared conn stmt (map (\v -> Just (convertResultValueToSQL (case lookup v args of
                    Just e -> e
                    Nothing -> error ("execWithParams: (all vars " ++ show params ++ ") " ++ show v ++ " is not found in " ++ show args)))) params) Binary
                case res of
                  Nothing -> error "result is Nothing"
                  Just res ->
                    if ret
                        then do
                          nrows <- liftIO $ ntuples res
                          liftIO $ infoM "SQL" ("returns " ++ show nrows ++ " rows")
                          allRowsResultStream vars oids res
                        else do
                            cmdtuples <- liftIO $ cmdTuples res
                            case cmdtuples of
                              Nothing -> error "cmdTuples returns Nothing"
                              Just cmdtuples -> liftIO $ infoM "SQL" ("updates " ++ unpack cmdtuples ++ " rows")
                            return mempty
        psdbStmtClose (LibPQStatement conn ret vars oids stmt params) = do
          let cmd = "DEALLOCATE " <> stmt
          _ <- exec conn stmt
          return ()


nextsid :: IORef Integer -> IO Integer
nextsid sid = atomicModifyIORef' sid (\a -> (a+1, a))

castTypeToOid :: CastType -> Oid
castTypeToOid TextType = varcharoid
castTypeToOid NumberType = int8oid
castTypeToOid ByteStringType = byteaoid
castTypeToOid ty = error ("castTypeToOid: cannot find oid for type " ++ show ty)

prepareLibPQStatement :: LibPQDBConnection -> (Bool, [Var], [CastType], String, [Var]) -> IO LibPQStatement
prepareLibPQStatement (LibPQDBConnection conn _ sid) (ret, retvars, retvartypes, query, params) = do
  -- putStrLn ("prepareLibPQStatement: " ++ query)
  a <- nextsid sid
  let stmtname = "preparedStatement" <> pack (show a)
  let oids = map castTypeToOid retvartypes
  res <- prepare conn stmtname (pack query) (Just oids)
  case res of
    Nothing -> error "result is Nothing"
    Just res -> do
      st <- resultStatus res
      case st of
        CommandOk -> return ()
        _ -> error ("result is error: " ++ show res)
  return (LibPQStatement conn ret retvars oids stmtname params)

data LibPQDBConnection = LibPQDBConnection Connection Bool (IORef Integer)

instance IDBConnection0 LibPQDBConnection  where
        dbBegin _ = return ()
        dbCommit (LibPQDBConnection conn twophasecommit _) =  do
            _ <- exec conn (if twophasecommit then "COMMIT PREPARED trans" else "COMMIT")
            return ()
        dbPrepare (LibPQDBConnection conn twophasecommit _) =
            when twophasecommit $ do
              _ <- exec conn "PREPARED TRANSACTION trans"
              return ()
        dbRollback (LibPQDBConnection conn twophasecommit _) = do
            _ <- exec conn (if twophasecommit then "ROLLBACK PREPARED trans" else "ROLLBACK")
            return ()
        dbClose (LibPQDBConnection conn _ _) = finish conn

instance IDBConnection LibPQDBConnection where
        type StatementType LibPQDBConnection = PSDBStatement LibPQStatement
        type QueryType LibPQDBConnection = (Bool, [Var], [CastType], String, [Var])
        prepareQuery conn query = do
            infoM "SQL" ("prepare statement " ++ show query)
            PSDBStatement <$> prepareLibPQStatement conn query

-- the QueryDB instance is provided for each DB type
