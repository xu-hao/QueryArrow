{-# LANGUAGE TypeFamilies, TypeSynonymInstances, FlexibleInstances, MultiParamTypeClasses, FlexibleContexts, RankNTypes, GADTs #-}
module Cypher.Neo4jConnection where

import FO.Data
import DBQuery
import ResultStream
import Cypher.Cypher
import QueryPlan

import Prelude hiding (lookup)
import Control.Monad.IO.Class (liftIO)
import Data.Map.Strict (insert, empty)
import Data.Convertible.Base
import qualified Database.Neo4j.Cypher as C
import Database.Neo4j (withConnection)
import qualified Data.HashMap.Strict as M
import Data.ByteString.Char8(pack)
import qualified Data.Text as T
import Data.Aeson.Types as A
import Data.Int
import Data.Scientific

-- connections


data Neo4jQueryStatement = Neo4jQueryStatement Neo4jConnection CypherQuery
data Neo4jInsertStatement = Neo4jInsertStatement Neo4jConnection Cypher
type Neo4jConnInfo = (String, Int)
type Neo4jConnection = Neo4jConnInfo

instance Convertible Expr C.ParamValue where
    safeConvert (IntExpr i) = Right (C.newparam (fromIntegral i :: Int64))
    safeConvert (StringExpr s) = Right (C.newparam (T.pack s))
    safeConvert e = Left (ConvertError "" "" ""     ("unsupported param value expr type: " ++ show e))

instance Convertible ([Var], [A.Value]) MapResultRow where
    safeConvert (vars, values) = Right (foldl (\row (var, value) ->
                insert var (case value of
                        A.Number n -> case floatingOrInteger n of
                            Left r -> error ("floating not supported")
                            Right i -> IntValue (fromIntegral i)
                        A.String text -> StringValue (T.unpack text)
                        A.Null -> StringValue "<null>"
                        _ -> error ("unsupported json value: " ++ show value)) row) empty (zip vars values) )

instance PreparedStatement_ Neo4jQueryStatement where
        execWithParams (Neo4jQueryStatement (host, port) (vars, stmt)) args = resultStream2 (do
                resp <- liftIO $ withConnection (pack host) port $ do
                    C.cypher (T.pack (show stmt)) M.empty
                case resp of
                    Left t -> error (T.unpack t)
                    Right (C.Response cols rows) ->
                        return (map (\row -> convert (vars, row)) rows)
                ) (return ())
        closePreparedStatement _ = return ()

instance PreparedStatement_ Neo4jInsertStatement where
        execWithParams (Neo4jInsertStatement (host, port) stmt) args = resultStream2 (do
                  resp <- liftIO $ withConnection (pack host) port $ do
                      C.cypher (T.pack (show stmt)) M.empty
                  case resp of
                      Left t -> error (T.unpack t)
                      Right (C.Response cols rows) ->
                          return (map (\row -> convert ([Var "i"], row)) rows)
                  ) (return ())
        closePreparedStatement _ = return ()

instance DBConnection Neo4jConnection CypherQuery Cypher where
        prepareQueryStatement conn query = return (PreparedStatement (Neo4jQueryStatement conn query))
        prepareInsertStatement conn query = return (PreparedStatement (Neo4jInsertStatement conn query))
        connClose _ = return ()
        connCommit _ = return ()
        connRollback _ = return ()
        connBegin _ = return ()
