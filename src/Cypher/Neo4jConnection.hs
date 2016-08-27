{-# LANGUAGE TypeFamilies, TypeSynonymInstances, FlexibleInstances, MultiParamTypeClasses, FlexibleContexts, RankNTypes, GADTs, OverloadedStrings #-}
module Cypher.Neo4jConnection where

import FO.Data
import DB.GenericDatabase
import DB.ResultStream
import Cypher.Cypher
import QueryPlan

import Prelude hiding (lookup)
import Control.Monad.IO.Class (liftIO)
import Data.Map.Strict (insert, empty, Map, foldlWithKey)
import Data.Convertible.Base
import qualified Database.Neo4j.Cypher as C
import Database.Neo4j (withAuthConnection)
import qualified Data.HashMap.Strict as M
import Data.ByteString.Char8(pack)
import qualified Data.Text as T
import Data.Aeson.Types as A
import Data.Int
import Data.Scientific

import System.Log.Logger


-- connections


data Neo4jQueryStatement = Neo4jQueryStatement Neo4jConnection CypherQuery
type Neo4jConnInfo = (String, Int, String, String)
type Neo4jConnection = Neo4jConnInfo

instance Convertible Expr C.ParamValue where
    safeConvert (IntExpr i) = Right (C.newparam (fromIntegral i :: Int64))
    safeConvert (StringExpr s) = Right (C.newparam s)
    safeConvert e = Left (ConvertError (show e) "Expr" "ParamValue" "unsupported param value expr type")

instance Convertible ([Var], [A.Value]) MapResultRow where
    safeConvert (vars, values) = Right (foldl (\row (var, value) ->
                insert var (case value of
                        A.Number n -> case floatingOrInteger n of
                            Left r -> error ("floating not supported")
                            Right i -> IntValue (fromIntegral i)
                        A.String text -> StringValue (text)
                        A.Null -> StringValue "<null>"
                        _ -> error ("unsupported json value: " ++ show value)) row) empty (zip vars values) )

toCypherParams :: Map Var Expr -> M.HashMap T.Text C.ParamValue
toCypherParams = foldlWithKey (\m (Var k) v-> M.insert (T.pack k) (convert v) m) M.empty

instance PreparedStatement_ Neo4jQueryStatement where
        execWithParams (Neo4jQueryStatement (host, port, username, password) stmt@(CypherQuery vars _ _)) args = resultStream2 (do
                resp <- withAuthConnection (pack host) port (pack username, pack password) $ do
                    liftIO $ infoM "Cypher" (show stmt ++ " with " ++ show args)
                    C.cypher (T.pack (show stmt)) (toCypherParams args)
                case resp of
                    Left t -> do
                        liftIO $ errorM "Cypher" ("returns an error: " ++ T.unpack t)
                        error (T.unpack t)
                    Right (C.Response cols rows) -> do
                        liftIO $ infoM "Cypher" ("query returns " ++  show cols ++ show rows)
                        return (map (\row -> convert (vars, row)) rows)
                ) (return ())
        closePreparedStatement _ = return ()

instance DBConnection Neo4jConnection where
        prepareQueryStatement conn (_, _, query, _) = return (PreparedStatement (Neo4jQueryStatement conn (read query)))
        connClose _ = return ()
        connCommit _ = return True
        connPrepare _ = return True
        connRollback _ = return ()
        connBegin _ = return ()
