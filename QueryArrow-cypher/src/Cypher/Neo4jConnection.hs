{-# LANGUAGE TypeFamilies, TypeSynonymInstances, FlexibleInstances, MultiParamTypeClasses, FlexibleContexts, RankNTypes, GADTs, OverloadedStrings, InstanceSigs #-}
module Cypher.Neo4jConnection where

import FO.Data
import DB.ResultStream
import DB.DB
import DB.NoConnection
import DB.GenericDatabase
import Cypher.Cypher
import Utils

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


type Neo4jConnInfo = (String, Int, String, String)
type Neo4jDatabase = Neo4jConnInfo

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

instance INoConnectionDatabase2 (GenericDatabase CypherTrans Neo4jDatabase ) where
        type NoConnectionRowType (GenericDatabase CypherTrans Neo4jDatabase ) = MapResultRow
        type NoConnectionQueryType (GenericDatabase CypherTrans Neo4jDatabase ) = CypherQuery
        noConnectionDBStmtExec :: (GenericDatabase CypherTrans Neo4jDatabase ) -> CypherQuery -> DBResultStream MapResultRow -> DBResultStream MapResultRow
        noConnectionDBStmtExec (GenericDatabase  _  (host, port, username, password) _ _) stmt@(CypherQuery vars _ _) rs = do
                row <- rs
                let args = convert row
                resp <- liftIO $ withAuthConnection (pack host) port (pack username, pack password) $ do
                    liftIO $ infoM "Cypher" (serialize stmt ++ " with " ++ show args)
                    liftIO $ putStrLn ("Cypher: execute " ++ serialize stmt ++ " with " ++ show args)
                    C.cypher (T.pack (serialize stmt)) (toCypherParams args)
                case resp of
                    Left t -> do
                        liftIO $ errorM "Cypher" ("returns an error: " ++ T.unpack t)
                        error (T.unpack t)
                    Right (C.Response cols rows) -> do
                        liftIO $ infoM "Cypher" ("query returns " ++  show cols ++ show rows)
                        listResultStream (map (\row -> convert (vars, row)) rows)
