{-# LANGUAGE TypeFamilies, TypeSynonymInstances, FlexibleInstances, MultiParamTypeClasses, FlexibleContexts, RankNTypes, GADTs, OverloadedStrings, InstanceSigs #-}
module QueryArrow.Cypher.Neo4jConnection where

import QueryArrow.FO.Data
import QueryArrow.DB.ResultStream
import QueryArrow.DB.DB
import QueryArrow.DB.NoConnection
import QueryArrow.DB.GenericDatabase
import QueryArrow.Cypher.Cypher

import Prelude hiding (lookup)
import Control.Monad.IO.Class (liftIO)
import Data.Map.Strict (insert, empty, foldlWithKey)
import Data.Convertible.Base
import qualified Database.Neo4j.Cypher as C
import qualified Database.Neo4j.Transactional.Cypher as TC
import Database.Neo4j (withAuthConnection)
import qualified Data.HashMap.Strict as M
import Data.ByteString.Char8(pack)
import Data.Text.Encoding
import qualified Data.Text as T
import Data.Aeson.Types as A
import Data.Int
import Data.Scientific

import System.Log.Logger


-- connections


type Neo4jConnInfo = (String, Int, String, String)
type Neo4jDatabase = Neo4jConnInfo

instance Convertible ResultValue C.ParamValue where
    safeConvert (IntValue i) = Right (C.newparam (fromIntegral i :: Int64))
    safeConvert (StringValue s) = Right (C.newparam s)
    safeConvert (ByteStringValue s) = Right (C.newparam (decodeUtf8 s))
    safeConvert e = Left (ConvertError (show e) "Expr" "ParamValue" "unsupported param value expr type")

instance Convertible ([Var], [A.Value]) MapResultRow where
    safeConvert (vars, values) = Right (foldl (\row (var0, value) ->
                insert var0 (case value of
                        A.Number n -> case floatingOrInteger n of
                            Left r -> error ("floating not supported")
                            Right i -> IntValue (fromIntegral i)
                        A.String text -> StringValue text
                        A.Null -> StringValue "<null>"
                        _ -> error ("unsupported json value: " ++ show value)) row) empty (zip vars values) )

toCypherParams :: MapResultRow -> M.HashMap T.Text C.ParamValue
toCypherParams = foldlWithKey (\m (Var k) v-> M.insert (T.pack k) (convert v) m) M.empty

instance INoConnectionDatabase2 (GenericDatabase CypherTrans Neo4jDatabase ) where
        type NoConnectionRowType (GenericDatabase CypherTrans Neo4jDatabase ) = MapResultRow
        type NoConnectionQueryType (GenericDatabase CypherTrans Neo4jDatabase ) = CypherQuery
        noConnectionDBStmtExec :: GenericDatabase CypherTrans Neo4jDatabase -> CypherQuery -> DBResultStream MapResultRow -> DBResultStream MapResultRow
        noConnectionDBStmtExec (GenericDatabase  _  (host, port, username, password) _ _) stmt@(CypherQuery vars _ _) rs = do
                args <- rs
                resp <- liftIO $ withAuthConnection (pack host) port (pack username, pack password) $ do
                    liftIO $ infoM "Cypher" (serialize stmt ++ " with " ++ show args)
                    -- liftIO $ putStrLn ("Cypher: execute " ++ serialize stmt ++ " with " ++ show args)
                    TC.runTransaction $ TC.cypher (T.pack (serialize stmt)) (toCypherParams args)
                case resp of
                    Left t -> do
                        let errmsg = "code: " ++ T.unpack (fst t) ++ ", msg: " ++ T.unpack (snd t)
                        liftIO $ errorM "Cypher" ("error: " ++ errmsg)
                        error errmsg
                    Right (TC.Result cols rows _ _) -> do
                        liftIO $ infoM "Cypher" ("query returns " ++  show cols ++ show rows)
                        listResultStream (map (\row -> convert (vars, row)) rows)
