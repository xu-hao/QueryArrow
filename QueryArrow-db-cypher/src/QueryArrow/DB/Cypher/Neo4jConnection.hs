{-# LANGUAGE TypeFamilies, TypeSynonymInstances, FlexibleInstances, MultiParamTypeClasses, FlexibleContexts, RankNTypes, GADTs, OverloadedStrings, InstanceSigs #-}
module QueryArrow.DB.Cypher.Neo4jConnection where

import QueryArrow.Syntax.Term
import QueryArrow.Semantics.Value
import QueryArrow.Syntax.Serialize
import QueryArrow.DB.ResultStream
import QueryArrow.DB.DB
import QueryArrow.DB.NoConnection
import QueryArrow.DB.GenericDatabase
import QueryArrow.DB.Cypher.Cypher

import Prelude hiding (lookup)
import Control.Monad.IO.Class (liftIO)
import Data.Map.Strict (insert, empty, foldlWithKey, Map, mapKeys)
import qualified Data.Map.Strict as M
import Data.Convertible.Base
import Database.Bolt as BOLT
import Data.ByteString.Char8(pack)
import Data.Text.Encoding
import qualified Data.Text as T
import Data.Int
import Data.Scientific
import Data.Default
import Data.Conduit
import qualified Data.Conduit.Combinators as C

import System.Log.Logger


-- connections


type Neo4jConnInfo = (String, Int, String, String)
type Neo4jDatabase = Neo4jConnInfo

instance Convertible ResultValue BOLT.Value where
    safeConvert (Int64Value i) = Right (BOLT.I (fromIntegral i))
    safeConvert (StringValue s) = Right (BOLT.T s)
    safeConvert (ByteStringValue s) = Right (BOLT.T (decodeUtf8 s))
    safeConvert e = Left (ConvertError (show e) "ResultValue" "Value" "unsupported param value expr type")

instance Convertible BOLT.Value ResultValue where
    safeConvert (BOLT.I i) = Right (Int64Value (fromIntegral i))
    safeConvert (BOLT.T s) = Right (StringValue s)
    safeConvert (BOLT.N ()) = Right Null
    safeConvert e = Left (ConvertError (show e) "Value" "ResultValue" "unsupported param value expr type")
    
instance Convertible BOLT.Record MapResultRow where
    safeConvert r = Right (mapKeys (Var . T.unpack) (M.map convert r))

toCypherParams :: MapResultRow -> Map T.Text BOLT.Value
toCypherParams = foldlWithKey (\m (Var k) v-> M.insert (T.pack k) (convert v) m) M.empty

instance INoConnectionDatabase2 (GenericDatabase CypherTrans Neo4jDatabase ) where
        type NoConnectionRowType (GenericDatabase CypherTrans Neo4jDatabase ) = MapResultRow
        type NoConnectionQueryType (GenericDatabase CypherTrans Neo4jDatabase ) = CypherQuery
        noConnectionDBStmtExec :: GenericDatabase CypherTrans Neo4jDatabase -> CypherQuery -> DBResultStream MapResultRow -> DBResultStream MapResultRow
        noConnectionDBStmtExec (GenericDatabase  _  (host, port, username, password) _ _) stmt@(CypherQuery vars _ _) rs = do
            rs .| awaitForever (\args -> do 
                pipe <- BOLT.connect def {
                            host = host,
                            port = port,
                            user = T.pack username,
                            password = T.pack password
                        }
                liftIO $ infoM "Cypher" (serialize stmt ++ " with " ++ show args)
                -- liftIO $ putStrLn ("Cypher: execute " ++ serialize stmt ++ " with " ++ show args)
                rows <- BOLT.run pipe (BOLT.queryP (T.pack (serialize stmt)) (toCypherParams args))
                liftIO $ infoM "Cypher" ("query returns " ++ show rows)
                C.yieldMany (map convert rows))
