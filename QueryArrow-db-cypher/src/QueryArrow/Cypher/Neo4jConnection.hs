{-# LANGUAGE TypeFamilies, TypeSynonymInstances, FlexibleInstances, MultiParamTypeClasses, FlexibleContexts, RankNTypes, GADTs, OverloadedStrings, InstanceSigs #-}
module QueryArrow.Cypher.Neo4jConnection where

import QueryArrow.Syntax.Data
import QueryArrow.Semantics.ResultValue
import QueryArrow.Semantics.ResultStream
import QueryArrow.Semantics.ResultValue.AbstractResultValue
import QueryArrow.Semantics.ResultRow.VectorResultRow
import QueryArrow.Semantics.ResultHeader.VectorResultHeader
import QueryArrow.Semantics.ResultHeader
import QueryArrow.Semantics.ResultSet.ResultStreamResultSet
import QueryArrow.Semantics.ResultSet.VectorResultSetTransformer
import QueryArrow.Semantics.ResultSet
import QueryArrow.DB.DB
import QueryArrow.DB.NoConnection
import QueryArrow.DB.GenericDatabase
import QueryArrow.Cypher.Cypher
import QueryArrow.Data.Monoid.Action

import Prelude hiding (lookup)
import Control.Monad.IO.Class (liftIO)
import Data.Map.Strict (insert, empty, foldlWithKey)
import Data.Convertible.Base
import qualified Database.Neo4j.Cypher as C
import qualified Database.Neo4j.Transactional.Cypher as TC
import Database.Neo4j (withAuthConnection)
import qualified Data.HashMap.Strict as M
import Data.ByteString.Char8(pack)
import Data.Text.Encoding (decodeUtf8)
import qualified Data.Text as T
import Data.Aeson.Types as A
import Data.Int
import Data.Scientific
import qualified Data.Vector as V
import Data.Conduit
import Data.Conduit.List (sourceList)
import QueryArrow.Data.Some

import System.Log.Logger


-- connections


type Neo4jConnInfo = (String, Int, String, String)
type Neo4jDatabase = Neo4jConnInfo

toParamValue :: ConcreteResultValue -> C.ParamValue
toParamValue (Int64Value i) =  C.newparam (fromIntegral i :: Int64)
toParamValue  (StringValue s) =  C.newparam s
toParamValue  (ByteStringValue s) =  C.newparam (decodeUtf8 s)
toParamValue  e = error  (show e ++ "Expr to ParamValue unsupported param value expr type")

toResultRow :: [A.Value] -> VectorResultRow AbstractResultValue
toResultRow values = V.map (\value -> Some (case value of
                        A.Number n -> case floatingOrInteger n of
                            Left r -> error ("floating not supported")
                            Right i -> Int64Value (fromIntegral i)
                        A.String text -> StringValue text
                        A.Null -> StringValue "<null>"
                        _ -> error ("unsupported json value: " ++ show value))) (V.fromList values)

toCypherParams :: ResultHeader -> VectorResultRow AbstractResultValue -> M.HashMap T.Text C.ParamValue
toCypherParams hdr row = V.foldl (\m (Var k, v) -> M.insert (T.pack k) (toParamValue (case v of Some arv -> toConcreteResultValue arv)) m) M.empty (V.zip hdr row)

instance INoConnectionDatabase2 (GenericDatabase CypherTrans Neo4jDatabase ) where
        type NoConnectionInputRowType (GenericDatabase CypherTrans Neo4jDatabase ) = VectorResultRow AbstractResultValue
        type NoConnectionQueryType (GenericDatabase CypherTrans Neo4jDatabase ) = CypherQuery
        type NoConnectionResultSetType (GenericDatabase CypherTrans Neo4jDatabase ) = ResultStreamResultSet (ResultSetTransformer AbstractResultValue) (VectorResultRow AbstractResultValue)
        noConnectionDBStmtExec (GenericDatabase  _  (host, port, username, password) _ _) stmt@(CypherQuery ret _ _) rset = do
          let hdr = getHeader rset
          let rs = toResultStream rset
          let hdr2 = toHeader ret :: ResultHeader
          let hdr3 = hdr V.++ hdr2
          return (ResultStreamResultSet RSId hdr3 (ResultStream (runResultStream rs =$= awaitForever (\args -> do
                let rowtrans = tconvert (combineRow hdr args hdr2 :: ResultSetTransformer AbstractResultValue) :: ResultRowTransformer AbstractResultValue
                resp <- liftIO $ withAuthConnection (pack host) port (pack username, pack password) $ do
                    liftIO $ infoM "Cypher" (serialize stmt ++ " with " ++ show args)
                    -- liftIO $ putStrLn ("Cypher: execute " ++ serialize stmt ++ " with " ++ show args)
                    TC.runTransaction $ TC.cypher (T.pack (serialize stmt)) (toCypherParams hdr args)
                case resp of
                    Left t -> do
                        let errmsg = "code: " ++ T.unpack (fst t) ++ ", msg: " ++ T.unpack (snd t)
                        liftIO $ errorM "Cypher" ("error: " ++ errmsg)
                        error errmsg
                    Right (TC.Result cols rows _ _) -> do
                        liftIO $ infoM "Cypher" ("query returns " ++  show cols ++ show rows)
                        sourceList (map (rowtrans `act`) (map toResultRow rows))))))
