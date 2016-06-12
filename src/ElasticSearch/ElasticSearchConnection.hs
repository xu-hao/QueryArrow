{-# LANGUAGE DeriveGeneric, MultiParamTypeClasses, FlexibleInstances, FlexibleContexts #-}

module ElasticSearch.ElasticSearchConnection where

-- http://swizec.com/blog/writing-a-rest-client-in-haskell/swizec/6152

import Prelude hiding (lookup)
import qualified Data.ByteString.Lazy.Char8 as BL8
import Data.Map.Strict hiding (map, elemAt)
import Data.Text (unpack, pack, Text)
import Data.Char (toLower)
import Data.Convertible
import Data.Scientific (toBoundedInteger)
import Data.Aeson (Value (String, Number))

import FO.Domain
import FO.Data
import DBQuery
import QueryPlan
import ResultStream
import Config

import ElasticSearch.Record
import qualified ElasticSearch.Query as ESQ
import ElasticSearch.QueryResult
import ElasticSearch.QueryResultHits
import ElasticSearch.ESQL

import Debug.Trace

type ElasticSearchConnection = ESQ.ElasticSearchConnInfo
data ElasticSearchStatement = ElasticSearchStatement ElasticSearchConnection ElasticSearchQuery

convertExprToString :: Expr -> String
convertExprToString (IntExpr i) = show i
convertExprToString (StringExpr s) = unpack s
convertExprToString _ = error ("unsupported param value expr type")

instance Convertible Value ResultValue where
    safeConvert (String s) = Right (StringValue s)
    safeConvert (Number n) = case toBoundedInteger n of
                                Just n1 -> Right (IntValue n1)
                                Nothing -> Left (ConvertError "Value" "ResultValue" (show n) "")
    safeConvert a = Left (ConvertError "Value" "ResultValue" (show a) "")

convertHitToMapResultRow :: ElasticSearchQuery -> ESHit -> MapResultRow
convertHitToMapResultRow esquery eshit =
    case esquery of
        ElasticSearchQuery _ map2 ->
            let (ESRecord map1) = _source eshit in
                foldrWithKey (\key val map3 -> case val of
                    ElasticSearchQueryVar var ->
                        case lookup key map1 of
                            Just val2 -> insert var (convert val2) map3
                            Nothing -> map3
                    _ -> map3) empty map2
        _ -> empty


extractQueryItem :: Text -> ElasticSearchQueryExpr -> Map Var Expr -> [ESQ.ESTermQuery]
extractQueryItem key val args = case val of
    ElasticSearchQueryParam i ->
        case lookup i args of
            Just expr ->
                case expr of
                    StringExpr s -> [ESQ.ESTermQuery (ESQ.ESStrTermQuery key s)]
                    IntExpr i -> [ESQ.ESTermQuery (ESQ.ESIntTermQuery key i)]
                    _ -> error "unsupported expr type"
            Nothing -> error "no such argument"
    ElasticSearchQueryIntVal val ->
        [ESQ.ESTermQuery (ESQ.ESIntTermQuery key val)]
    ElasticSearchQueryStrVal val ->
        [ESQ.ESTermQuery (ESQ.ESStrTermQuery key val)]
    ElasticSearchQueryVar _ ->
        []

extractInsertItem :: Text -> ElasticSearchQueryExpr -> Map Var Expr -> (Text, Value)
extractInsertItem key val args = case val of
    ElasticSearchQueryParam i ->
        case lookup i args of
            Just expr ->
                case expr of
                    StringExpr s -> (key, String s)
                    IntExpr i -> (key, Number (fromInteger (toInteger i)))
                    _ -> error "unsupported expr type"
            Nothing -> error "no such argument"
    ElasticSearchQueryIntVal val ->
        (key, Number (fromInteger (toInteger val)))
    ElasticSearchQueryStrVal val ->
        (key, String val)
    ElasticSearchQueryVar _ ->
        error "var"

page :: Int
page = 100
instance PreparedStatement_ ElasticSearchStatement where
        execWithParams (ElasticSearchStatement esci qu@(ElasticSearchQuery type0 rec)) args =
            let esquery =
                    ESQ.ESQuery 0 page (
                        ESQ.ESBoolQuery (
                            ESQ.ESMustQuery (
                                foldrWithKey (\key val list -> extractQueryItem key val args ++ list) [] rec
                            )
                        )
                    ) in
                resultStream2 (do
                    res <- ESQ.queryBySearch esci type0 esquery
                    case res of
                        Left res1 -> error ("execWithParams: cannot decode response " ++ res1)
                        Right (ESQueryResult _ _ _ (ESQueryResultHits _ _ hits1)) -> return (map (convertHitToMapResultRow qu) hits1)) (return ())

        execWithParams (ElasticSearchStatement esci (ElasticSearchInsert type0 rec)) args =
            let esquery =
                    ESRecord
                        (fromList (foldrWithKey (\key val list -> extractInsertItem key val args : list) [] rec)) in
                resultStream2 (do
                    _ <- ESQ.postESRecord esci type0 esquery
                    return [mempty]
                    ) (return ())

        execWithParams (ElasticSearchStatement esci (ElasticSearchDelete type0 rec)) args =
            let esquery =
                    ESQ.ESQuery 0 page (
                        ESQ.ESBoolQuery (
                            ESQ.ESMustQuery (
                                foldrWithKey (\key val list -> extractQueryItem key val args ++ list) [] rec
                            )
                        )
                    ) in
                resultStream2 (do
                    res <- ESQ.queryBySearch esci type0 esquery
                    case res of
                        Left res1 -> error ("execWithParams: cannot decode response " ++ res1)
                        Right (ESQueryResult _ _ _ (ESQueryResultHits _ _ hits1)) -> do
                            trace ("ElasticSearch deleting " ++ show hits1) $ mapM_ (\hit -> ESQ.deleteById esci type0 (_id hit)) hits1
                            return [mempty]
                            ) (return ())
        closePreparedStatement _ = return ()

instance DBConnection ElasticSearchConnection ElasticSearchQuery where
        prepareQueryStatement conn query = return (PreparedStatement (ElasticSearchStatement conn query))
        connClose _ = return ()
        connCommit _ = return True
        connPrepare _ = return True
        connRollback _ = return ()
        connBegin _ = return ()


instance DBConnection conn ElasticSearchQuery  => ExtractDomainSize DBAdapterMonad conn ESTrans where
    extractDomainSize _ _ _ (Atom _ args) =
        return (concatMap (\arg -> case arg of
                (VarExpr v) -> [v]
                _ -> []) args)
