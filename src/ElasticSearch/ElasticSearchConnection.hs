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
import Control.Monad (zipWithM_)
import Control.Monad.IO.Class (liftIO, MonadIO)
import Control.Applicative ((<|>))

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

convertHitToMapResultRow :: Text -> Map Text ElasticSearchQueryExpr -> ESHit -> MapResultRow
convertHitToMapResultRow _ map2 eshit =
    let (ESRecord map1) = _source eshit in
        foldrWithKey (\key val map3 -> case val of
            ElasticSearchQueryVar var ->
                case lookup key map1 of
                    Just val2 -> insert var (convert val2) map3
                    Nothing -> map3
            _ -> map3) empty map2

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

recordToQuery :: Map Text ElasticSearchQueryExpr -> Map Var Expr -> Int -> Int -> ESQ.ESQuery
recordToQuery rec args off lim = ESQ.ESQuery off lim (
                        ESQ.ESBoolQuery (
                            ESQ.ESMustQuery (
                                foldrWithKey (\key val list -> extractQueryItem key val args ++ list) [] rec
                            )
                        )
                    )

recordToESRecord :: Map Text ElasticSearchQueryExpr -> Map Var Expr -> ESRecord
recordToESRecord rec args =
    ESRecord (foldrWithKey (\key val list -> insert key (snd (extractInsertItem key val args)) list) empty rec)


esResultStream :: MonadIO m => ESQ.ElasticSearchConnInfo -> Text -> Map Text ElasticSearchQueryExpr -> Map Var Expr -> ResultStream m ESHit
esResultStream esci type0 rec args = do
        let getrs off lim = do
                let esquery = recordToQuery rec args off lim
                rows <- liftIO $ do
                    res <- ESQ.queryBySearch esci type0 esquery
                    case res of
                        Left res1 -> error ("execWithParams: cannot decode response " ++ res1)
                        Right (ESQueryResult _ _ _ (ESQueryResultHits _ _ hits1)) -> return hits1
                case rows of
                    [] -> emptyResultStream
                    _ -> do
                        listResultStream rows <|> getrs (off + lim) lim
        getrs 0 page

instance PreparedStatement_ ElasticSearchStatement where
    execWithParams (ElasticSearchStatement esci qu@(ElasticSearchQuery type0 rec)) args = do
        hit <- esResultStream esci type0 rec args
        return (convertHitToMapResultRow type0 rec hit)

    execWithParams (ElasticSearchStatement esci (ElasticSearchInsert type0 rec)) args =
        let esquery =
                ESRecord
                    (fromList (foldrWithKey (\key val list -> extractInsertItem key val args : list) [] rec)) in
            resultStream2 (do
                _ <- ESQ.postESRecord esci type0 esquery
                return [mempty]
                ) (return ())

    execWithParams (ElasticSearchStatement esci (ElasticSearchDelete type0 rec)) args = (do
        hit <- esResultStream esci type0 rec args
        liftIO $ ESQ.deleteById esci type0 (_id hit)
        emptyResultStream) <|> return mempty

    execWithParams (ElasticSearchStatement esci (ElasticSearchUpdateProperty type0 rec updaterec)) args = (do
        hit <- esResultStream esci type0 rec args
        let rec = _source hit
            id0 = _id hit
            updatedrec = updateProps (recordToESRecord updaterec args) rec
        liftIO $ ESQ.updateESRecord esci type0 id0 updatedrec
        emptyResultStream
        ) <|> return mempty

    execWithParams (ElasticSearchStatement esci (ElasticSearchDeleteProperty type0 rec diff)) args = (do
        hit <- esResultStream esci type0 rec args
        let rec = _source hit
            id0 = _id hit
            updatedrec = deleteProps diff rec
        liftIO $ ESQ.updateESRecord esci type0 id0 updatedrec
        emptyResultStream
        ) <|> return mempty

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
