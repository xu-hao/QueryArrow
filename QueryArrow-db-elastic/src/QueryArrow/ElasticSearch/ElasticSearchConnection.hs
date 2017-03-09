{-# LANGUAGE MultiParamTypeClasses, FlexibleInstances, TypeFamilies, StandaloneDeriving, DeriveGeneric #-}

module QueryArrow.ElasticSearch.ElasticSearchConnection where

-- http://swizec.com/blog/writing-a-rest-client-in-haskell/swizec/6152

import Prelude hiding (lookup)
import Data.Map.Strict (Map, fromList, empty, foldrWithKey, insert, lookup)
import Data.Text (unpack, Text)
import Data.Convertible
import Data.Scientific (toBoundedInteger, Scientific, base10Exponent, coefficient, scientific)
import Data.Aeson (Value (..))
import Control.Monad.IO.Class (liftIO, MonadIO)
import Control.Applicative ((<|>))
import GHC.Generics
import Data.MessagePack (MessagePack(..))

import QueryArrow.FO.Data
import QueryArrow.DB.DB
import QueryArrow.DB.GenericDatabase
import QueryArrow.DB.NoConnection
import QueryArrow.DB.ResultStream
import QueryArrow.Utils ()

import QueryArrow.ElasticSearch.Record
import qualified QueryArrow.ElasticSearch.Query as ESQ
import QueryArrow.ElasticSearch.QueryResult
import QueryArrow.ElasticSearch.QueryResultHits
import QueryArrow.ElasticSearch.ESQL

type ElasticSearchDB = ESQ.ElasticSearchConnInfo

instance INoConnectionDatabase2 (GenericDatabase ESTrans ElasticSearchDB) where
    type NoConnectionQueryType (GenericDatabase ESTrans ElasticSearchDB) = (ElasticSearchQuery, [Var])
    type NoConnectionRowType (GenericDatabase ESTrans ElasticSearchDB) = MapResultRow
    noConnectionDBStmtExec (GenericDatabase _ db _ _) (qu, vars) rs = do
      row <- rs
      execWithParams db qu row

execWithParams :: ElasticSearchDB -> ElasticSearchQuery -> MapResultRow -> DBResultStream MapResultRow
execWithParams esci (ElasticSearchQuery type0 rec) args = do
    hit <- esResultStream esci type0 rec args
    return (convertHitToMapResultRow type0 rec hit)

execWithParams esci (ElasticSearchInsert type0 rec) args =
    let esquery =
            ESRecord
                (fromList (foldrWithKey (\key val list -> extractInsertItem key val args : list) [] rec)) in
        resultStream2 (do
            _ <- ESQ.postESRecord esci type0 esquery
            return [mempty]
            ) (return ())

execWithParams esci (ElasticSearchDelete type0 rec) args = (do
    hit <- esResultStream esci type0 rec args
    _ <- liftIO $ ESQ.deleteById esci type0 (_id hit)
    emptyResultStream) <|> return mempty

execWithParams esci (ElasticSearchUpdateProperty type0 rec updaterec) args = (do
    hit <- esResultStream esci type0 rec args
    let rec = _source hit
        id0 = _id hit
        updatedrec = updateProps (recordToESRecord updaterec args) rec
    _ <- liftIO $ ESQ.updateESRecord esci type0 id0 updatedrec
    emptyResultStream
    ) <|> return mempty

execWithParams esci (ElasticSearchDeleteProperty type0 rec diff) args = (do
    hit <- esResultStream esci type0 rec args
    let rec = _source hit
        id0 = _id hit
        updatedrec = deleteProps diff rec
    _ <- liftIO $ ESQ.updateESRecord esci type0 id0 updatedrec
    emptyResultStream
    ) <|> return mempty


convertExprToString :: Expr -> String
convertExprToString (IntExpr i) = show i
convertExprToString (StringExpr s) = unpack s
convertExprToString _ = error ("unsupported param value expr type")


instance ResultValue Value where
    toConcreteResultValue (String s) = (StringValue s)
    toConcreteResultValue (Number n) = case toBoundedInteger n of
                                Just n1 -> (Int64Value n1)
                                Nothing -> error ("ConvertError Value to ResultValue" ++ show n)
    toConcreteResultValue a = error ("ConvertError: Value to ResultValue" ++ show a)
    castTypeOf (String s) = TextType
    castTypeOf (Number n) = Int64Type
    castTypeOf a = error ("ConvertError: Value to ResultValue" ++ show a)
    toNetworkResultValue a = toNetworkResultValue (toConcreteResultValue a)

deriving instance Generic Value
instance MessagePack Value
instance MessagePack Scientific where
  toObject a = toObject (fromIntegral (coefficient a) :: Int, base10Exponent a)
  fromObject a = do
    (baseobj, expobj) <- fromObject a
    base <- fromObject baseobj
    exp <- fromObject expobj
    return (scientific (fromIntegral (base :: Int)) exp)

convertHitToMapResultRow :: Text -> Map Text ElasticSearchQueryExpr -> ESHit -> MapResultRow
convertHitToMapResultRow _ map2 eshit =
    let (ESRecord map1) = _source eshit in
        foldrWithKey (\key val map3 -> case val of
            ElasticSearchQueryVar var ->
                case lookup key map1 of
                    Just val2 -> insert var (AbstractResultValue val2) map3
                    Nothing -> map3
            _ -> map3) empty map2

extractQueryItem :: Text -> ElasticSearchQueryExpr -> MapResultRow -> [ESQ.ESTermQuery]
extractQueryItem key val args = case val of
    ElasticSearchQueryParam i ->
        case lookup i args of
            Just expr ->
                case case expr of AbstractResultValue arv -> toConcreteResultValue arv of
                    StringValue s -> [ESQ.ESTermQuery (ESQ.ESStrTermQuery key s)]
                    Int64Value i -> [ESQ.ESTermQuery (ESQ.ESIntTermQuery key (fromIntegral i))]
                    _ -> error "unsupported expr type"
            Nothing -> error "no such argument"
    ElasticSearchQueryIntVal val ->
        [ESQ.ESTermQuery (ESQ.ESIntTermQuery key val)]
    ElasticSearchQueryStrVal val ->
        [ESQ.ESTermQuery (ESQ.ESStrTermQuery key val)]
    ElasticSearchQueryVar _ ->
        []

extractInsertItem :: Text -> ElasticSearchQueryExpr -> MapResultRow -> (Text, Value)
extractInsertItem key val args = case val of
    ElasticSearchQueryParam i ->
        case lookup i args of
            Just expr ->
                case case expr of AbstractResultValue arv -> toConcreteResultValue arv of
                    StringValue s -> (key, String s)
                    Int64Value i -> (key, Number (fromInteger (toInteger i)))
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

recordToQuery :: Map Text ElasticSearchQueryExpr -> MapResultRow -> Int -> Int -> ESQ.ESQuery
recordToQuery rec args off lim = ESQ.ESQuery off lim (
                        ESQ.ESBoolQuery (
                            ESQ.ESMustQuery (
                                foldrWithKey (\key val list -> extractQueryItem key val args ++ list) [] rec
                            )
                        )
                    )

recordToESRecord :: Map Text ElasticSearchQueryExpr -> MapResultRow -> ESRecord
recordToESRecord rec args =
    ESRecord (foldrWithKey (\key val list -> insert key (snd (extractInsertItem key val args)) list) empty rec)


esResultStream :: MonadIO m => ESQ.ElasticSearchConnInfo -> Text -> Map Text ElasticSearchQueryExpr -> MapResultRow -> ResultStream m ESHit
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
                    _ -> listResultStream rows <|> getrs (off + lim) lim
        getrs 0 page
