{-# LANGUAGE MultiParamTypeClasses, FlexibleInstances, TypeFamilies, StandaloneDeriving, DeriveGeneric #-}

module QueryArrow.ElasticSearch.ElasticSearchConnection where

-- http://swizec.com/blog/writing-a-rest-client-in-haskell/swizec/6152

import Prelude hiding (lookup)
import Data.Map.Strict (Map, fromList, empty, foldrWithKey, insert, lookup, toList)
import Data.Text (unpack, Text)
import Data.Convertible
import Data.Scientific (toBoundedInteger, Scientific, base10Exponent, coefficient, scientific)
import Data.Aeson (Value (..))
import Control.Monad.IO.Class (liftIO, MonadIO)
import Control.Applicative ((<|>))
import GHC.Generics
import Data.MessagePack (MessagePack(..))

import QueryArrow.Syntax.Data
import QueryArrow.DB.DB
import QueryArrow.DB.GenericDatabase
import QueryArrow.DB.NoConnection
import QueryArrow.Semantics.ResultStream
import QueryArrow.Semantics.ResultRow.VectorResultRow
import QueryArrow.Semantics.ResultValue.AbstractResultValue
import QueryArrow.Semantics.ResultValue
import QueryArrow.Semantics.ResultSet
import QueryArrow.Semantics.ResultHeader.VectorResultHeader
import QueryArrow.Semantics.ResultSet.ResultStreamResultSet
import QueryArrow.Semantics.ResultSet.ResultSetResultStreamResultSet
import QueryArrow.Semantics.ResultSet.VectorResultSetTransformer
import QueryArrow.Semantics.ResultHeader
import QueryArrow.Semantics.Sendable
import QueryArrow.DB.ParametrizedStatement
import QueryArrow.Utils ()
import qualified Data.Vector as V

import QueryArrow.ElasticSearch.Record
import qualified QueryArrow.ElasticSearch.Query as ESQ
import QueryArrow.ElasticSearch.QueryResult
import QueryArrow.ElasticSearch.QueryResultHits
import QueryArrow.ElasticSearch.ESQL
import QueryArrow.Data.Some
import Data.Conduit
import Data.Conduit.List (sourceList)
import Data.Binary

type ElasticSearchDB = ESQ.ElasticSearchConnInfo

instance IPSDBStatement (ElasticSearchDB, ElasticSearchQuery, ResultHeader) where
    -- execWithParams :: (ElasticSearchDB, ElasticSearchQuery, [Var]) -> VectorResultRow AbstractResultValue -> DBResultStream (VectorResultRow AbstractResultValue)
    type ParameterType (ElasticSearchDB, ElasticSearchQuery, ResultHeader) = (ResultHeader, VectorResultRow AbstractResultValue)
    type PSResultSetType (ElasticSearchDB, ElasticSearchQuery, ResultHeader) = ResultStreamResultSet (ResultSetTransformer AbstractResultValue) (VectorResultRow AbstractResultValue)
    execWithParams (esci, ElasticSearchQuery type0 rec, hdr) (hdr2, args) =
        return (ResultStreamResultSet RSId hdr (ResultStream (runResultStream (esResultStream esci type0 rec hdr2 args) =$= awaitForever (\hit ->
            yield (convertHitToMapResultRow type0 rec hit)))))

    execWithParams (esci, ElasticSearchInsert type0 rec, hdr) (hdr2, args) =
        let esquery =
                ESRecord
                    (fromList (foldrWithKey (\key val list -> extractInsertItem key val hdr2 args : list) [] rec)) in
            return (ResultStreamResultSet RSId hdr (ResultStream (do
              _ <- liftIO $ ESQ.postESRecord esci type0 esquery
              yield V.empty)))

    execWithParams (esci, ElasticSearchDelete type0 rec, hdr) (hdr2, args) =
      return (ResultStreamResultSet RSId hdr (ResultStream (runResultStream (esResultStream esci type0 rec hdr2 args) =$= do
        awaitForever (\hit -> do
            _ <- liftIO $ ESQ.deleteById esci type0 (_id hit)
            return ())
        yield V.empty)))

    execWithParams (esci, ElasticSearchUpdateProperty type0 rec updaterec, hdr) (hdr2, args) =
      return (ResultStreamResultSet RSId hdr (ResultStream (runResultStream (esResultStream esci type0 rec hdr2 args) =$= do
        awaitForever (\hit -> do
            let rec = _source hit
                id0 = _id hit
                updatedrec = updateProps (recordToESRecord updaterec hdr2 args) rec
            _ <- liftIO $ ESQ.updateESRecord esci type0 id0 updatedrec
            return ())
        yield V.empty)))

    execWithParams (esci, ElasticSearchDeleteProperty type0 rec diff, hdr) (hdr2, args) =
      return (ResultStreamResultSet RSId hdr (ResultStream (runResultStream (esResultStream esci type0 rec hdr2 args) =$= do
        awaitForever (\hit -> do
            let rec = _source hit
                id0 = _id hit
                updatedrec = deleteProps diff rec
            _ <- liftIO $ ESQ.updateESRecord esci type0 id0 updatedrec
            return ())
        yield V.empty)))

instance Convertible (ResultHeader, VectorResultRow AbstractResultValue) (ResultHeader, VectorResultRow AbstractResultValue) where
  safeConvert = return

instance INoConnectionDatabase2 (GenericDatabase ESTrans ElasticSearchDB) where
    type NoConnectionQueryType (GenericDatabase ESTrans ElasticSearchDB) = (ElasticSearchQuery, [Var])
    type NoConnectionInputRowType (GenericDatabase ESTrans ElasticSearchDB) = VectorResultRow AbstractResultValue
    type NoConnectionResultSetType (GenericDatabase ESTrans ElasticSearchDB) = ResultSetResultStreamResultSet (ResultSetTransformer AbstractResultValue) (ResultStreamResultSet (ResultSetTransformer AbstractResultValue) (VectorResultRow AbstractResultValue))
    noConnectionDBStmtExec (GenericDatabase _ db _ _) (qu, vars) rs =
      dbStmtExec (PSDBStatement (db, qu, toHeader vars :: ResultHeader)) rs

convertExprToString :: Expr -> String
convertExprToString (IntExpr i) = show i
convertExprToString (StringExpr s) = unpack s
convertExprToString _ = error ("unsupported param value expr type")

instance Binary Value where
  put v = put (toConcreteResultValue v)
  get = do
    concretev <- get
    return  (fromConcreteResultValue concretev)

instance Sendable Value where
  send h v = send h (toConcreteResultValue v)

instance ResultValue Value where
    toConcreteResultValue (String s) = StringValue s
    toConcreteResultValue (Number n) = case toBoundedInteger n of
                                Just n1 -> (Int64Value n1)
                                Nothing -> error ("ConvertError Value to ResultValue" ++ show n)
    toConcreteResultValue a = error ("ConvertError: Value to ResultValue" ++ show a)
    fromConcreteResultValue (StringValue s) = String s
    fromConcreteResultValue (Int64Value n) = Number (fromIntegral n)
    fromConcreteResultValue a = error ("ConvertError: ResultValue to Value" ++ show a)
    castTypeOf (String s) = TextType
    castTypeOf (Number n) = Int64Type
    castTypeOf a = error ("ConvertError: Value to ResultValue" ++ show a)

deriving instance Generic Value
instance MessagePack Value
instance MessagePack Scientific where
  toObject a = toObject (fromIntegral (coefficient a) :: Int, base10Exponent a)
  fromObject a = do
    (baseobj, expobj) <- fromObject a
    base <- fromObject baseobj
    exp <- fromObject expobj
    return (scientific (fromIntegral (base :: Int)) exp)

argsToVars :: Map Text ElasticSearchQueryExpr -> [(Text, Var)]
argsToVars map2 =
        concatMap (\(key, val) -> case val of
            ElasticSearchQueryVar var -> [(key, var)]
            _ -> []) (toList map2)


convertHitToMapResultRow :: Text -> Map Text ElasticSearchQueryExpr -> ESHit -> VectorResultRow AbstractResultValue
convertHitToMapResultRow _ map2 eshit =
    let (ESRecord map1) = _source eshit
        cols = V.fromList (argsToVars map2) in
        V.map (\(key, var0) ->
            case lookup key map1 of
                Nothing -> error ("convertHitToMapResultRow: cannot find key " ++ show key ++ " in " ++ show map1)
                Just val -> Some val) cols

extractQueryItem :: Text -> ElasticSearchQueryExpr -> ResultHeader -> VectorResultRow AbstractResultValue -> [ESQ.ESTermQuery]
extractQueryItem key val hdr args = case val of
    ElasticSearchQueryParam i ->
        case ext i hdr args of
            Just expr ->
                case case expr of Some arv -> toConcreteResultValue arv of
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

extractInsertItem :: Text -> ElasticSearchQueryExpr -> ResultHeader -> VectorResultRow AbstractResultValue -> (Text, Value)
extractInsertItem key val hdr args = case val of
    ElasticSearchQueryParam i ->
        case ext i hdr args of
            Just expr ->
                case case expr of Some arv -> toConcreteResultValue arv of
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

recordToQuery :: Map Text ElasticSearchQueryExpr -> ResultHeader -> VectorResultRow AbstractResultValue -> Int -> Int -> ESQ.ESQuery
recordToQuery rec hdr args off lim = ESQ.ESQuery off lim (
                        ESQ.ESBoolQuery (
                            ESQ.ESMustQuery (
                                foldrWithKey (\key val list -> extractQueryItem key val hdr args ++ list) [] rec
                            )
                        )
                    )

recordToESRecord :: Map Text ElasticSearchQueryExpr -> ResultHeader -> VectorResultRow AbstractResultValue -> ESRecord
recordToESRecord rec hdr args =
    ESRecord (foldrWithKey (\key val list -> insert key (snd (extractInsertItem key val hdr args)) list) empty rec)


esResultStream :: MonadIO m => ESQ.ElasticSearchConnInfo -> Text -> Map Text ElasticSearchQueryExpr -> ResultHeader -> VectorResultRow AbstractResultValue -> ResultStream m ESHit
esResultStream esci type0 rec hdr args =
        let getrs off lim = ResultStream (do
                let esquery = recordToQuery rec hdr args off lim
                res <- liftIO $ ESQ.queryBySearch esci type0 esquery
                case res of
                    Left res1 -> error ("execWithParams: cannot decode response " ++ res1)
                    Right (ESQueryResult _ _ _ (ESQueryResultHits _ _ hits1)) ->
                        case hits1 of
                            [] -> return ()
                            _ -> do
                              sourceList hits1
                              runResultStream (getrs (off + lim) lim)) in
            getrs 0 page
