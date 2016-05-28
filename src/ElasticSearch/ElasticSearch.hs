{-# LANGUAGE DeriveGeneric, MultiParamTypeClasses, FlexibleInstances, FlexibleContexts #-}

module ElasticSearch.ElasticSearch where

-- http://swizec.com/blog/writing-a-rest-client-in-haskell/swizec/6152

import Prelude hiding (lookup)
import qualified Data.ByteString.Lazy.Char8 as BL8
import Data.Map.Strict hiding (map, elemAt)
import Data.Text (unpack, pack)
import Data.Char (toLower)

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

data ElasticSearchQueryExpr =
    ElasticSearchQueryParam Var
    | ElasticSearchQueryVar Var
    | ElasticSearchQueryIntVal Int
    | ElasticSearchQueryStrVal String deriving Show

data ElasticSearchQuery = ElasticSearchQuery {
    esquery_obj_id :: ElasticSearchQueryExpr ,
    esquery_meta_id :: ElasticSearchQueryExpr,
    esquery_attribute :: ElasticSearchQueryExpr,
    esquery_value :: ElasticSearchQueryExpr,
    esquery_unit :: ElasticSearchQueryExpr
} | ElasticSearchInsert {
    esinsert_obj_id :: ElasticSearchQueryExpr,
    esinsert_meta_id :: ElasticSearchQueryExpr,
    esinsert_attribute :: ElasticSearchQueryExpr,
    esinsert_value :: ElasticSearchQueryExpr,
    esinsert_unit :: ElasticSearchQueryExpr
} | ElasticSearchDelete {
    esdelete_obj_id :: ElasticSearchQueryExpr,
    esdelete_meta_id :: ElasticSearchQueryExpr,
    esdelete_attribute :: ElasticSearchQueryExpr,
    esdelete_value :: ElasticSearchQueryExpr,
    esdelete_unit :: ElasticSearchQueryExpr
} deriving Show

type ElasticSearchConnection = ESQ.ElasticSearchConnInfo
data ElasticSearchStatement = ElasticSearchStatement ElasticSearchConnection ElasticSearchQuery

type ElasticSearchParam = String

convertExprToString :: Expr -> String
convertExprToString (IntExpr i) = show i
convertExprToString (StringExpr s) = unpack s
convertExprToString _ = error ("unsupported param value expr type")

extractMappingFromElasticSearchQueryExpr :: ElasticSearchQueryExpr -> ResultValue -> [(Var, ResultValue)]
extractMappingFromElasticSearchQueryExpr esqe resval =
    case esqe of
        ElasticSearchQueryVar var -> [(var, resval)]
        _ -> []


convertHitToMapResultRow :: ElasticSearchQuery -> ESHit -> MapResultRow
convertHitToMapResultRow esquery eshit =
        fromList (concat [
            extractMappingFromElasticSearchQueryExpr (esquery_obj_id esquery) (IntValue (obj_id (_source eshit))),
            extractMappingFromElasticSearchQueryExpr (esquery_meta_id esquery) (IntValue (meta_id (_source eshit))),
            extractMappingFromElasticSearchQueryExpr (esquery_attribute esquery) (StringValue (pack (attribute  (_source eshit)))),
            extractMappingFromElasticSearchQueryExpr (esquery_value esquery) (StringValue (pack (value  (_source eshit)))),
            extractMappingFromElasticSearchQueryExpr (esquery_unit esquery) (StringValue (pack (unit  (_source eshit))))
        ])


class Cast a b where
    cast :: a -> b

instance Cast a a where
    cast = id

instance Cast Int String where
    cast _ = error "cannot cast Int to String"

instance Cast String Int where
    cast _ = error "cannot cast String to Int"

instance Cast Expr String where
    cast (StringExpr s ) = unpack s
    cast e = error ("cannot cast " ++ show e ++ " to String")

instance Cast Expr Int where
    cast (IntExpr s) = s
    cast e = error ("cannot cast " ++ show e ++ " to Int")

extractQueryItem :: (Cast Int a, Cast String a, Cast Expr a) => ElasticSearchQueryExpr -> (a -> ESQ.ESTermQueryItem) -> Map Var Expr -> [ESQ.ESTermQuery]
extractQueryItem e c args = case e of
    ElasticSearchQueryParam i ->
        case lookup i args of
            Just expr -> [ESQ.ESTermQuery (c (cast expr))]
            Nothing -> error "no such argument"
    ElasticSearchQueryIntVal val ->
        [ESQ.ESTermQuery (c (cast val))]
    ElasticSearchQueryStrVal val ->
        [ESQ.ESTermQuery (c (cast val))]
    ElasticSearchQueryVar _ ->
        []

extractInsertItem :: (Cast Int a, Cast String a, Cast Expr a) => ElasticSearchQueryExpr -> Map Var Expr -> a
extractInsertItem e args = case e of
    ElasticSearchQueryParam i ->
        case lookup i args of
            Just expr -> cast expr
            Nothing -> error "no such argument"
    ElasticSearchQueryIntVal val ->
        cast val
    ElasticSearchQueryStrVal val ->
        cast val
    ElasticSearchQueryVar _ ->
        error "var"

page :: Int
page = 100
instance PreparedStatement_ ElasticSearchStatement where
        execWithParams (ElasticSearchStatement esci qu@(ElasticSearchQuery o m a v u)) args =
            let esquery =
                    ESQ.ESQuery 0 page (
                        ESQ.ESBoolQuery (
                            ESQ.ESMustQuery (
                                concat [
                                    extractQueryItem o (ESQ.ESIntTermQuery "obj_id") args,
                                    extractQueryItem m (ESQ.ESIntTermQuery "meta_id") args,
                                    extractQueryItem a (ESQ.ESStrTermQuery "attribute") args,
                                    extractQueryItem v (ESQ.ESStrTermQuery "value") args,
                                    extractQueryItem u (ESQ.ESStrTermQuery "unit") args
                                ]
                            )
                        )
                    ) in
                resultStream2 (do
                    res <- ESQ.queryBySearch esci esquery
                    case res of
                        Left res1 -> error ("execWithParams: cannot decode response " ++ res1)
                        Right (ESQueryResult _ _ _ (ESQueryResultHits _ _ hits1)) -> return (map (convertHitToMapResultRow qu) hits1)) (return ())

        execWithParams (ElasticSearchStatement esci (ElasticSearchInsert o m a v u)) args =
            let esquery =
                    ESRecord
                        (extractInsertItem o args)
                        (extractInsertItem m args)
                        (extractInsertItem a args)
                        (extractInsertItem v args)
                        (extractInsertItem u args) in
                resultStream2 (do
                    _ <- ESQ.postESRecord esci esquery
                    return [mempty]
                    ) (return ())

        execWithParams (ElasticSearchStatement esci (ElasticSearchDelete o m a v u)) args =
            let esquery =
                    ESQ.ESQuery 0 page (
                        ESQ.ESBoolQuery (
                            ESQ.ESMustQuery (
                                concat [
                                    extractQueryItem o (ESQ.ESIntTermQuery "obj_id") args,
                                    extractQueryItem m (ESQ.ESIntTermQuery "meta_id") args,
                                    extractQueryItem a (ESQ.ESStrTermQuery "attribute") args,
                                    extractQueryItem v (ESQ.ESStrTermQuery "value") args,
                                    extractQueryItem u (ESQ.ESStrTermQuery "unit") args
                                ]
                            )
                        )
                    ) in
                resultStream2 (do
                    res <- ESQ.queryBySearch esci esquery
                    case res of
                        Left res1 -> error ("execWithParams: cannot decode response " ++ res1)
                        Right (ESQueryResult _ _ _ (ESQueryResultHits _ _ hits1)) -> do
                            mapM_ (\hit -> ESQ.deleteById esci (_id hit)) hits1
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


data ESTrans = ESTrans

instance DBConnection conn ElasticSearchQuery  => ExtractDomainSize DBAdapterMonad conn ESTrans where
    extractDomainSize _ _ _ (Atom _ args) =
        return (fromList [(fv, Bounded 1) | fv <- freeVars args])

translateQueryArg :: [Var] -> Expr -> ([ElasticSearchQueryExpr], [Var])
translateQueryArg env (VarExpr var)  =
    if elem var env
        then ([ElasticSearchQueryParam var], [var])
        else ([ElasticSearchQueryVar var], [var])
translateQueryArg _ (IntExpr i) =
    ([ElasticSearchQueryIntVal i], [])
translateQueryArg _ (StringExpr i)  =
    ([ElasticSearchQueryStrVal (unpack i)], [])
translateQueryArg _ _  =
    error "unsupported"



translateQueryToElasticSearch :: Pred -> [Expr] -> [Var] -> (ElasticSearchQuery, [Var])
translateQueryToElasticSearch _ args env =
    let (args2, params) = mconcat (map (translateQueryArg env) args) in
    (ElasticSearchQuery (args2 !! 0) (args2 !! 1) (args2 !! 2) (args2 !! 3) (args2 !! 4), params)

translateInsertToElasticSearch :: Pred -> [Expr] -> [Var] -> (ElasticSearchQuery, [Var])
translateInsertToElasticSearch _ args env =
    let (args2, params) = mconcat (map (translateQueryArg env) args) in
    (ElasticSearchInsert (args2 !! 0) (args2 !! 1) (args2 !! 2) (args2 !! 3) (args2 !! 4), params)

translateDeleteToElasticSearch :: Pred -> [Expr] -> [Var] -> (ElasticSearchQuery, [Var])
translateDeleteToElasticSearch _ args env =
    let (args2, params) = mconcat (map (translateQueryArg env) args) in
    (ElasticSearchDelete (args2 !! 0) (args2 !! 1) (args2 !! 2) (args2 !! 3) (args2 !! 4), params)

instance Translate ESTrans MapResultRow ElasticSearchQuery where
    translateQueryWithParams _ (Query _ (FAtomic (Atom pred1 args))) env =
        translateQueryToElasticSearch pred1 args env
    translateQueryWithParams _ (Query _ (FInsert (Lit Pos (Atom pred1 args)))) env =
        translateInsertToElasticSearch pred1 args env
    translateQueryWithParams _ (Query _ (FInsert (Lit Neg (Atom pred1 args)))) env =
        translateDeleteToElasticSearch pred1 args env
    translateQueryWithParams _ _ _ =
        error "unsupported"

    translateable _ (FAtomic _) _ = True
    translateable _ (FInsert _) _ = True
    translateable _ _ _ = False

    translateable' _ (Atomic _) _ = True
    translateable' _ _ _ = False

makeElasticSearchDBAdapter :: String -> ESQ.ElasticSearchConnInfo -> GenericDB   ElasticSearchConnection ESTrans
makeElasticSearchDBAdapter ns conn = GenericDB conn "ElasticSearch" [Pred (QPredName ns "ES_META") (PredType ObjectPred [Key "Int", Key "Int", Property "String", Property "String", Property "String"])] ESTrans

getDB :: ICATDBConnInfo -> IO [Database DBAdapterMonad MapResultRow]
getDB ps = do
    let conn = ESQ.ElasticSearchConnInfo (db_host ps) (db_port ps) (map toLower (db_path ps !! 0)) (db_path ps !! 1)
    let db = makeElasticSearchDBAdapter (db_name ps !! 0) conn
    return [Database db]
