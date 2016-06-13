{-# LANGUAGE DeriveGeneric, MultiParamTypeClasses, FlexibleInstances, FlexibleContexts #-}

module ElasticSearch.ESQL where

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

import Debug.Trace

data ElasticSearchQueryExpr =
    ElasticSearchQueryParam Var
    | ElasticSearchQueryVar Var
    | ElasticSearchQueryIntVal Int
    | ElasticSearchQueryStrVal Text deriving Show

data ElasticSearchQuery = ElasticSearchQuery Text (Map Text ElasticSearchQueryExpr)
                        | ElasticSearchInsert Text (Map Text ElasticSearchQueryExpr)
                        | ElasticSearchUpdateProperty Text (Map Text ElasticSearchQueryExpr) (Map Text ElasticSearchQueryExpr)
                        | ElasticSearchDelete Text (Map Text ElasticSearchQueryExpr)
                        | ElasticSearchDeleteProperty Text (Map Text ElasticSearchQueryExpr) [Text] deriving Show

data ESTrans = ESTrans (Map Pred (Text, [Text]))

translateQueryArg :: [Var] -> Expr -> ([ElasticSearchQueryExpr], [Var])
translateQueryArg env (VarExpr var)  =
    if elem var env
        then ([ElasticSearchQueryParam var], [var])
        else ([ElasticSearchQueryVar var], [var])
translateQueryArg _ (IntExpr i) =
    ([ElasticSearchQueryIntVal i], [])
translateQueryArg _ (StringExpr i)  =
    ([ElasticSearchQueryStrVal i], [])
translateQueryArg _ _  =
    error "unsupported"


translateQueryToElasticSearch :: ESTrans -> [Var] -> Pred -> [Expr] -> [Var] -> (ElasticSearchQuery, [Var])
translateQueryToElasticSearch (ESTrans map1) _ pred0 args env =
    let (args2, params) = mconcat (map (translateQueryArg env) args)
        (type0, props) = case lookup pred0 map1 of
                Just props0 -> props0
                Nothing -> error "cannot find predicate" in
        (ElasticSearchQuery type0 (fromList (zip props args2)), params)

translateInsertToElasticSearch :: ESTrans -> Pred -> [Expr] -> [Var] -> (ElasticSearchQuery, [Var])
translateInsertToElasticSearch (ESTrans map1) pred0@(Pred _ (PredType ObjectPred _) ) args env =
    let (args2, params) = mconcat (map (translateQueryArg env) args)
        (type0, props) = case lookup pred0 map1 of
                Just props0 -> props0
                Nothing -> error "cannot find predicate" in
        (ElasticSearchInsert type0 (fromList (zip props args2)), params)
translateInsertToElasticSearch (ESTrans map1) pred0@(Pred _ (PredType PropertyPred _) ) args env =
    let (args2, params) = mconcat (map (translateQueryArg env) args)
        keyargs = keyComponents pred0 args2
        propargs = propComponents pred0 args2
        (type0, props) = case lookup pred0 map1 of
                Just props0 -> props0
                Nothing -> error "cannot find predicate"
        keyprops = keyComponents pred0 props
        propprops = propComponents pred0 props in
        (ElasticSearchUpdateProperty type0 (fromList (zip keyprops keyargs)) (fromList (zip propprops propargs)), params)


translateDeleteToElasticSearch :: ESTrans -> Pred -> [Expr] -> [Var] -> (ElasticSearchQuery, [Var])
translateDeleteToElasticSearch (ESTrans map1) pred0@(Pred _ (PredType ObjectPred _) ) args env =
    let (args2, params) = mconcat (map (translateQueryArg env) args)
        (type0, props) = case lookup pred0 map1 of
                Just props0 -> props0
                Nothing -> error "cannot find predicate" in
        (ElasticSearchDelete type0 (fromList (zip props args2)), params)
translateDeleteToElasticSearch (ESTrans map1) pred0@(Pred _ (PredType PropertyPred _) ) args env =
    let (args2, params) = mconcat (map (translateQueryArg env) args)
        keyargs = keyComponents pred0 args2
        propargs = propComponents pred0 args2
        (type0, props) = case lookup pred0 map1 of
                Just props0 -> props0
                Nothing -> error "cannot find predicate"
        keyprops = keyComponents pred0 props
        propprops = propComponents pred0 props in
        (ElasticSearchDeleteProperty type0 (fromList (zip keyprops keyargs)) propprops, params)

instance Translate ESTrans MapResultRow ElasticSearchQuery where
    translateQueryWithParams trans vars (Query  (FAtomic (Atom pred1 args))) env =
        translateQueryToElasticSearch trans vars pred1 args env
    translateQueryWithParams trans vars (Query  (FInsert (Lit Pos (Atom pred1 args)))) env =
        translateInsertToElasticSearch trans pred1 args env
    translateQueryWithParams trans vars (Query  (FInsert (Lit Neg (Atom pred1 args)))) env =
        translateDeleteToElasticSearch trans pred1 args env
    translateQueryWithParams _ _ _ _ =
        error "unsupported"

    translateable _ (FAtomic _) _ = True
    translateable _ (FInsert _) _ = True
    translateable _ _ _ = False
