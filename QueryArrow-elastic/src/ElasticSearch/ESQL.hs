{-# LANGUAGE TypeFamilies #-}

module ElasticSearch.ESQL where

-- http://swizec.com/blog/writing-a-rest-client-in-haskell/swizec/6152

import Prelude hiding (lookup)
import Data.Map.Strict (lookup, fromList, Map)
import Data.Text (Text)
import Data.Set (toAscList, unions, singleton)
import Algebra.Lattice

import FO.Data
import DB.GenericDatabase
import DB.DB

import Debug.Trace

data ElasticSearchQueryExpr =
    ElasticSearchQueryParam Var
    | ElasticSearchQueryVar Var
    | ElasticSearchQueryIntVal Int
    | ElasticSearchQueryStrVal Text deriving (Show, Read)

data ElasticSearchQuery = ElasticSearchQuery Text (Map Text ElasticSearchQueryExpr)
                        | ElasticSearchInsert Text (Map Text ElasticSearchQueryExpr)
                        | ElasticSearchUpdateProperty Text (Map Text ElasticSearchQueryExpr) (Map Text ElasticSearchQueryExpr)
                        | ElasticSearchDelete Text (Map Text ElasticSearchQueryExpr)
                        | ElasticSearchDeleteProperty Text (Map Text ElasticSearchQueryExpr) [Text] deriving (Show, Read)

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

instance IGenericDatabase01 ESTrans where
    type GDBQueryType ESTrans = (ElasticSearchQuery, [Var])
    type GDBFormulaType ESTrans = Formula
    gTranslateQuery trans vars (FAtomic (Atom pred1 args)) env =
        return (translateQueryToElasticSearch trans (toAscList vars) pred1 args (toAscList env))
    gTranslateQuery trans vars (FInsert (Lit Pos (Atom pred1 args))) env =
        return (translateInsertToElasticSearch trans pred1 args (toAscList env))
    gTranslateQuery trans vars (FInsert (Lit Neg (Atom pred1 args))) env =
        return (translateDeleteToElasticSearch trans pred1 args (toAscList env))
    gTranslateQuery _ _ _ _ =
        error "unsupported"

    gSupported _ (FAtomic _) _ = True
    gSupported _ (FInsert _) _ = True
    gSupported _ _ _ = False
    gDeterminateVars _ vars (Atom _ args) =
        (unions (map (\arg -> case arg of
                (VarExpr v) -> singleton v
                _ -> bottom) args)) \/ vars
