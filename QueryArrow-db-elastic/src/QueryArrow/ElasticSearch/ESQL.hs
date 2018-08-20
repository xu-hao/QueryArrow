{-# LANGUAGE TypeFamilies #-}

module QueryArrow.ElasticSearch.ESQL where

-- http://swizec.com/blog/writing-a-rest-client-in-haskell/swizec/6152

import Prelude hiding (lookup)
import Data.Map.Strict (lookup, fromList, Map, keys)
import Data.Text (Text)
import Data.Set (toAscList)

import QueryArrow.Syntax.Term
import QueryArrow.Semantics.TypeChecker
import QueryArrow.Syntax.Type
import QueryArrow.DB.GenericDatabase

import Debug.Trace

data ElasticSearchQueryExpr =
    ElasticSearchQueryParam Var
    | ElasticSearchQueryVar Var
    | ElasticSearchQueryIntVal Integer
    | ElasticSearchQueryStrVal Text deriving (Show, Read)

data ElasticSearchQuery = ElasticSearchQuery Text (Map Text ElasticSearchQueryExpr)
                        | ElasticSearchInsert Text (Map Text ElasticSearchQueryExpr)
                        | ElasticSearchUpdateProperty Text (Map Text ElasticSearchQueryExpr) (Map Text ElasticSearchQueryExpr)
                        | ElasticSearchDelete Text (Map Text ElasticSearchQueryExpr)
                        | ElasticSearchDeleteProperty Text (Map Text ElasticSearchQueryExpr) [Text] deriving (Show, Read)

data ESTrans = ESTrans PredTypeMap (Map PredName (Text, [Text]))

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


translateQueryToElasticSearch :: ESTrans -> [Var] -> PredName -> [Expr] -> [Var] -> (ElasticSearchQuery, [Var])
translateQueryToElasticSearch (ESTrans ptm map1) _ pred0 args env =
    let (args2, params) = mconcat (map (translateQueryArg env) args)
        (type0, props) = case lookup pred0 map1 of
                Just props0 -> props0
                Nothing -> error "cannot find predicate" in
        (ElasticSearchQuery type0 (fromList (zip props args2)), params)

translateInsertToElasticSearch :: ESTrans -> PredName -> [Expr] -> [Var] -> (ElasticSearchQuery, [Var])
translateInsertToElasticSearch (ESTrans ptm map1) pred0 args env =
  case lookup pred0 ptm of
    Nothing -> error ("translateInsertToElasticSearch: cannot find predicate " ++ show pred0)
    Just (PredType ObjectPred _) ->
        let (args2, params) = mconcat (map (translateQueryArg env) args)
            (type0, props) = case lookup pred0 map1 of
                    Just props0 -> props0
                    Nothing -> error "cannot find predicate" in
            (ElasticSearchInsert type0 (fromList (zip props args2)), params)
    Just pt@(PredType PropertyPred _) ->
        let (args2, params) = mconcat (map (translateQueryArg env) args)
            keyargs = keyComponents pt args2
            propargs = propComponents pt args2
            (type0, props) = case lookup pred0 map1 of
                    Just props0 -> props0
                    Nothing -> error "cannot find predicate"
            keyprops = keyComponents pt props
            propprops = propComponents pt props in
            (ElasticSearchUpdateProperty type0 (fromList (zip keyprops keyargs)) (fromList (zip propprops propargs)), params)


translateDeleteToElasticSearch :: ESTrans -> PredName -> [Expr] -> [Var] -> (ElasticSearchQuery, [Var])
translateDeleteToElasticSearch (ESTrans ptm map1) pred0 args env =
  case lookup pred0 ptm of
    Nothing -> error ("translateInsertToElasticSearch: cannot find predicate " ++ show pred0)
    Just (PredType ObjectPred _) ->
        let (args2, params) = mconcat (map (translateQueryArg env) args)
            (type0, props) = case lookup pred0 map1 of
                    Just props0 -> props0
                    Nothing -> error "cannot find predicate" in
            (ElasticSearchDelete type0 (fromList (zip props args2)), params)
    Just pt@(PredType PropertyPred _) ->
        let (args2, params) = mconcat (map (translateQueryArg env) args)
            keyargs = keyComponents pt args2
            propargs = propComponents pt args2
            (type0, props) = case lookup pred0 map1 of
                    Just props0 -> props0
                    Nothing -> error "cannot find predicate"
            keyprops = keyComponents pt props
            propprops = propComponents pt props in
            (ElasticSearchDeleteProperty type0 (fromList (zip keyprops keyargs)) propprops, params)

instance IGenericDatabase01 ESTrans where
    type GDBQueryType ESTrans = (ElasticSearchQuery, [Var])
    type GDBFormulaType ESTrans = FormulaT
    gTranslateQuery trans vars (FAtomicA _ (Atom pred1 args)) env =
        return (translateQueryToElasticSearch trans (toAscList vars) pred1 args (toAscList env))
    gTranslateQuery trans vars (FInsertA _ (Lit Pos (Atom pred1 args))) env =
        return (translateInsertToElasticSearch trans pred1 args (toAscList env))
    gTranslateQuery trans vars (FInsertA _ (Lit Neg (Atom pred1 args))) env =
        return (translateDeleteToElasticSearch trans pred1 args (toAscList env))
    gTranslateQuery _ _ _ _ =
        error "unsupported"

    gSupported _ _ (FAtomicA _ _) _ = True
    gSupported _ _ (FInsertA _ _) _ = True
    gSupported _ _ _ _ = False
