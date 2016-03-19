{-# LANGUAGE TypeFamilies, MultiParamTypeClasses, FunctionalDependencies, ExistentialQuantification, FlexibleInstances, OverloadedStrings,
   RankNTypes, FlexibleContexts, GADTs, DeriveGeneric #-}
module FO where

import ResultStream
import FO.Data
import FO.Domain
import FO.Parser
import QueryPlan
import Rewriting
import Config
import Parser
import DBQuery
import Utils
-- import Plugins
import FO

import Prelude  hiding (lookup)
import Data.Map.Strict (Map, (!), empty, member, insert, foldrWithKey, foldlWithKey, alter, lookup, fromList, toList, unionWith, unionsWith, intersectionWith, elems, delete, singleton, keys, filterWithKey)
import qualified Data.Map.Strict
import Data.List ((\\), intercalate, union)
import Control.Monad.Trans.State.Strict (StateT, evalStateT,evalState, get, put, State, runState   )
import Control.Applicative ((<$>), liftA2)
import Data.Convertible.Base
import Control.Monad.Logic
import Control.Monad.Except
import Data.Maybe
import qualified Data.ByteString.Lazy as B
import qualified Data.ByteString.Lazy.Char8 as B8
import Text.ParserCombinators.Parsec hiding (State)
import Debug.Trace




-- example MapDB


filterResults :: (Functor m, Monad m) => [Var] -> (Pred-> ResultValue -> ResultValue -> MapResultRow -> ResultStream m MapResultRow)
    -> (Pred-> Var -> ResultValue -> MapResultRow -> ResultStream m MapResultRow)
    -> (Pred-> ResultValue -> Var -> MapResultRow -> ResultStream m MapResultRow)
    -> (Pred-> Var -> Var -> MapResultRow -> ResultStream m MapResultRow)
    -> ([Var]-> Var -> Formula -> MapResultRow -> ResultStream m MapResultRow)
    -> (Pred-> ResultValue -> ResultValue -> MapResultRow -> ResultStream m MapResultRow)
    -> ([Var]-> Var -> Formula -> MapResultRow -> ResultStream m MapResultRow)
    -> ResultStream m MapResultRow
    -> Formula
    -> ResultStream m MapResultRow
filterResults freevars filterBy expand1 expand2 expand12 exists excludeBy notExists results (Conjunction formulas) =
    foldl (filterResults freevars filterBy expand1 expand2 expand12 exists excludeBy notExists)  results formulas

filterResults freevars filterBy expand1 expand2 expand12 exists excludeBy notExists results (Disjunction formulas) =
    join ( listResultStream ( map (filterResults freevars filterBy expand1 expand2 expand12 exists excludeBy notExists results) formulas))

filterResults _ filterBy expand1 expand2 expand12 _ _ _ results (Atomic (Atom thepred args)) = do
    resrow <- results
    case args of
        VarExpr var1 : (VarExpr var2 : _)
            | var1 `member` resrow ->
                if var2 `member` resrow then
                    filterBy thepred (resrow ! var1) (resrow ! var2) resrow else
                    expand2 thepred (resrow ! var1) var2 resrow
            | var2 `member` resrow -> expand1 thepred var1 (resrow ! var2) resrow
            | otherwise -> expand12 thepred var1 var2 resrow
        VarExpr var1 : StringExpr str2 : _ ->
            if var1 `member` resrow
                then filterBy thepred (resrow ! var1) (StringValue str2) resrow
                else expand1 thepred var1 (StringValue str2) resrow
        StringExpr str1 : VarExpr var2 : _ ->
            if var2 `member` resrow
                then filterBy thepred (StringValue str1) (resrow ! var2) resrow
                else expand2 thepred (StringValue str1) var2 resrow
        StringExpr str1 : StringExpr str2 : _ ->
            filterBy thepred (StringValue str1) (StringValue str2) resrow
        _ -> listResultStream []

filterResults _ _ _ _ _ _ excludeBy _ results (Not (Atomic (Atom thepred args))) = do
    resrow <- results
    case args of
        VarExpr var1 : VarExpr var2 : _ ->
            if var1 `member` resrow && var2 `member` resrow
                then excludeBy thepred (resrow ! var1) (resrow ! var2) resrow
                else error ("unconstrained variable " ++ show var1 ++ " or " ++ show var2)
        VarExpr var1 : StringExpr str2 : _ ->
            if var1 `member` resrow
                then excludeBy thepred (resrow ! var1) (StringValue str2) resrow
                else error ("unconstrained variable " ++ show var1 ++ " in " ++ show (Lit Neg (Atom thepred args)))
        StringExpr str1 : VarExpr var2 : _ ->
            if var2 `member` resrow
                then excludeBy thepred (StringValue str1) (resrow ! var2) resrow
                else error ("unconstrained variable " ++ show var2 ++ " in " ++ show (Lit Neg (Atom thepred args)))
        StringExpr str1 : StringExpr str2 : _ ->
            excludeBy thepred (StringValue str1) (StringValue str2) resrow
        _ -> listResultStream [resrow]

filterResults freevars2 _ _ _ _ exists _ _ results (Exists var conj) =  do
    row <- results
    exists (freevars2 `union` (freeVars conj \\ [var])) var conj row

filterResults freevars2 _ _ _ _ _ _ notExists results (Not (Exists var conj)) = do
    row <- results
    notExists freevars2 var conj row

filterResults _ _ _ _ _ _ _ _ _ (Not _) = error "not not pushed to atoms or existential quantifications"

limitvarsInRow :: [Var] -> MapResultRow -> MapResultRow
limitvarsInRow vars row = transform (keys row) vars row

data MapDB (m :: * -> * )= MapDB String String [(ResultValue, ResultValue)] deriving Show

instance (Functor m, Monad m) => Database_ (MapDB m) m MapResultRow where
    dbBegin _ = return ()
    dbCommit _ = return ()
    dbRollback _ = return ()
    getName (MapDB name _ _) = name
    getPreds (MapDB _ predname _) = [ Pred predname (PredType ObjectPred [Key "String", Key "String"]) ]
    domainSize db varDomainSize thesign (Atom thepred args)
        | thepred `elem` getPreds db = return (case thesign of
            Pos -> case db of
                MapDB _ _ rows ->
                    mmins (map (exprDomainSizeMap varDomainSize (Bounded (length rows))) args)
            Neg ->
                mmins (map (exprDomainSizeMap varDomainSize Unbounded) args)) -- this just look up each var from the varDomainSize
    domainSize _ _ _ _ = return empty

    doQuery (MapDB _ _ rows) (Query vars disjs) _ stream  = do
        row2 <- mapDBFilterResults rows  stream disjs
        return (limitvarsInRow vars row2)
    doInsert db (Insert lits disjs) rsvars stream = do
        let (MapDB _ _ rows) = db
        let freevars = freeVars lits
        let add rows1 row = rows1 `union` [row]
        let remove rows1 row = rows1 \\ [row]
        let arg12 (Lit _ (Atom _ [a,b])) = (convert a, convert b)
            arg12 _ = error "wrong number of args"
        rows1 <- lift (getAllResultsInStream (doQuery db (Query freevars disjs) rsvars stream))
        let rows2 = foldl (\rows' lit@(Lit thesign (Atom _ _)) ->
                foldl (\rows'' row1 -> (case thesign of
                        Pos -> add
                        Neg -> remove) rows'' (arg12 (substResultValue row1 lit))) rows' rows1) rows lits
        return empty
    supported (MapDB _ predname _) (Atomic (Atom (Pred p _) _)) | p == predname = True
    supported _ _ = False
    supportedInsert _ _ _ = False


-- update mapdb

data StateMapDB (m :: * -> * )= StateMapDB String String deriving Show

instance (Functor m, Monad m) => Database_ (StateMapDB m) (StateT [(ResultValue, ResultValue)] m) MapResultRow where
    dbBegin _ = return ()
    dbCommit _ = return ()
    dbRollback _ = return ()
    getName (StateMapDB name _) = name
    getPreds (StateMapDB _ predname) = [ Pred predname (PredType ObjectPred [Key "String", Key "String"]) ]
    domainSize db varDomainSize thesign (Atom thepred args)
        | thepred `elem` getPreds db = case thesign of
            Pos -> do
                rows <- get
                return (mmins (map (exprDomainSizeMap varDomainSize (Bounded (length rows))) args))
            Neg ->
                return (mmins (map (exprDomainSizeMap varDomainSize Unbounded) args)) -- this just look up each var from the varDomainSize
    domainSize _ _ _ _ = return empty

    doQuery db (Query vars disjs) _ stream  = do
        rows <- lift get
        row2 <- mapDBFilterResults rows stream disjs
        return (limitvarsInRow vars row2)
    doInsert db (Insert lits disjs) rsvars stream = do
        rows <- lift get
        let freevars = freeVars lits
        let add rows1 row = rows1 `union` [row]
        let remove rows1 row = rows1 \\ [row]
        let arg12 (Lit _ (Atom _ [a,b])) = (convert a, convert b)
            arg12 _ = error "wrong number of args"
        rows1 <- lift (getAllResultsInStream (doQuery db (Query freevars disjs) rsvars stream))
        let rows2 = foldl (\rows' lit@(Lit thesign (Atom _ _)) ->
                foldl (\rows'' row1 -> (case thesign of
                        Pos -> add
                        Neg -> remove) rows'' (arg12 (substResultValue row1 lit))) rows' rows1) rows lits
        lift $ put rows2
        return empty
    supported _ (Atomic _) = True
    supported _ _ = False
    supportedInsert _ (Conjunction []) [_]  = True
    supportedInsert _ _ _ = False




mapDBFilterResults :: (Functor m, Monad m) => [(ResultValue, ResultValue)] -> ResultStream m MapResultRow -> Formula -> ResultStream m MapResultRow
mapDBFilterResults rows  results (Atomic (Atom thepred args)) = do
    resrow <- results
    case args of
        VarExpr var1 : (VarExpr var2 : _)
            | var1 `member` resrow ->
                if var2 `member` resrow then do
                    guard ((resrow ! var1, resrow ! var2) `elem` rows)
                    return mempty
                else
                    listResultStream [singleton var2 (snd x) | x <- rows, fst x == resrow ! var1]

            | var2 `member` resrow ->
                listResultStream [singleton var1 (fst x) | x <- rows, snd x == resrow ! var2]
            | otherwise ->
                listResultStream [fromList [(var1, fst x), (var2, snd x)] | x <- rows]
        VarExpr var1 : StringExpr str2 : _ ->
            if var1 `member` resrow then do
                guard ((resrow ! var1, StringValue str2) `elem` rows)
                return mempty
            else
                listResultStream [singleton var1 (fst x) | x <- rows, snd x == StringValue str2]
        StringExpr str1 : VarExpr var2 : _ ->
            if var2 `member` resrow then do
                guard ((StringValue str1, resrow ! var2) `elem` rows)
                return mempty
            else
                listResultStream [singleton var2 (snd x) | x <- rows, snd x == StringValue str1]
        StringExpr str1 : StringExpr str2 : _ -> do
                guard ((StringValue str1, StringValue str2) `elem` rows)
                return mempty
        _ -> do
            guard False
            return mempty


-- example EqDB

data EqDB (m :: * -> *) = EqDB String
instance (Functor m, Monad m) => Database_ (EqDB m) m MapResultRow where
    dbBegin _ = return ()
    dbCommit _ = return ()
    dbRollback _ = return ()
    getName (EqDB name) = name
    getPreds _ = [ Pred "eq" (PredType EssentialPred [Key "String", Key "String"]) ]
    domainSize db varDomainSize thesign (Atom thepred args)
        | thepred `elem` getPreds db =
            let [d1, d2] = map (exprDomainSizeMap varDomainSize Unbounded) args in
                return (case thesign of
                    Pos -> case toList d1 of
                        [] -> d2
                        [(var1, ds1)] -> case toList d2 of
                            [] -> d1
                            [(var2, ds2)] ->
                                let ds = min ds1 ds2 in
                                    fromList [(var, ds) | var <- [var1, var2]]
                    Neg -> mmin d1 d2)
    domainSize _ _ _ _ = return empty

    doQuery db (Query vars disjs) rsvars stream = (do
        row2 <- filterResults vars filterBy expand1 expand2 expand12 exists excludeBy notExists stream (convertForall Pos disjs)
        return (limitvarsInRow vars row2)) where
            filterBy _ str1 str2 resrow = listResultStream [resrow | str1 == str2]
            excludeBy _ str1 str2 resrow = listResultStream [resrow | str1 /= str2 ]
            exists freevars _ conj resrow = doQuery db (Query freevars conj) rsvars (return resrow)
            notExists freevars _ conj resrow = do
                e <- eos (doQuery db (Query freevars conj) rsvars (return resrow))
                listResultStream (if e
                    then [resrow]
                    else [])
            expand1 _ var1 str2 resrow = listResultStream [insert var1 str2 resrow ]
            expand2 _ str1 var2 resrow = listResultStream [insert var2 str1 resrow ]
            expand12 _ _ _ _ = error "unconstrained eq predicate"
    doInsert _ _ _ _ = do
        error "cannot update equality constaints"
    supported _ (Atomic (Atom (Pred "eq" _) _)) = True
    supported _ (Not (Atomic (Atom (Pred "eq" _) _))) = True
    supported _ _ = False
    supportedInsert _ _ _ = False
