{-# LANGUAGE TypeFamilies, MultiParamTypeClasses, FunctionalDependencies, ExistentialQuantification, FlexibleInstances, OverloadedStrings,
   RankNTypes, FlexibleContexts, GADTs, DeriveGeneric #-}
module Utils where

import ResultStream
import FO.Data
import FO.Domain
import QueryPlan
import Rewriting

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


intResultStream :: (Functor m, Monad m) => Int -> ResultStream m MapResultRow
intResultStream i = return (insert (Var "i") (IntValue i) empty)

-- map from predicate name to database names
type PredDBMap = Map String [String]

constructPredMap :: [Database m  row] -> PredMap
constructPredMap = foldr addPredFromDBToMap empty where
    addPredFromDBToMap (Database db) predmap = foldr addPredToMap predmap preds where
        addPredToMap thepred = insert (predName thepred) thepred
        preds = getPreds db

-- construct map from predicates to db names
constructPredDBMap :: [Database m  row] -> PredDBMap
constructPredDBMap = foldr addPredFromDBToMap empty where
    addPredFromDBToMap (Database db) predmap = foldr addPredToMap predmap preds where
        dbname = getName db
        addPredToMap thepred = alter alterValue (predName thepred) where
            alterValue Nothing = Just [dbname]
            alterValue (Just dbnames) = Just (dbname : dbnames)
        preds = getPreds db


-- a formula is called "effective" only when it has a finite domain size.
-- a variable is called "determined" only when it has a finite domain size.
-- the first stage is a query and subsequent stages are filters
-- each stage T is associated with a database D
-- a set S of literals is assigned a stage T. Denote by A -> T -> D. The assignment must satisfy the following
-- for each L in S
-- (1) the predicate in L is provided by D and
-- (2) all variable in L are determined by S and D or previous stages
-- a variable is determined by S and D, iff
-- there is a literal L in S such that
-- (1) x in fv(L) and
-- (2) L has a finite domain size in D given the variables determined in previous stages

-- given
-- a list of variables that has already been determined
-- a list of databases
-- a list of effective literals
-- a list of literal
-- find the longest prefix of the literal list the literals in which become effective in at least one databases
findEffectiveFormulas :: (Monad m ) => ([(Database m  row, DomainSizeMap)], [Formula], [Formula]) -> m ([(Database m  row, DomainSizeMap)], [Formula], [Formula])
findEffectiveFormulas (db_maps, effective, []) = return (db_maps, effective, [])
findEffectiveFormulas (db_maps, effective, candidates@(formula : rest)) = do
    let isFormulaEffective formula' (db@(Database db_), map1) = do
        map1' <- determinedVars (domainSize db_ map1) Pos formula'
        let map2 = mmin map1 map1'
        let freevars = freeVars formula'
        return (if all (\freevar -> case lookupDomainSize freevar map2 of
            Bounded _ -> True
            Unbounded -> False) freevars
                then (Just (db, map2))
                else Nothing)
    db_mapmaybes <- mapM (isFormulaEffective formula) db_maps
    let db_mapsnew = catMaybes db_mapmaybes
    if null db_mapsnew
        then return (db_maps, effective, candidates)
        else findEffectiveFormulas (db_mapsnew, effective ++ [formula], rest)

combineLits :: [Lit] -> ([Atom] -> [Atom] -> a) -> ([Atom] -> a) -> (Atom -> a) -> a
combineLits lits generateUpdate generateInsert generateDelete = do
    let objpredlits = filter isObjectPredLit lits
    let proppredlits = lits \\ objpredlits
    let (posobjpredatoms, negobjpredatoms) = splitPosNegLits objpredlits
    let (pospropredatoms, negproppredatoms) = splitPosNegLits proppredlits
    case (posobjpredatoms, negobjpredatoms) of
        ([], []) ->  generateUpdate pospropredatoms negproppredatoms   -- update property
        ([posobjatom], []) ->  case negproppredatoms of
            [] -> generateInsert (posobjatom:pospropredatoms)      -- insert
            _ -> error "trying to delete properties of an object to be created"
        ([], [negobjatom]) ->   case (pospropredatoms, negproppredatoms) of
            ([], _) -> generateDelete negobjatom
            _ -> error "tyring to modify propertiese of an object to be deleted" -- delete
        ([posobjatom], [negobjatom]) -> generateUpdate (posobjatom:pospropredatoms) (negobjatom:negproppredatoms) -- update property
