{-# LANGUAGE TypeFamilies, MultiParamTypeClasses, FunctionalDependencies, ExistentialQuantification, FlexibleInstances, OverloadedStrings,
   RankNTypes, FlexibleContexts, GADTs, DeriveGeneric #-}
module Utils where

import ResultStream
import FO.Data
import QueryPlan

import Prelude  hiding (lookup)
import Data.Map.Strict (Map, empty, insert, alter, lookup)
import Data.List ((\\), intercalate, transpose)
import Control.Monad.Catch


intResultStream :: (Functor m, Monad m) => Int -> ResultStream m MapResultRow
intResultStream i = return (insert (Var "i") (IntValue i) empty)

-- map from predicate name to database names
type PredDBMap = Map String [String]

constructPredMap :: [Database m row ] -> PredMap
constructPredMap = foldr addPredFromDBToMap empty where
    addPredFromDBToMap (Database db) predmap = foldr addPredToMap predmap preds where
        addPredToMap thepred = insert (predName thepred) thepred
        preds = getPreds db

-- construct map from predicates to db names
constructPredDBMap :: [Database m row ] -> PredDBMap
constructPredDBMap = foldr addPredFromDBToMap empty where
    addPredFromDBToMap (Database db) predmap = foldr addPredToMap predmap preds where
        dbname = getName db
        addPredToMap thepred = alter alterValue (predName thepred) where
            alterValue Nothing = Just [dbname]
            alterValue (Just dbnames) = Just (dbname : dbnames)
        preds = getPreds db


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
        x -> error ("combineLits: uncombinable " ++ show x)


maximumd :: Ord a => a -> [a] -> a
maximumd d [] = d
maximumd _ l = maximum l

superset :: (Eq a) => [a] -> [a] -> Bool
superset s = all (`elem` s)

dbCatch :: (MonadCatch m) => m a -> m (Either SomeException a)
dbCatch action =
        catch (do
            r <- action
            return (Right r)) (\e ->
                return (Left e))

pprint :: ([String], [Map String String]) -> String
pprint (vars, rows) = join vars ++ "\n" ++ intercalate "\n" rowstrs ++ "\n" where
    join = intercalate " " . map (uncurry pad) . zip collen
    rowstrs = map join m2
    m2 = transpose m
    collen = map (maximumd 0 . map length) m
    m = map f vars
    f var = map (g var) rows
    g::String-> Map String String -> String
    g var row = case lookup var row of
        Nothing -> "null"
        Just e -> e
    pad n s
        | length s < n  = s ++ replicate (n - length s) ' '
        | otherwise     = s

pprint3 :: ([String], [[String]]) -> String
pprint3 (vars, rows) = join vars ++ "\n" ++ intercalate "\n" rowstrs ++ "\n" where
            join = intercalate " " . map (uncurry pad) . zip collen
            rowstrs = map join m2
            m2 = transpose m
            collen = map (maximumd 0 . map length) m
            m = map f [0..length vars-1]
            f var = map (g var) rows
            g::Int->  [String] -> String
            g var row = row !! var
            pad n s
                | length s < n  = s ++ replicate (n - length s) ' '
                | otherwise     = s

pprint2 :: [Var] -> [MapResultRow] -> String
pprint2 vars rows = join  (map unVar vars) ++ "\n" ++ intercalate "\n" rowstrs ++ "\n" where
    join = intercalate " " . map (uncurry pad) . zip collen
    rowstrs = map join m2
    m2 = transpose m
    collen = map (maximumd 0 . map length) m
    m = map f vars
    f var = map (g var) rows
    g::Var->MapResultRow->String
    g var row = case lookup var row of
        Nothing -> "null"
        Just e -> show e
    pad n s
        | length s < n  = s ++ replicate (n - length s) ' '
        | otherwise     = s
