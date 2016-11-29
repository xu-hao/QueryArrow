{-# LANGUAGE TypeFamilies, MultiParamTypeClasses, FunctionalDependencies, ExistentialQuantification, FlexibleInstances, OverloadedStrings,
   RankNTypes, FlexibleContexts, GADTs, DeriveGeneric #-}
module Utils where

import DB.ResultStream
import FO.Data
import ListUtils
import DB.DB

import Prelude  hiding (lookup)
import Data.Map.Strict (Map, empty, insert, alter, lookup, mapKeys)
import qualified Data.Map.Strict as M
import Data.List ((\\), intercalate, transpose)
import Data.Convertible
import Control.Monad.Catch


import Data.Namespace.Path
import Data.Namespace.Namespace

instance Convertible MapResultRow (Map Var Expr) where
    safeConvert = Right . M.map convert


intResultStream :: (Functor m, Monad m) => Int -> ResultStream m MapResultRow
intResultStream i = return (insert (Var "i") (IntValue i) empty)

-- map from predicate name to database names
type PredDBMap = Map Pred [String]

constructDBPredMap :: IDatabase0 db => db -> PredMap
constructDBPredMap db = foldr addPredToMap mempty preds where
        addPredToMap thepred map1 =
            insertObject (predName thepred) thepred map1
        preds = getPreds db

-- construct map from predicates to db names
constructPredDBMap :: [AbstractDatabase row form] -> PredDBMap
constructPredDBMap = foldr addPredFromDBToMap empty where
        addPredFromDBToMap :: AbstractDatabase row form -> PredDBMap -> PredDBMap
        addPredFromDBToMap (AbstractDatabase db) predmap = foldr addPredToMap predmap preds where
            dbname = getName db
            addPredToMap thepred = alter alterValue  thepred
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


dbCatch :: (MonadCatch m) => m a -> m (Either SomeException a)
dbCatch action =
        try action

pprint2 :: [String] -> [Map String String] -> String
pprint2 vars rows =
  pprint3 (vars, map (\row -> map (g row) vars) rows) where
    g::Map String String ->String->  String
    g row var  = case lookup var row of
        Nothing -> "null"
        Just e -> e

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

pprint :: [Var] -> [MapResultRow] -> String
pprint vars rows =
    pprint2 (map unVar vars) (map (mapKeys unVar . M.map show) rows)
