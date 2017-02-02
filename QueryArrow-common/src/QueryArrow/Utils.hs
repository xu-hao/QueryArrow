{-# LANGUAGE TypeFamilies, MultiParamTypeClasses, FunctionalDependencies, ExistentialQuantification, FlexibleInstances, OverloadedStrings,
   RankNTypes, FlexibleContexts, GADTs, DeriveGeneric #-}
module QueryArrow.Utils where

import QueryArrow.DB.ResultStream
import QueryArrow.FO.Data
import QueryArrow.ListUtils
import QueryArrow.DB.DB

import Prelude  hiding (lookup)
import Data.Map.Strict (Map, empty, insert, alter, lookup, mapKeys)
import qualified Data.Map.Strict as M
import Data.List ((\\), intercalate, transpose)
import Data.Convertible
import Control.Monad.Catch
import Data.Maybe
import Data.Tree

import Data.Namespace.Namespace

-- instance Convertible MapResultRow (Map Var Expr) where
--     safeConvert = Right . M.map convert


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

combineLits :: PredTypeMap -> [Lit] -> ([Atom] -> [Atom] -> a) -> ([Atom] -> a) -> (Atom -> a) -> a
combineLits ptm lits generateUpdate generateInsert generateDelete = do
    let objpredlits = filter (isObjectPredLit ptm) lits
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

pprint2 :: Bool -> [String] -> [Map String String] -> String
pprint2 showhdr vars rows =
  pprint3 showhdr (vars, map (\row -> map (g row) vars) rows) where
    g::Map String String ->String->  String
    g row var  = case lookup var row of
        Nothing -> "null"
        Just e -> e

pprint3 :: Bool -> ([String], [[String]]) -> String
pprint3 showhdr (vars, rows) = (if showhdr then join vars ++ "\n" else "") ++ intercalate "\n" rowstrs ++ "\n" where
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

pprint :: Bool -> [Var] -> [MapResultRow] -> String
pprint showhdr vars rows =
    pprint2 showhdr (map unVar vars) (map (mapKeys unVar . M.map show) rows)

evalExpr :: MapResultRow -> Expr -> ResultValue
evalExpr row (StringExpr s) = StringValue s
evalExpr row (IntExpr s) = IntValue s
evalExpr row (VarExpr v) = case lookup v row of
    Nothing -> Null
    Just r -> r
evalExpr row expr = error ("evalExpr: unsupported expr " ++ show expr)

showPredMap :: PredMap -> String
showPredMap workspace =
  let show3 (k,a) = (case k of
                          Nothing -> ""
                          Just k -> k) ++ (case a of
                                    Nothing -> ""
                                    Just a -> ":" ++ show a)
  in
      drawTree (fmap show3 (toTree workspace))

samePredAndKey :: PredTypeMap -> Atom -> Atom -> Bool
samePredAndKey ptm (Atom p1 args1) (Atom p2 args2) | p1 == p2 =
    let predtype = fromMaybe (error ("samePredAndKey: cannot find predicate " ++ show p1)) (lookup p1 ptm) in
        keyComponents predtype args1 == keyComponents predtype args2
samePredAndKey _ _ _ = False

unionPred :: PredTypeMap -> [Atom] -> [Atom] -> [Atom]
unionPred ptm as1 as2 =
    as1 ++ filter (not . samePredAndKeys ptm as1) as2 where
        samePredAndKeys ptm as1 atom = any (samePredAndKey ptm atom) as1
