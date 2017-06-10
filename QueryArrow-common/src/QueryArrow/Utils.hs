{-# LANGUAGE TypeFamilies, MultiParamTypeClasses, ExistentialQuantification, FlexibleInstances, OverloadedStrings,
   RankNTypes, FlexibleContexts, GADTs #-}
module QueryArrow.Utils where

import QueryArrow.DB.ResultStream
import QueryArrow.FO.Data
import QueryArrow.ListUtils
import QueryArrow.DB.DB

import Prelude  hiding (lookup)
import Data.Map.Strict (Map, empty, insert, alter, lookup, mapKeys)
import qualified Data.Map.Strict as M
import Data.List ((\\), intercalate, transpose)
import Control.Monad.Catch
import Data.Maybe
import Data.Tree
import Data.Text (pack, unpack)
import Data.Text.Encoding
import Data.Int (Int64)

import Data.Namespace.Namespace

intResultStream :: (Monad m) => Int64 -> ResultStream m MapResultRow
intResultStream i = return (insert (Var "i") (AbstractResultValue (Int64Value i)) empty)

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
            addPredToMap thepred = alter alterValue thepred
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
    g row var0  = case lookup var0 row of
        Nothing -> "null"
        Just e -> e

pprint3 :: Bool -> ([String], [[String]]) -> String
pprint3 showhdr (vars, rows) = (if showhdr then join vars ++ "\n" else "") ++ intercalate "\n" rowstrs ++ "\n" where
            join = intercalate " " . map (uncurry pad) . zip collen
            rowstrs = map join m2
            m2 = transpose m
            collen = map (maximumd 0 . map length) m
            m = map f [0..length vars-1]
            f var0 = map (g var0) rows
            g::Int->  [String] -> String
            g var0 row = row !! var0
            pad n s
                | length s < n  = s ++ replicate (n - length s) ' '
                | otherwise     = s

pprint :: Bool -> Bool -> [Var] -> [MapResultRow] -> String
pprint showhdr showdetails vars rows =
    pprint2 showhdr (map unVar vars) (map (mapKeys unVar . M.map (if showdetails then show else show2)) rows) where 
             show2 a =
                 case a of 
                     AbstractResultValue a2 ->
                         case toConcreteResultValue a2 of
                             Int64Value i -> show i
                             Int32Value i -> show i
                             StringValue i -> show i
                             _ -> show a2

evalExpr :: MapResultRow -> Expr -> AbstractResultValue
evalExpr _ (StringExpr s) = AbstractResultValue (StringValue s)
evalExpr _ (IntExpr s) = AbstractResultValue (Int64Value (fromIntegral s))
evalExpr row (VarExpr v) = case lookup v row of
    Nothing -> AbstractResultValue Null
    Just r -> r
evalExpr row (CastExpr TextType e) =
    AbstractResultValue (StringValue (case evalExpr row e of
      AbstractResultValue r -> case toConcreteResultValue r of
        Int64Value i -> pack (show i)
        StringValue s -> s
        ByteStringValue bs -> decodeUtf8 bs
        _ -> error "cannot cast"))
evalExpr row (CastExpr ByteStringType e) =
    AbstractResultValue (ByteStringValue (case evalExpr row e of
      AbstractResultValue r -> case toConcreteResultValue r of
        Int64Value i -> encodeUtf8 (pack (show i))
        StringValue s -> encodeUtf8 s
        ByteStringValue bs -> bs
        _ -> error "cannot cast"))
evalExpr row (CastExpr Int64Type e) =
    AbstractResultValue (Int64Value (case evalExpr row e of
      AbstractResultValue r -> case toConcreteResultValue r of
        Int64Value i -> i
        StringValue s -> read (unpack s)
        ByteStringValue bs -> read (unpack (decodeUtf8 bs))
        _ -> error "cannot cast"))


evalExpr _ expr = error ("evalExpr: unsupported expr " ++ show expr)

showPredMap :: PredMap -> String
showPredMap workspace =
  let show3 (k,a) = (case k of
                          Nothing -> ""
                          Just k2 -> k2) ++ (case a of
                                    Nothing -> ""
                                    Just a2 -> ":" ++ show a2)
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
        samePredAndKeys ptm0 as0 atom0 = any (samePredAndKey ptm0 atom0) as0
