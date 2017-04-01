{-# LANGUAGE TypeFamilies, MultiParamTypeClasses, ExistentialQuantification, FlexibleInstances, OverloadedStrings,
   RankNTypes, FlexibleContexts, GADTs #-}
module QueryArrow.Utils where

import QueryArrow.Semantics.ResultStream
import QueryArrow.Syntax.Data
import QueryArrow.ListUtils
import QueryArrow.DB.DB
import QueryArrow.Data.Some

import Prelude  hiding (lookup)
import Data.Map.Strict (Map, empty, alter, lookup)
import Data.List ((\\), intercalate, transpose)
import Control.Monad.Catch
import Data.Maybe
import Data.Tree
import Data.Text (pack, unpack)
import Data.Text.Encoding (encodeUtf8, decodeUtf8)
import QueryArrow.Semantics.ResultValue
import QueryArrow.Semantics.ResultRow
import QueryArrow.Semantics.ResultRow.VectorResultRow
import QueryArrow.Semantics.ResultHeader
import QueryArrow.Semantics.ResultHeader.VectorResultHeader
import QueryArrow.Semantics.ResultValue.AbstractResultValue
import Data.Int (Int64)
import Data.Namespace.Namespace
import qualified Data.Vector as V

intResultStream :: (Monad m) => Int64 -> ResultStream m (VectorResultRow AbstractResultValue)
intResultStream i =
  let hdr = V.fromList [Var "i"] in
      listResultStream [newRow hdr (V.fromList [Some (Int64Value i)])]

-- map from predicate name to database names
type PredDBMap = Map Pred [String]

constructDBPredMap :: IDatabase0 db => db -> PredMap
constructDBPredMap db = foldr addPredToMap mempty preds where
        addPredToMap thepred map1 =
            insertObject (predName thepred) thepred map1
        preds = getPreds db

-- construct map from predicates to db names
constructPredDBMap :: [AbstractDatabase trans row form] -> PredDBMap
constructPredDBMap = foldr addPredFromDBToMap empty where
        addPredFromDBToMap :: AbstractDatabase trans row form -> PredDBMap -> PredDBMap
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

pprint2 :: Bool -> [String] -> [[String]] -> String
pprint2 showhdr vars rows =
  pprint3 showhdr (vars, rows)

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

pprint :: Bool -> [Var] -> [VectorResultRow AbstractResultValue] -> String
pprint showhdr vars rows =
    pprint2 showhdr (map unVar vars) (map (\row -> map (\a -> case a of
                                                                  Some rv -> show (toConcreteResultValue rv)) (V.toList row)) rows)

evalExpr :: ResultHeader -> VectorResultRow AbstractResultValue -> Expr -> AbstractResultValue
evalExpr _ _ (StringExpr s) = Some (StringValue s)
evalExpr _ _ (IntExpr s) = Some (Int64Value (fromIntegral s))
evalExpr hdr row (VarExpr v) = case ext v hdr row of
    Nothing -> Some Null
    Just r -> r
evalExpr hdr row (CastExpr TextType e) =
    Some (StringValue (case evalExpr hdr row e of
      Some r -> case toConcreteResultValue r of
        Int64Value i -> pack (show i)
        StringValue s -> s
        ByteStringValue bs -> decodeUtf8 bs
        _ -> error "cannot cast"))
evalExpr hdr row (CastExpr ByteStringType e) =
    Some (ByteStringValue (case evalExpr hdr row e of
      Some r -> case toConcreteResultValue r of
        Int64Value i -> encodeUtf8 (pack (show i))
        StringValue s -> encodeUtf8 s
        ByteStringValue bs -> bs
        _ -> error "cannot cast"))
evalExpr hdr row (CastExpr Int64Type e) =
    Some (Int64Value (case evalExpr hdr row e of
      Some r -> case toConcreteResultValue r of
        Int64Value i -> i
        StringValue s -> read (unpack s)
        ByteStringValue bs -> read (unpack (decodeUtf8 bs))
        _ -> error "cannot cast"))


evalExpr _ _ expr = error ("evalExpr: unsupported expr " ++ show expr)

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
