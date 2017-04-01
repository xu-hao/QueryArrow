{-# LANGUAGE TypeFamilies, MultiParamTypeClasses, FlexibleInstances, ScopedTypeVariables, GADTs, UndecidableInstances, ConstraintKinds,
   RankNTypes, FlexibleContexts #-}
module QueryArrow.Semantics.ResultSet.VectorResultSetTransformer where

import QueryArrow.Syntax.Data
import QueryArrow.Semantics.ResultValue
import QueryArrow.Semantics.ResultSet
import QueryArrow.Semantics.ResultHeader.VectorResultHeader
import QueryArrow.Semantics.ResultRow.VectorResultRow
import QueryArrow.Semantics.Sendable
import Data.Set
import Data.Vector (Vector)
import Data.Maybe
import qualified Data.Vector as V

data ResultSetTransformer a = RSId | RSTrans (Vector (Var, Either Int a))

instance Monoid (ResultSetTransformer a) where
  mempty = RSId
  RSId `mappend` a = a
  a `mappend` RSId = a
  RSTrans vec1 `mappend` RSTrans vec2 = RSTrans (V.map (\(var2, eic2) ->
                                                (var2, case eic2 of
                                                  Left index2 -> case vec1 V.!? index2 of
                                                                                  Just (_, eic1) -> eic1
                                                                                  Nothing ->  error ("mappend of ResultHeaderTransformer: index out of bound " ++ show var2 ++ " and " ++ show index2)
                                                  Right _ -> eic2)) vec2)

instance TotalConvertible (ResultSetTransformer a) ResultHeaderTransformer where
  tconvert RSId = RHId
  tconvert (RSTrans vec) = RHTrans (V.map fst vec)

instance TotalConvertible (ResultSetTransformer a) (ResultRowTransformer a) where
  tconvert RSId = RRId
  tconvert (RSTrans vec) = RRTrans (V.map snd vec)

instance Sendable a => Sendable (ResultSetTransformer a) where
  send h RSId = hPutWord8 h 0
  send h (RSTrans vec) = do
    hPutWord8 h 1
    hPutWord8 h (fromIntegral (V.length vec))
    V.mapM_ (send h) vec

instance Receivable a => Receivable (ResultSetTransformer a) where
  receive h = do
    byty <- hGetWord8 h
    case byty of
      0 -> return RSId
      1 -> do
        len <- fromIntegral <$> hGetWord8 h
        RSTrans <$> V.replicateM len (receive h)
      _ -> error ("receive of ResultSetTransformer a: unsupported constructor byte " ++ show byty)

instance (ResultValue a, Ord a, Fractional a) => Coherent (ResultSetTransformer a) (VectorResultRow a) where
  combineRow hdr row hdr2 =
    if V.null hdr
      then RSId
      else
        let off = V.length hdr
            len = V.length hdr2 in
            RSTrans (V.zipWith (\var1 a -> (var1, Right a)) hdr row V.++ V.generate len (\i  -> (hdr2 V.! i, Left (off + i))))

  filterRow _ vars hdr =
    let hdrfiltered = V.filter (\(var1, _) -> var1 `member` vars) (V.imap (\i var1 -> (var1, Left i)) hdr) in
        if V.null hdrfiltered
          then RSId
          else
            RSTrans hdrfiltered
  alignHeaders _ hdr hdr2 =
    if hdr == hdr2
      then RSId
      else
        RSTrans (V.map (\var1 -> (var1, Left (fromMaybe (error ("alignHeaders of Coherent (ResultSetTransformer a) (VectorResultRow a): cannot alignHeaders " ++ show hdr ++ show hdr2 ++ show var1)) (V.findIndex ( == var1) hdr2)))) hdr)
