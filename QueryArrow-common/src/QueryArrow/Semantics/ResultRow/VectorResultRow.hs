{-# LANGUAGE ExistentialQuantification, MultiParamTypeClasses, TypeSynonymInstances, FlexibleInstances, FlexibleContexts, TypeFamilies, ScopedTypeVariables #-}
module QueryArrow.Semantics.ResultRow.VectorResultRow where

import QueryArrow.Syntax.Data
import QueryArrow.Semantics.ResultValue
import QueryArrow.Semantics.ResultRow
import QueryArrow.Semantics.ResultHeader.VectorResultHeader

import Data.Vector (Vector)
import qualified Data.Vector as V
import Data.Maybe
import QueryArrow.Data.Monoid.Action
import QueryArrow.Semantics.Sendable

-- result row
type VectorResultRow a = Vector a

instance Action (ResultRowTransformer a) (VectorResultRow a) where
  RRId `act` a = a
  RRTrans rowtrans `act` vec = V.map (\eic2 -> case eic2 of
                                                    Left i -> fromMaybe (error ("transform of VectorResultRow: index out of range " ++ show (V.length vec) ++ " " ++ show i))(vec V.!? i)
                                                    Right a -> a) rowtrans
instance (Ord a, Num a, Fractional a, ResultValue a) => IResultRow (VectorResultRow a) where
    type ElemType (VectorResultRow a) = a
    type HeaderType (VectorResultRow a) = ResultHeader
    type RowTransType (VectorResultRow a) = ResultRowTransformer a
    ext v hdr map1 = (map1 V.!) <$> V.findIndex ( == v) hdr
    updateRow vl hdr row = row V.// concatMap (\(v1, a) ->
                                case V.findIndex ( == v1) hdr of
                                    Nothing -> []
                                    Just i -> [(i, a)]) vl
    newRow _ vl = vl

data ResultRowTransformer a = RRId | RRTrans (Vector (Either Int a))

instance Sendable a => Sendable (VectorResultRow a) where
  send h vec = do
    let len = V.length vec
    hPutWord8 h (fromIntegral len) -- assuming no more than 255 columns
    V.mapM_ (send h) vec

instance Receivable a => Receivable (VectorResultRow a) where
  receive h = do
    len <- hGetWord8 h -- assuming no more than 255 columns
    V.replicateM (fromIntegral len) (receive h)
