{-# LANGUAGE FlexibleInstances #-}
module QueryArrow.Mapping where

import QueryArrow.Syntax.Term



standardPredMap :: [Pred] -> PredMap
standardPredMap standardPreds = constructPredMap standardPreds

qStandardPreds :: String -> [Pred] -> [Pred]
qStandardPreds ns standardPreds = map (setPredNamespace ns) standardPreds

qStandardPredsMap :: String -> [Pred] -> PredMap
qStandardPredsMap ns standardPreds = constructPredMap (qStandardPreds ns standardPreds)
