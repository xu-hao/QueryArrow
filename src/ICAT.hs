{-# LANGUAGE FlexibleContexts #-}
module ICAT where

import ICATGen
import FO.Data

standardPreds :: [Pred]
standardPreds = preds

standardBuiltInPreds = [
        Pred (UQPredName "le") (PredType ObjectPred [Key "BigInt", Key "BigInt"]),
        Pred (UQPredName "lt") (PredType ObjectPred [Key "BigInt", Key "BigInt"]),
        Pred (UQPredName "eq") (PredType ObjectPred [Key "Any", Key "Any"]),
        Pred (UQPredName "like") (PredType ObjectPred [Key "String", Key "Pattern"]),
        Pred (UQPredName "like_regex") (PredType ObjectPred [Key "String", Key "Pattern"])]


standardBuiltInPredsMap = constructPredMap standardBuiltInPreds

standardPredMap = constructPredMap standardPreds

qStandardPreds :: String -> [Pred]
qStandardPreds ns = map (setPredNamespace ns) standardPreds

qStandardBuiltInPreds :: String -> [Pred]
qStandardBuiltInPreds ns = map (setPredNamespace ns) standardBuiltInPreds

qStandardPredsMap ns = constructPredMap (qStandardPreds ns)

qStandardBuiltInPredsMap ns = constructPredMap (qStandardBuiltInPreds ns)
