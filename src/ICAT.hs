{-# LANGUAGE DeriveGeneric, FlexibleContexts, TemplateHaskell #-}
module ICAT where

import ICATGen
import FO.Data

standardPreds :: [Pred]
standardPreds = preds

standardBuiltInPreds = [
        Pred (PredName Nothing "le") (PredType ObjectPred [Key "BigInt", Key "BigInt"]),
        Pred (PredName Nothing "lt") (PredType ObjectPred [Key "BigInt", Key "BigInt"]),
        Pred (PredName Nothing "eq") (PredType ObjectPred [Key "Any", Key "Any"]),
        Pred (PredName Nothing "like") (PredType ObjectPred [Key "String", Key "Pattern"]),
        Pred (PredName Nothing "like_regex") (PredType ObjectPred [Key "String", Key "Pattern"])]


standardBuiltInPredsMap = constructPredMap standardBuiltInPreds

standardPredMap = constructPredMap standardPreds

qStandardPreds :: String -> [Pred]
qStandardPreds ns = map (setPredNamespace ns) standardPreds

qStandardBuiltInPreds :: String -> [Pred]
qStandardBuiltInPreds ns = map (setPredNamespace ns) standardBuiltInPreds

qStandardPredsMap ns = constructPredMap (qStandardPreds ns)

qStandardBuiltInPredsMap ns = constructPredMap (qStandardBuiltInPreds ns)
