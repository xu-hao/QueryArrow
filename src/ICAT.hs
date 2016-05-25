{-# LANGUAGE DeriveGeneric, FlexibleContexts, TemplateHaskell #-}
module ICAT where

import ICATGen
import FO.Data

standardPreds :: [Pred]
standardPreds = preds ++ standardBuiltInPreds

standardBuiltInPreds = [
        Pred "le" (PredType ObjectPred [Key "BigInt", Key "BigInt"]),
        Pred "lt" (PredType ObjectPred [Key "BigInt", Key "BigInt"]),
        Pred "eq" (PredType ObjectPred [Key "Any", Key "Any"]),
        Pred "like" (PredType ObjectPred [Key "String", Key "Pattern"]),
        Pred "like_regex" (PredType ObjectPred [Key "String", Key "Pattern"])]
