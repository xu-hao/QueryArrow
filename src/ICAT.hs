{-# LANGUAGE DeriveGeneric, FlexibleContexts, TemplateHaskell #-}
module ICAT where

import ICATGen
import FO.Data

standardPreds :: [Pred]
standardPreds = preds ++ [
        Pred "le" (PredType EssentialPred [Key "BigInt", Key "BigInt"]),
        Pred "lt" (PredType EssentialPred [Key "BigInt", Key "BigInt"]),
        Pred "eq" (PredType EssentialPred [Key "Any", Key "Any"]),
        Pred "like" (PredType EssentialPred [Key "String", Key "Pattern"]),
        Pred "like_regex" (PredType EssentialPred [Key "String", Key "Pattern"])]
