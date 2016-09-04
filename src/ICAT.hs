module ICAT where

import System.IO (FilePath, readFile)
import FO.Data

standardBuiltInPreds = [
        Pred (UQPredName "le") (PredType ObjectPred [Key "BigInt", Key "BigInt"]),
        Pred (UQPredName "lt") (PredType ObjectPred [Key "BigInt", Key "BigInt"]),
        Pred (UQPredName "eq") (PredType ObjectPred [Key "Any", Key "Any"]),
        Pred (UQPredName "like") (PredType ObjectPred [Key "String", Key "Pattern"]),
        Pred (UQPredName "like_regex") (PredType ObjectPred [Key "String", Key "Pattern"]),
        Pred (UQPredName "in") (PredType ObjectPred [Key "String", Key "String"])]


standardBuiltInPredsMap = constructPredMap standardBuiltInPreds

standardPredMap standardPreds = constructPredMap standardPreds

qStandardPreds :: String -> [Pred] -> [Pred]
qStandardPreds ns standardPreds = map (setPredNamespace ns) standardPreds

qStandardBuiltInPreds :: String -> [Pred]
qStandardBuiltInPreds ns = map (setPredNamespace ns) standardBuiltInPreds

qStandardPredsMap ns standardPreds = constructPredMap (qStandardPreds ns standardPreds)

qStandardBuiltInPredsMap ns = constructPredMap (qStandardBuiltInPreds ns)

loadPreds :: FilePath -> IO [Pred]
loadPreds path = do
    content <- readFile path
    return (read content)
