module ICAT where

import System.IO (FilePath, readFile)
import Text.Read (readMaybe)
import FO.Data

standardBuiltInPreds = [
        Pred (UQPredName "le") (PredType ObjectPred [Key "BigInt", Key "BigInt"]),
        Pred (UQPredName "lt") (PredType ObjectPred [Key "BigInt", Key "BigInt"]),
        Pred (UQPredName "eq") (PredType ObjectPred [Key "Any", Property "Any"]),
        Pred (UQPredName "like") (PredType ObjectPred [Key "String", Key "Pattern"]),
        Pred (UQPredName "like_regex") (PredType ObjectPred [Key "String", Key "Pattern"]),
        Pred (UQPredName "in") (PredType ObjectPred [Key "String", Key "String"]),
        Pred (UQPredName "add") (PredType ObjectPred [Key "Int", Key "Int", Property "Int"]),
        Pred (UQPredName "concat") (PredType ObjectPred [Key "String", Key "String", Property "String"]),
        Pred (UQPredName "substr") (PredType ObjectPred [Key "String", Key "Int", Key "Int", Property "String"]),
        Pred (UQPredName "regex_replace") (PredType ObjectPred [Key "String", Key "String", Key "String", Property "String"]),
        Pred (UQPredName "replace") (PredType ObjectPred [Key "String", Key "String", Key "String", Property "String"]),
        Pred (UQPredName "strlen") (PredType ObjectPred [Key "String", Property "Int"])
        ]


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
    case readMaybe content of
        Just preds -> return preds
        Nothing -> error ("error cannot parse " ++ path)
