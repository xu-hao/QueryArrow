module BuiltIn where

import System.IO (FilePath, readFile)
import Text.Read (readMaybe)
import FO.Data

standardBuiltInPreds = [
        Pred (UQPredName "le") (PredType ObjectPred [Key "Int", Key "Int"]),
        Pred (UQPredName "lt") (PredType ObjectPred [Key "Int", Key "Int"]),
        Pred (UQPredName "eq") (PredType ObjectPred [Key "Any", Property "Any"]),
        Pred (UQPredName "ge") (PredType ObjectPred [Key "Int", Key "Int"]),
        Pred (UQPredName "gt") (PredType ObjectPred [Key "Int", Key "Int"]),
        Pred (UQPredName "ne") (PredType ObjectPred [Key "Any", Property "Any"]),
        Pred (UQPredName "like") (PredType ObjectPred [Key "Text", Key "Pattern"]),
        Pred (UQPredName "not_like") (PredType ObjectPred [Key "Text", Key "Pattern"]),
        Pred (UQPredName "like_regex") (PredType ObjectPred [Key "Text", Key "Pattern"]),
        Pred (UQPredName "not_like_regex") (PredType ObjectPred [Key "Text", Key "Pattern"]),
        Pred (UQPredName "in") (PredType ObjectPred [Key "Text", Key "Text"]),
        Pred (UQPredName "add") (PredType ObjectPred [Key "Int", Key "Int", Property "Int"]),
        Pred (UQPredName "sub") (PredType ObjectPred [Key "Int", Key "Int", Property "Int"]),
        Pred (UQPredName "mul") (PredType ObjectPred [Key "Int", Key "Int", Property "Int"]),
        Pred (UQPredName "div") (PredType ObjectPred [Key "Int", Key "Int", Property "Int"]),
        Pred (UQPredName "mod") (PredType ObjectPred [Key "Int", Key "Int", Property "Int"]),
        Pred (UQPredName "exp") (PredType ObjectPred [Key "Int", Key "Int", Property "Int"]),
        Pred (UQPredName "concat") (PredType ObjectPred [Key "Text", Key "Text", Property "Text"]),
        Pred (UQPredName "substr") (PredType ObjectPred [Key "Text", Key "Int", Key "Int", Property "Text"]),
        Pred (UQPredName "regex_replace") (PredType ObjectPred [Key "Text", Key "Text", Key "Text", Property "Text"]),
        Pred (UQPredName "replace") (PredType ObjectPred [Key "Text", Key "Text", Key "Text", Property "Text"]),
        Pred (UQPredName "strlen") (PredType ObjectPred [Key "Text", Property "Int"])
        ]


standardBuiltInPredsMap = constructPredMap standardBuiltInPreds

qStandardBuiltInPreds :: String -> [Pred]
qStandardBuiltInPreds ns = map (setPredNamespace ns) standardBuiltInPreds

qStandardBuiltInPredsMap ns = constructPredMap (qStandardBuiltInPreds ns)
