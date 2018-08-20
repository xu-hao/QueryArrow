module QueryArrow.BuiltIn where

import System.IO (FilePath, readFile)
import Text.Read (readMaybe)
import QueryArrow.Syntax.Term
import QueryArrow.Syntax.Type

standardBuiltInPreds = [
        Pred (UQPredName "le") (PredType ObjectPred [ParamType True True False False Int64Type, ParamType True True False False Int64Type]),
        Pred (UQPredName "lt") (PredType ObjectPred [ParamType True True False False Int64Type, ParamType True True False False Int64Type]),
        Pred (UQPredName "eq") (PredType ObjectPred [ParamType True True False False (TypeVar "a"), ParamType False True True False (TypeVar "a")]),
        Pred (UQPredName "ge") (PredType ObjectPred [ParamType True True False False Int64Type, ParamType True True False False Int64Type]),
        Pred (UQPredName "gt") (PredType ObjectPred [ParamType True True False False Int64Type, ParamType True True False False Int64Type]),
        Pred (UQPredName "ne") (PredType ObjectPred [ParamType True True False False (TypeVar "a"), ParamType True True False False (TypeVar "a")]),
        Pred (UQPredName "like") (PredType ObjectPred [ParamType True True False False TextType, ParamType True True False False TextType]),
        Pred (UQPredName "not_like") (PredType ObjectPred [ParamType True True False False TextType, ParamType True True False False TextType]),
        Pred (UQPredName "like_regex") (PredType ObjectPred [ParamType True True False False TextType, ParamType True True False False TextType]),
        Pred (UQPredName "not_like_regex") (PredType ObjectPred [ParamType True True False False TextType, ParamType True True False False TextType]),
        Pred (UQPredName "in") (PredType ObjectPred [ParamType True True False False (TypeVar "a"), ParamType True True False False (ListType (TypeVar "a"))]),
        Pred (UQPredName "add") (PredType ObjectPred [ParamType True True False False Int64Type, ParamType True True False False Int64Type, ParamType False False True False Int64Type]),
        Pred (UQPredName "sub") (PredType ObjectPred [ParamType True True False False Int64Type, ParamType True True False False Int64Type, ParamType False False True False Int64Type]),
        Pred (UQPredName "mul") (PredType ObjectPred [ParamType True True False False Int64Type, ParamType True True False False Int64Type, ParamType False False True False Int64Type]),
        Pred (UQPredName "div") (PredType ObjectPred [ParamType True True False False Int64Type, ParamType True True False False Int64Type, ParamType False False True False Int64Type]),
        Pred (UQPredName "mod") (PredType ObjectPred [ParamType True True False False Int64Type, ParamType True True False False Int64Type, ParamType False False True False Int64Type]),
        Pred (UQPredName "exp") (PredType ObjectPred [ParamType True True False False Int64Type, ParamType True True False False Int64Type, ParamType False False True False Int64Type]),
        Pred (UQPredName "concat") (PredType ObjectPred [ParamType True True False False TextType, ParamType True True False False TextType, ParamType False False True False TextType]),
        Pred (UQPredName "substr") (PredType ObjectPred [ParamType True True False False TextType, ParamType True True False False Int64Type, ParamType True True False False Int64Type, ParamType False False True False TextType]),
        Pred (UQPredName "regex_replace") (PredType ObjectPred [ParamType True True False False TextType, ParamType True True False False TextType, ParamType True True False False TextType, ParamType False False True False TextType]),
        Pred (UQPredName "replace") (PredType ObjectPred [ParamType True True False False TextType, ParamType True True False False TextType, ParamType True True False False TextType, ParamType False False True False TextType]),
        Pred (UQPredName "strlen") (PredType ObjectPred [ParamType True True False False TextType, ParamType False False True False Int64Type])
        ]


standardBuiltInPredsMap = constructPredMap standardBuiltInPreds

qStandardBuiltInPreds :: String -> [Pred]
qStandardBuiltInPreds ns = map (setPredNamespace ns) standardBuiltInPreds

qStandardBuiltInPredsMap ns = constructPredMap (qStandardBuiltInPreds ns)
