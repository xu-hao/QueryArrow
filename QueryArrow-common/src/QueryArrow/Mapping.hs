{-# LANGUAGE StandaloneDeriving, FlexibleInstances, DeriveGeneric #-}
module QueryArrow.Mapping where

import System.IO (FilePath, readFile)
import Text.Read (readMaybe)
import QueryArrow.Syntax.Term
import QueryArrow.Semantics.Value
import Data.Aeson
import Data.Yaml
import Data.Namespace.Path
import QueryArrow.Syntax.Type
import QueryArrow.Semantics.TypeChecker
import QueryArrow.Serialize
import GHC.Generics



standardPredMap standardPreds = constructPredMap standardPreds

qStandardPreds :: String -> [Pred] -> [Pred]
qStandardPreds ns standardPreds = map (setPredNamespace ns) standardPreds

qStandardPredsMap ns standardPreds = constructPredMap (qStandardPreds ns standardPreds)
