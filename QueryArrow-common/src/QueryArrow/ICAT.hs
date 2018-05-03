module QueryArrow.ICAT where

import System.IO (FilePath, readFile)
import Text.Read (readMaybe)
import QueryArrow.FO.Data

standardPredMap standardPreds = constructPredMap standardPreds

qStandardPreds :: String -> [Pred] -> [Pred]
qStandardPreds ns standardPreds = map (setPredNamespace ns) standardPreds

qStandardPredsMap ns standardPreds = constructPredMap (qStandardPreds ns standardPreds)
