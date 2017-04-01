module QueryArrow.ICAT where

import System.IO (FilePath, readFile)
import Text.Read (readMaybe)
import QueryArrow.Syntax.Data

standardPredMap standardPreds = constructPredMap standardPreds

qStandardPreds :: String -> [Pred] -> [Pred]
qStandardPreds ns standardPreds = map (setPredNamespace ns) standardPreds

qStandardPredsMap ns standardPreds = constructPredMap (qStandardPreds ns standardPreds)

loadPreds :: FilePath -> IO [Pred]
loadPreds path = do
    content <- readFile path
    case readMaybe content of
        Just preds -> return preds
        Nothing -> error ("error cannot parse " ++ path)
