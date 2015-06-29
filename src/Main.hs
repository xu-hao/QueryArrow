module Main where

import QueryArrow
import Control.Arrow
import Control.Category
import Data.Foldable
import qualified Data.Map as Map
import Control.Monad.State hiding (lift)
import Prelude hiding (id,(.), concat, foldr)

graphFromList :: [CGEdge] -> CGraph
graphFromList = foldr (\ (vout, l, vin) ->
            insertVertex vin . insertVertex vout . insertEdge vin l (vout, l, vin) .  insertEdge vout l (vout, l, vin)
        ) Map.empty where
            insertVertex v m = if Map.member v m then m else Map.insert v Map.empty m
            insertEdge v l e = Map.adjust (\ em ->
                                     if Map.member l em then Map.adjust (\es -> es ++ [e]) l em else Map.insert l [e] em) v

g :: CGraph
g = graphFromList [("1","a","2"), ("1","a","4"), ("4","c","3"), ("4","c","5"), ("3","a","5"), ("5","d","4"), ("2", "b","3"), ("3", "c", "1")]

main :: IO ()
main = 
    let (a, _) = runCQuery (startV "1" >>> selectE "a" >>> selectOutV >>> groupCount (Kleisli (\ p -> 
            return (case p of VLeaf v -> v
                              VCons v _ -> v)))) g () in
        print a
