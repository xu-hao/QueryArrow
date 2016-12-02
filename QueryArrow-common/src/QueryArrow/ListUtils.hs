module QueryArrow.ListUtils where


import Prelude  hiding (lookup)
import Data.Map.Strict (Map, empty, insert, alter, lookup)
import Data.List ((\\), intercalate, transpose, union)


maximumd :: Ord a => a -> [a] -> a
maximumd d [] = d
maximumd _ l = maximum l

superset :: (Eq a) => [a] -> [a] -> Bool
superset s = all (`elem` s)

subset :: (Eq a) => [a] -> [a] -> Bool
subset s t = superset t s

endswith :: Eq a => [a] -> [a] -> Bool
endswith a b = drop (length b - length a) b == a

startswith :: Eq a => [a] -> [a] -> Bool
startswith a b = take (length a) b == a

replace :: Eq a => [a] -> [a] -> [a] -> [a]
replace old new [] = []
replace old new a@(x:xs) =
    if startswith a old
        then new ++ replace old new (drop (length old) a)
        else x : replace old new xs

unions :: Eq a => [[a]] -> [a]
unions = foldl union []

intersects :: Eq a => [[a]] -> [a]
intersects [] = error "can't intersect zero lists"
intersects (hd : tl) = foldl (\ as bs -> [ x | x <- as, x `elem` bs ]) hd tl

findRepeats :: Eq a => [a] -> [a]
findRepeats [] = []
findRepeats (a : l) =
        if a `elem` l
            then a : findRepeats (l \\ [a])
            else findRepeats l
