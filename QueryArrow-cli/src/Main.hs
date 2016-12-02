{-# LANGUAGE OverloadedStrings, TypeFamilies #-}
module Main where

--import QueryArrowList
--import QueryArrow
--import Control.Arrow
--import Control.Category
--import Data.Foldable
--import qualified Data.Map as Map
--import Control.Monad.State hiding (lift)
--import Prelude hiding (id,(.), concat, foldr)
--
--graphFromList :: [CGEdge] -> CGraph
--graphFromList = foldr (\ (vout, l, vin) ->
--            insertVertex vin . insertVertex vout . insertEdge vin l (vout, l, vin) .  insertEdge vout l (vout, l, vin)
--        ) Map.empty where
--            insertVertex v m = if Map.member v m then m else Map.insert v Map.empty m
--            insertEdge v l e = Map.adjust (\ em ->
--                                     if Map.member l em then Map.adjust (\es -> es ++ [e]) l em else Map.insert l [e] em) v
--
--l = [1,2,3,1,2,3]
--
--l2 = [ x | x <- l, then group using f] where f l = [l]
--
--g :: CGraph
--g = graphFromList [("1","a","2"), ("1","a","4"), ("4","c","3"), ("4","c","5"), ("3","a","5"), ("5","d","4"), ("2", "b","3"), ("3", "c", "1")]
--
--main :: IO ()
--main =
--    let (a, _) = runCQuery (startV "1" >>> selectE "a" >>> selectOutV >>> groupCount (Kleisli (\ p ->
--            return (case p of VLeaf v -> v
--                              VCons v _ -> v)))) g () in
--        print a
import QueryArrow.DB.ResultStream
import QueryArrow.DB.DB hiding (Null)
import QueryArrow.DBMap
import QueryArrow.Parser
-- import Plugins
import QueryArrow.FO.Data
import QueryArrow.Config
import QueryArrow.Utils

import Prelude hiding (lookup)
import Data.Set (fromList)
import Data.Map.Strict (singleton, keys, toList, Map)
import Text.Parsec (runParser)
import Control.Monad.IO.Class (liftIO)
import Control.Monad.Trans.Resource
import System.Environment
import Control.Monad.Except
import qualified Data.Text as T
import System.Log.Logger
import QueryArrow.Logging
import Control.Monad.Trans.Either
-- import Data.Serialize
import qualified Data.Set as Set
import QueryArrow.RPC.DB

main::IO()
main = do
    args2 <- getArgs
    mainArgs args2

mainArgs :: [String] -> IO ()
mainArgs args2 = do
    setup
    if null args2
        then
            putStrLn "usage: <command> <config file> <query> <headers>"

        else do
            ps <- getConfig (head args2)
            run2 (words (args2 !! 2)) (args2 !! 1) ps


run2 :: [String] -> String -> TranslationInfo -> IO ()
run2 hdr query ps = do
    let vars = map Var hdr
    AbstractDatabase tdb <- transDB "tdb" ps
    conn <- dbOpen tdb
    ret <- runEitherT $ run (fromList vars) query mempty tdb conn
    case ret of
      Left e -> putStrLn ("error: " ++ e)
      Right pp -> putStr (pprint vars pp)
    dbClose conn
