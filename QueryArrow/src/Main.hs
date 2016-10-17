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
import DB.ResultStream
import DB.DB hiding (Null)
import DBMap
import Parser
-- import Plugins
import FO.Data
import Config
import Utils

import Prelude hiding (lookup)
import Data.Map.Strict (fromList, singleton, keys, toList, Map)
import Text.Parsec (runParser)
import Control.Monad.IO.Class (liftIO)
import Control.Monad.Trans.Resource
import System.Environment
import Data.Time
import Control.Exception
import Control.Monad.Except
import Serialization
import Data.Aeson
import Data.Aeson.Types
import qualified Data.Text as T
import System.Log.Logger
import Logging
-- import Data.Serialize
import qualified Data.Set as Set
import Data.String
import Control.Monad.Logger.HSLogger ()

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
    AbstractDatabase tdb <- transDB "tdb" ps
    conn <- dbOpen tdb
    (hdr, pp) <- run3 hdr query tdb conn "rods" "tempZone"
    putStr (pprint hdr pp)
    dbClose conn


run3 :: (IDatabase db, DBFormulaType db ~ Formula, RowType (StatementType (ConnectionType db)) ~ MapResultRow) => [String] -> String -> db -> ConnectionType db -> String -> String -> IO ([String], [Map String String])
run3 hdr query tdb conn user zone = do
    let predmap = constructDBPredMap tdb
    let params = fromList [(Var "client_user_name",StringValue (T.pack user)), (Var "client_zone", StringValue (T.pack zone))]


    r <- runResourceT $ dbCatch $ case runParser progp (mempty, predmap, mempty) "" query of
                            Left err -> error (show err)
                            Right (commands, _) ->
                                concat <$> mapM (\command -> case command of
                                    Begin -> do
                                        liftIO $ dbBegin conn
                                        return []
                                    Prepare -> do
                                        b <- liftIO $ dbPrepare conn
                                        if b
                                            then return []
                                            else do
                                                liftIO $ errorM "QA" "prepare failed, cannot commit"
                                                error "prepare failed, cannot commit"
                                    Commit -> do
                                        b <- liftIO $ dbCommit conn
                                        if b
                                            then return []
                                            else do
                                                liftIO $ errorM "QA" "commit failed"
                                                error "commit failed"
                                    Rollback -> do
                                        liftIO $ dbRollback conn
                                        return []
                                    Execute qu ->
                                        case runExcept (checkQuery qu) of
                                              Right _ -> getAllResultsInStream ( doQueryWithConn tdb conn (Set.fromList (map Var hdr)) qu (Set.fromList (keys params)) (pure params))
                                              Left e -> error e) commands
                            -- Right (Right Commit) -> do
                            --     b <- liftIO $ dbPrepare tdb
                            --     if b
                            --         then do
                            --             b <- liftIO $ dbCommit tdb
                            --             if b
                            --                 then return [mempty]
                            --                 else do
                            --                     liftIO $ dbRollback tdb
                            --                     liftIO $ errorM "QA" "prepare succeeded but cannot commit"
                            --         else do
                            --             liftIO $ dbRollback tdb
                            --             )
    return (case r of
        Right rows ->  (hdr, map (\row -> fromList (map (\(Var v,r) -> (v, show r)) (toList row))) rows)
        Left e ->  (["error"], [singleton "error" (show e)]))
