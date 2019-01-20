{-# LANGUAGE OverloadedStrings, TypeFamilies #-}
module QueryArrow.Client where

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
import QueryArrow.DB.DB
import QueryArrow.DBMap
import QueryArrow.Config

import Prelude hiding (lookup)
import Control.Monad.Trans.Except
import QueryArrow.RPC.DB
import System.IO
import Control.Exception
import QueryArrow.Serialization ()
import Network.Socket
import QueryArrow.RPC.Data
import QueryArrow.RPC.Serialization ()
import QueryArrow.RPC.Message
import QueryArrow.Syntax.Term
import Data.Set (fromList)

import QueryArrow.Utils

data Query = QAL String | SQL Formula

resolve :: String -> String -> IO AddrInfo
resolve host port = do
  let hints = defaultHints { addrSocketType = Stream }
  addr:_ <- getAddrInfo (Just hints) (Just host) (Just port)
  return addr

open :: AddrInfo -> IO Handle
open addr = do
  sock <- socket (addrFamily addr) (addrSocketType addr) (addrProtocol addr)
  connect sock $ addrAddress addr
  socketToHandle sock ReadWriteMode
  

runTCP :: String -> Int -> Bool -> [String] -> Query -> MapResultRow -> IO ()
runTCP  host port showhdr hdr qu params =
  bracket
    (do
      addr <- resolve host (show port)
      open addr)
    hClose
    (\ handle -> runHandle handle showhdr hdr qu params)

runUDS :: String -> Bool -> [String] -> Query -> MapResultRow -> IO ()
runUDS addr showhdr hdr qu params =
  bracket
    (do
      sock <- socket AF_UNIX Stream defaultProtocol
      connect sock (SockAddrUnix addr)
      socketToHandle sock ReadWriteMode)
    hClose
    (\ handle -> runHandle handle showhdr hdr qu params)

runHandle :: Handle -> Bool -> [String] -> Query -> MapResultRow -> IO ()
runHandle handle showhdr hdr qu params = do
  sendMsgPack handle QuerySet {
                  qsquery = Static [Begin],
                  qsheaders = mempty,
                  qsparams = mempty}
  _ <- receiveMsgPack handle :: IO (Maybe ResultSet)
  let name = QuerySet {
                qsquery = case qu of 
                            QAL qu -> Dynamic qu
                            SQL qu -> Static [Execute qu],
                qsheaders = fromList (map Var hdr),
                qsparams = params
                }
  sendMsgPack handle name
  rep <- receiveMsgPack handle
  case rep of
      Just (ResultSetNormal results) -> do
                putStrLn (pprint showhdr False (map Var hdr) results)
                sendMsgPack handle QuerySet {
                  qsquery = Static [Prepare, Commit],
                  qsheaders = mempty,
                  qsparams = mempty}
                _ <- receiveMsgPack handle :: IO (Maybe ResultSet)
                return ()
      Just (ResultSetError err) ->
                putStrLn ("error: " ++ show err)
      Nothing ->
                putStrLn ("cannot parse response: " ++ show rep)
  let name2 = QuerySet {
                qsquery = Quit,
                qsheaders = mempty,
                qsparams = mempty
        }
  sendMsgPack handle name2

run2 ::  Bool -> [String] -> Query -> MapResultRow -> TranslationInfo -> IO ()
run2 showhdr hdr query params ps = do
    let vars = map Var hdr
    AbstractDatabase tdb <- transDB ps
    conn <- dbOpen tdb
    dbBegin conn
    ret <- runExceptT $ case query of
                          QAL qu -> run (fromList vars) qu params tdb conn
                          SQL qu -> run3 (fromList vars) [Execute qu] params tdb conn
    case ret of
      Left e -> putStrLn ("error: " ++ show e)
      Right pp -> do
        dbPrepare conn
        dbCommit conn
        putStr (pprint showhdr False vars pp)
    dbClose conn
