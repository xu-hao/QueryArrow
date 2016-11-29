{-# LANGUAGE OverloadedStrings #-}
import Prelude hiding (lookup)
import Control.Monad
import Data.String
import System.IO
import System.Exit
import System.Environment
import Serialization
import Network.Socket
import QueryArrow.RPC.Message
import FO.Data
import Data.Set (fromList)

import Utils

main :: IO ()
main = do
    args <- getArgs
    when (length args /= 3) $ do
        hPutStrLn stderr "usage: prompt <addr> <query> <headers>"
        exitFailure
    let addr = head args
    sock <- socket AF_UNIX Stream defaultProtocol
    connect sock (SockAddrUnix addr)
    handle <- socketToHandle sock ReadWriteMode
    let hdr = map Var (words (fromString (args !! 2)))
    let qu = fromString (args !! 1)
    let name = QuerySet {
                  qsquery = Dynamic qu,
                  qsheaders = fromList hdr,
                  qsparams = mempty
                  }
    sendMsg handle name
    rep <- receiveMsg handle
    case rep of
        Just (ResultSet err results) ->
            if null err
              then
                  putStrLn (pprint hdr results)
              else
                  putStrLn ("error: " ++ err)
        Nothing ->
            putStrLn ("cannot parse response: " ++ show rep)
    let name2 = QuerySet {
                  qsquery = Quit,
                  qsheaders = mempty,
                  qsparams = mempty
                  }
    sendMsg handle name2
