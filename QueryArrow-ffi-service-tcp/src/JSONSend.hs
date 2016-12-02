{-# LANGUAGE OverloadedStrings #-}
import Prelude hiding (lookup)
import Control.Monad
import Data.String
import System.IO
import System.Exit
import System.Environment
import QueryArrow.Serialization
import Network
import QueryArrow.RPC.Message
import QueryArrow.FO.Data
import Data.Set (fromList)
-- import Data.Serialize

import QueryArrow.Utils

main :: IO ()
main = do
    args <- getArgs
    when (length args /= 4) $ do
        hPutStrLn stderr "usage: prompt <ip> <port> <query> <headers>"
        exitFailure
    let addr = head args
    let port = read (args !! 1)
    handle <- connectTo addr (PortNumber port)
    let hdr = map Var (words (fromString (args !! 3)))
    let qu = fromString (args !! 2)
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
