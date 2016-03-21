{-# LANGUAGE OverloadedStrings #-}
import Prelude hiding (lookup)
import Control.Monad
import Data.Monoid
import Data.String
import System.IO
import System.Exit
import System.Environment
import System.ZMQ4.Monadic
import Data.Map.Strict hiding (map)
import Data.List (intercalate, transpose)
import Data.Aeson
import Data.ByteString.Lazy (fromStrict)
import Serialization
-- import Data.Serialize

import Utils



main :: IO ()
main = do
    args <- getArgs
    when (length args < 2) $ do
        hPutStrLn stderr "usage: prompt <address> <username>"
        exitFailure
    let addr = head args
        name = fromString (args !! 1) <> ":"
    runZMQ $ do
        req <- socket Req
        connect req addr
        if (length args == 3)
            then
                run req name (fromString (args !! 2))
            else
                forever $ do
                    line <- liftIO $ fromString <$> getLine
                    run req name line

run req name line = do
    send req [] (name <> line)
    rep <- receive req
    let a = case decode (fromStrict rep) of
                Nothing -> "error decoding reply from server: " ++ show rep
                Just (ResultSet hdr results) -> pprint3 ((hdr, results) :: ([String], [[String]]))
    liftIO $ putStrLn a
