module Main where

import Prelude hiding (lookup)
import Control.Monad
import Data.Monoid
import Data.String
import System.IO
import System.Exit
import System.Environment
import Data.Map.Strict hiding (map)
import Data.List (intercalate, transpose)
import Data.Aeson
import QueryArrow.Serialization
import Network.HTTP.Conduit
import HTTP.ElasticSearchUtils
import qualified Data.Text as T
import Data.Text.Encoding

main :: IO ()
main = do
    args <- getArgs
    when (length args < 2) $ do
        hPutStrLn stderr "usage: prompt <method> <url> [<data>]"
        exitFailure
    let method = head args
    let addr = args !! 1
    case method of
        "get" -> do
            res <- get addr
            -- BL8.putStrLn res
        "post" -> do
            let postdata = args !! 2
            let postdatabs = encodeUtf8 (T.pack postdata)
            res <- post addr (RequestBodyBS postdatabs)
            -- BL8.putStrLn res
        _ -> error "unsupported method"
