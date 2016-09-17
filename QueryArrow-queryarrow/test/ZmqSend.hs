{-# LANGUAGE OverloadedStrings #-}
import Prelude hiding (lookup)
import Control.Monad
import Control.Monad.IO.Class
import Data.Monoid
import Data.String
import System.IO
import System.Exit
import System.Environment
import Data.Map.Strict hiding (map)
import Data.List (intercalate, transpose)
import Data.Aeson
import Data.ByteString.Lazy (fromStrict)
import qualified Data.ByteString.Char8 as B8
import Serialization
import Network.JsonRpc
import Data.Conduit.Network
import Control.Monad.Logger.HSLogger
import Data.Text (Text)
-- import Data.Serialize

import Utils

data ClientRequest = ClientRequest Text Value
instance ToRequest ClientRequest where
    requestMethod (ClientRequest met _) = met
    requestIsNotif _ = False

instance ToJSON ClientRequest where
    toJSON (ClientRequest _ val) = val

main :: IO ()
main = do
    args <- getArgs
    when (length args < 4) $ do
        hPutStrLn stderr "usage: prompt <ip> <port> <username> <userzone>"
        exitFailure
    let addr = head args
    let port = read (args !! 1)
    jsonRpcTcpClient
        V2
        True
        (clientSettings port (B8.pack addr))
        (do
            if length args == 5
                then do
                    let name = QuerySet {
                            qsuser = fromString (args !! 2),
                            qszone = fromString (args !! 3),
                            qsquery = fromString (args !! 4),
                            qsheaders = words (fromString (args !! 5))
                            }
                    (ResultSet hdr results) <- run name
                    liftIO $ putStrLn (pprint3 ((hdr, results) :: ([String], [[String]])))
                else
                    let client = do
                            line <- liftIO $ fromString <$> getLine
                            if line == "exit"
                                then return ()
                                else do
                                    line2 <- liftIO $ fromString <$> getLine
                                    let name = QuerySet {
                                            qsuser = fromString (args !! 2),
                                            qszone = fromString (args !! 3),
                                            qsquery = line,
                                            qsheaders = words line2
                                            }
                                    (ResultSet hdr results) <- run name
                                    liftIO $ putStrLn (pprint3 ((hdr, results) :: ([String], [[String]])))
                                    client in
                    client)

run :: QuerySet -> JsonRpcT IO ResultSet
run json = do
  rep <- sendRequest (ClientRequest "QuerySet" (toJSON json))
  case rep of
      Nothing ->
          return (ResultSet ["error"] [["error decoding reply from server: " ++ show rep]])
      Just rep1 -> case rep1 of
                        (Left err) ->
                            return (ResultSet ["error"] [["error decoding reply from server: " ++ show err]])
                        (Right json) -> case fromJSON json of
                                            (Error errmsg) ->
                                                return (ResultSet ["error"] [["error decoding reply from server: " ++ errmsg]])
                                            (Success rs) ->
                                                return rs
