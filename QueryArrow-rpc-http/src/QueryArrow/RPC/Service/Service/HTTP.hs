{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE QuasiQuotes       #-}
{-# LANGUAGE TemplateHaskell   #-}
{-# LANGUAGE TypeFamilies      #-}

module QueryArrow.RPC.Service.Service.HTTP where

import QueryArrow.FO.Data
import QueryArrow.Syntax.Type
import QueryArrow.FO.TypeChecker
import QueryArrow.DB.DB hiding (Null)
import QueryArrow.DBMap
import QueryArrow.RPC.Config
import QueryArrow.Utils
import QueryArrow.Parser
import QueryArrow.DB.ResultStream

import Text.Parsec (runParser)
import Data.Set (fromList)
import Data.Map.Strict (Map, keys, toList, singleton)
import Prelude hiding (lookup)
import qualified Data.Set as Set
import Control.Exception (SomeException, try)
import Control.Monad.IO.Class (liftIO)
import Control.Monad.Trans.Resource
import System.Environment
import Data.Time
import Control.Monad.Except
import QueryArrow.Serialization
import Data.Aeson
import qualified Data.Text as T
import System.Log.Logger
import QueryArrow.Logging
import           Yesod
import QueryArrow.RPC.DB
import Control.Monad.Trans.Except
import QueryArrow.RPC.Service


data App = App {
    appParameters :: AbstractDatabase MapResultRow FormulaT
}

mkYesod "App" [parseRoutes|
/queryarrow QueryR GET POST
|]

instance Yesod App

runQuery :: (MonadIO m, MonadHandler m) => String -> AbstractDatabase MapResultRow FormulaT -> m Value
runQuery method db = do
    liftIO $ infoM "QA" ("REST client connected")
    case db of
        AbstractDatabase tdb -> do
            (_, conn) <- allocate (dbOpen tdb) dbClose
            t0 <- liftIO $ getCurrentTime
            ret <- case method of
                "get" -> do
                    liftIO $ infoM "QA" ("parsing get request")
                    (Just qu) <- lookupGetParam "qsquery"
                    (Just hdr) <- lookupGetParam "qsheaders"
                    liftIO $ infoM "QA" ("received REST query " ++ T.unpack qu)
                    liftIO $ runExceptT $ run (fromList (map Var (words (T.unpack hdr)))) (T.unpack qu) mempty tdb conn
                "post" -> do
                    liftIO $ infoM "QA" ("parsing post request")
                    req <- requireJsonBody
                    let qu = qsquery req
                    let hdr = qsheaders req
                    let par = qsparams req
                    liftIO $ infoM "QA" ("received REST query " ++ show qu)
                    liftIO $ case qu of
                        Dynamic qu ->
                          runExceptT $ run hdr qu par tdb conn
                        Static qu ->
                          runExceptT $ run3 hdr qu par tdb conn
                _ -> error ("unsupported method " ++ method)
            ret1 <- case ret of
                Left e ->
                    return (object [
                        "error" .= show e
                        ])
                Right rep ->
                    return (toJSON (resultSet rep))
            t1 <- liftIO $ getCurrentTime
            liftIO $ infoM "QA" (show (diffUTCTime t1 t0))
            liftIO $ infoM "QA" ("REST client disconnected")
            return ret1

getQueryR :: Handler TypedContent
getQueryR = selectRep $ do
    provideRep $ do
        app <- getYesod
        runQuery "get" (appParameters app)

postQueryR :: Handler TypedContent
postQueryR = selectRep $ do
    provideRep $ do
        app <- getYesod
        runQuery "post" (appParameters app)

data ServiceHTTPRPCService = ServiceHTTPRPCService

instance RPCService ServiceHTTPRPCService where
    startService _ db ps0 = do
        let ps = case fromJSON ps0 of
                    Error err -> error err
                    Success ps2 -> ps2
        warp (http_server_port ps) App {appParameters = db}
