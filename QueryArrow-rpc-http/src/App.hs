{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE QuasiQuotes       #-}
{-# LANGUAGE TemplateHaskell   #-}
{-# LANGUAGE TypeFamilies      #-}

module Main where

import QueryArrow.FO.Data
import QueryArrow.DB.DB hiding (Null)
import QueryArrow.DBMap
import QueryArrow.Config
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
import Control.Monad.Trans.Either

data App = App {
    appParameters :: TranslationInfo
}

mkYesod "App" [parseRoutes|
/queryarrow QueryR GET POST
|]

instance Yesod App

runQuery :: (MonadIO m, MonadHandler m) => String -> TranslationInfo -> m Value
runQuery method ps = do
    liftIO $ infoM "QA" ("REST client connected")
    (AbstractDatabase tdb) <- liftIO $ transDB "tdb" ps
    liftIO $ infoM "QA" ("database loaded")
    (_, conn) <- allocate (dbOpen tdb) dbClose
    t0 <- liftIO $ getCurrentTime
    ret <- case method of
        "get" -> do
            liftIO $ infoM "QA" ("parsing get request")
            (Just qu) <- lookupGetParam "qsquery"
            (Just hdr) <- lookupGetParam "qsheaders"
            liftIO $ infoM "QA" ("received REST query " ++ T.unpack qu)
            liftIO $ runEitherT $ run (fromList (map Var (words (T.unpack hdr)))) (T.unpack qu) mempty tdb conn
        "post" -> do
            liftIO $ infoM "QA" ("parsing post request")
            req <- requireJsonBody
            let qu = qsquery req
            let hdr = qsheaders req
            let par = qsparams req
            liftIO $ infoM "QA" ("received REST query " ++ show qu)
            liftIO $ case qu of
                Dynamic qu ->
                  runEitherT $ run hdr qu par tdb conn
                Static qu ->
                  runEitherT $ run3 hdr qu par tdb conn
        _ -> error ("unsupported method " ++ method)
    ret1 <- case ret of
        Left e ->
            return (object [
                "error" .= e
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


main :: IO ()
main = do
    setup
    args2 <- getArgs
    ps <- getConfig (head args2)
    warp (server_port ps) App {appParameters = ps}
