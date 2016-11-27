{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE QuasiQuotes       #-}
{-# LANGUAGE TemplateHaskell   #-}
{-# LANGUAGE TypeFamilies      #-}

module Main where

import FO.Data
import DB.DB hiding (Null)
import DBMap
import Config
import Utils
import Parser
import DB.ResultStream

import Text.Parsec (runParser)
import Data.Map.Strict (Map, keys, fromList, toList, singleton)
import Prelude hiding (lookup)
import qualified Data.Set as Set
import Control.Exception (SomeException, try)
import Control.Monad.IO.Class (liftIO)
import Control.Monad.Trans.Resource
import System.Environment
import Data.Time
import Control.Monad.Except
import Serialization
import Data.Aeson
import qualified Data.Text as T
import System.Log.Logger
import Logging
import           Yesod

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
    (qu, user, zone, hdr) <- case method of
        "get" -> do
            liftIO $ infoM "QA" ("parsing get request")
            (Just qu) <- lookupGetParam "qsquery"
            (Just user) <- lookupGetParam "qsuser"
            (Just zone) <- lookupGetParam "qszone"
            (Just hdr) <- lookupGetParam "qsheaders"
            return (T.unpack qu, T.unpack user, T.unpack zone, words (T.unpack hdr))
        "post" -> do
            req <- requireJsonBody
            let qu = qsquery req
            let user = qsuser req
            let zone = qszone req
            let hdr = qsheaders req
            return (qu, user, zone, hdr)
        _ -> error ("unsupported method " ++ method)
    liftIO $ infoM "QA" ("received REST query " ++ qu)
    ret <- liftIO $ runResourceT $ do
        (_, conn) <- allocate (dbOpen tdb) dbClose
        t0 <- liftIO $ getCurrentTime
        ret <- liftIO $ try (liftIO $ run3 hdr qu tdb conn user zone)
        ret1 <- case ret of
            Left e ->
                return (object [
                    "error" .= show (e :: SomeException)
                    ])
            Right (hdr, rep) ->
                return (toJSON (resultSet hdr rep))
        t1 <- liftIO $ getCurrentTime
        liftIO $ infoM "QA" (show (diffUTCTime t1 t0))
        return ret1
    liftIO $ infoM "QA" ("REST client disconnected")
    return ret

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
    return (case r of
        Right rows ->  (hdr, map (\row -> fromList (map (\(Var v,r) -> (v, show r)) (toList row))) rows)
        Left e ->  (["error"], [singleton "error" (show e)]))


main :: IO ()
main = do
    setup
    args2 <- getArgs
    ps <- getConfig (head args2)
    warp (server_port ps) App {appParameters = ps}
