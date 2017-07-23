{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE QuasiQuotes       #-}
{-# LANGUAGE TemplateHaskell   #-}
{-# LANGUAGE TypeFamilies      #-}

module QueryArrow.RPC.Service.Service.HTTP where

import QueryArrow.Syntax.Data
import QueryArrow.Syntax.Types
import QueryArrow.DB.DB hiding (Null)
import QueryArrow.DBMap
import QueryArrow.RPC.Config
import QueryArrow.Utils
import QueryArrow.Parser
import QueryArrow.Semantics.ResultStream
import QueryArrow.Semantics.ResultRow.VectorResultRow
import QueryArrow.Semantics.ResultValue.AbstractResultValue
import QueryArrow.Semantics.ResultSet.VectorResultSetTransformer

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
import QueryArrow.RPC.Service


data App = App {
    appParameters :: AbstractDatabase (ResultSetTransformer AbstractResultValue) (VectorResultRow AbstractResultValue) FormulaT
}

mkYesod "App" [parseRoutes|
/queryarrow QueryR GET POST
|]

instance Yesod App

runQuery :: (MonadIO m, MonadHandler m) => String -> AbstractDatabase (ResultSetTransformer AbstractResultValue) (VectorResultRow AbstractResultValue) FormulaT -> m Value
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
                    liftIO $ runEitherT $ run (fromList (map Var (words (T.unpack hdr)))) (T.unpack qu) mempty mempty tdb conn
                "post" -> do
                    liftIO $ infoM "QA" ("parsing post request")
                    req <- requireJsonBody
                    let qu = qsquery req
                    let hdr = qsheaders req
                    let hdr0 = qsparamheaders req
                    let par = qsparams req
                    liftIO $ infoM "QA" ("received REST query " ++ show qu)
                    liftIO $ case qu of
                        Dynamic qu ->
                          runEitherT $ run hdr qu hdr0 par tdb conn
                        Static qu ->
                          runEitherT $ run3 hdr qu hdr0 par tdb conn
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

data ServiceHTTPRPCService = ServiceHTTPRPCService

instance RPCService ServiceHTTPRPCService where
    startService _ db ps0 = do
        let ps = case fromJSON ps0 of
                    Error err -> error err
                    Success ps2 -> ps2
        warp (http_server_port ps) App {appParameters = db}
