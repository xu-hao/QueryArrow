{-# LANGUAGE MonadComprehensions, ForeignFunctionInterface #-}
module Plugins where

import FO
import FO.Data
import DBQuery
import ICAT
import FO.Parser
import FO.Config

import Control.Applicative ((<$>))
import qualified Data.ByteString.Lazy as B
import qualified Data.ByteString.Lazy.Char8 as B8
import System.Plugins.Load

-- the plugin must be compiled with -fPIC -dynamic -shared
getDB :: ICATDBConnInfo -> IO (Database DBAdapterMonad MapResultRow, Query -> String, Insert->String    )
getDB ps = do
    let objname = catalog_database_type ps ++ ".o"
    print ("loading" ++ objname)
    getDBFuncLoadStatus <- load objname ["."] [] "getDB"
    case getDBFuncLoadStatus of
        LoadSuccess _ getDBFunc -> getDBFunc ps
        LoadFailure err -> error (show err)

-- the plugin must be compiled with -fPIC -dynamic -shared
getVerifier :: PredMap -> VerificationInfo -> IO (TheoremProver, [Input])
getVerifier predmap ps = do
    let objname = verifier_type ps ++ ".o"
    print ("loading" ++ objname)
    getVerifierFuncLoadStatus <- load objname ["."] [] "getVerifier"
    case getVerifierFuncLoadStatus of
        LoadSuccess _ getVerifierFunc -> do
            verifier <- getVerifierFunc ps
            d0 <- B8.unpack <$> B.readFile (rule_file_path ps)
            let d = parseTPTP predmap d0
            return (verifier, d)
        LoadFailure err -> error (show err)
