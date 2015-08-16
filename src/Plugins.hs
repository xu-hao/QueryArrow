{-# LANGUAGE MonadComprehensions, ForeignFunctionInterface #-}
module Plugins where

import FO
import FO.Data
import Parser
import DBQuery
import ICAT
import ResultStream
import FO.Parser
import FO.Config

import Prelude hiding (lookup)
import Data.Map.Strict (empty, lookup)
import Text.Parsec (runParser)
import Control.Monad.IO.Class (liftIO)
import Control.Monad.Trans.State.Strict
import Control.Monad.Trans.Class (lift)
import System.Environment
import Data.List (intercalate, transpose)
import Data.Aeson
import Control.Applicative ((<$>))
import qualified Data.ByteString.Lazy as B
import qualified Data.ByteString.Lazy.Char8 as B8
import System.Plugins.Load
import Data.Either.Utils (fromEither)
import Data.Maybe (fromMaybe)
import Foreign
import Foreign.C
import Foreign.Marshal

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
