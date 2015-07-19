{-# LANGUAGE MonadComprehensions, DeriveGeneric #-}
module Mpostgres where

import FO
import Parser
import SQL.SQL
import SQL.HDBC
import SQL.HDBC.Sqlite3
import SQL.HDBC.PostgreSQL
import DBQuery
import ICAT

import Prelude hiding (lookup)
import Data.Map.Strict (empty, fromList,lookup)
import Text.Parsec (runParser)
import Data.Functor.Identity
import Control.Monad.Trans.State.Strict (evalStateT)
import Database.HDBC
import System.Environment
import Data.List (intercalate, transpose)
import Data.Aeson
import GHC.Generics
import Control.Applicative ((<$>))
import qualified Data.ByteString.Lazy as B

getDB :: ICATDBConnInfo -> IO (Database DBAdapterMonad MapResultRow, Query -> String)
getDB ps = do
    conn <- dbConnect (PostgreSQLDBConnInfo (DBConnInfo (db_host ps) (db_port ps) (db_username ps) (db_password ps) (db_name ps)))
    let db = makeICATSQLDBAdapter conn
    return (Database db, show . translate db)
                        
