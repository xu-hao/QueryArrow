{-# LANGUAGE TypeFamilies, MultiParamTypeClasses, FunctionalDependencies, ExistentialQuantification, FlexibleInstances, OverloadedStrings,
   RankNTypes, FlexibleContexts, GADTs, DeriveGeneric, PatternSynonyms #-}
module InMemory where

import ResultStream
import FO.Data
import FO.Domain
import Rewriting
import Config
import Parser
import DBQuery
import Utils
import DB

import Prelude  hiding (lookup)
import Data.Map.Strict (Map, (!), empty, member, insert, foldrWithKey, foldlWithKey, alter, lookup, fromList, toList, unionWith, unionsWith, intersectionWith, elems, delete, singleton, keys, filterWithKey)
import qualified Data.Map.Strict
import Data.List ((\\), intercalate, union)
import Control.Monad.Trans.State.Strict (StateT, evalStateT,evalState, get, put, State, runState   )
import Control.Applicative ((<$>), liftA2)
import Data.Convertible.Base
import Control.Monad.Logic
import Control.Monad.Except
import Control.Monad.Trans.Resource
import Data.Maybe
import qualified Data.ByteString.Lazy as B
import qualified Data.ByteString.Lazy.Char8 as B8
import Text.ParserCombinators.Parsec hiding (State)
import Text.Regex.TDFA ((=~))
import Data.Text (Text, unpack)
import Control.Concurrent (threadDelay)
import Algebra.Lattice
import Algebra.SemiBoundedLattice
import Data.Set (Set, toAscList)
import qualified Data.Set as Set
import Data.IORef


class NoPreparedStatementDBStatement stmt where
    npsdbStmtClose :: stmt -> IO ()

class (DBConnection0 conn, NoPreparedStatementDBStatement (NPSStatementType conn), IResultStream (NPSResultStreamType conn) (ResourceT IO)) => NoPreparedStatementDBConnection conn where
    type NPSResultStreamType conn
    type NPSStatementType conn
    type NPSQueryType conn
    npsdbStmtExec :: NPSQueryType conn -> NPSResultStreamType conn -> (NPSResultStreamType conn, NPSStatementType conn)

data NoPreparedStatement_ conn query = NoPreparedStatement_ conn query

instance (NoPreparedStatementDBConnection conn, query ~ NPSQueryType conn) => PreparedDBStatement (NoPreparedStatement_ conn query) where
    type ResultStreamType (NoPreparedStatement_ conn query) = NPSResultStreamType conn

instance (NoConnectionDB db) => DBConnection (NoConnection db m) where
    type QueryType (NoConnection db m) = Query
    type StatementType (NoConnection db m) = NoConnectionStmt db
    prepareQuery (NoConnection db) retvars qu vars = return (NoConnectionStmt db retvars qu vars)

class NoConnectionDB db where
    noConnectionDBStmtExec :: db -> Set Var -> Query -> Set Var -> ResultStream (ResourceT IO) MapResultRow -> ResultStream (ResourceT IO) MapResultRow

instance (NoConnectionDB db) => DBStatement (NoConnectionStmt db) where
    type RowType (NoConnectionStmt db) = MapResultRow
    dbStmtClose _ = return ()
    dbStmtExec (NoConnectionStmt db retvars qu vars) _ = noConnectionDBStmtExec db retvars qu vars

newtype NoConnectionDB_ db = NoConnectionDB_ db
instance (Database0 db) => (Database0 (NoConnectionDB_ db)) where
    getName (NoConnectionDB_ db) = getName db
    getPreds (NoConnectionDB_ db) = getPreds db
    determinateVars (NoConnectionDB_ db) = determinateVars db
    supported (NoConnectionDB_ db) = supported db

instance (NoConnectionDB db, Database0 db) => Database (NoConnectionDB_ db) where
    type ConnectionType (NoConnectionDB_ db) = NoConnection db (ResourceT IO)
    translateQuery _ _ qu _ = qu
    dbOpen (NoConnectionDB_ db) = return (NoConnection db)
