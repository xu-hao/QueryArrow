{-# LANGUAGE TypeFamilies, MultiParamTypeClasses, FunctionalDependencies, ExistentialQuantification, FlexibleInstances, OverloadedStrings,
   RankNTypes, FlexibleContexts, GADTs, DeriveGeneric, PatternSynonyms #-}
module DB.NoPreparedStatement where

import DB.ResultStream
import FO.Data
import FO.Domain
import Utils
import DB.DB

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

-- interface

class (IDBConnection0 conn) => INPSDBConnection conn where
    type NPSResultStreamType conn
    type NPSStatementType conn
    type NPSQueryType conn
    npsdbStmtExec :: conn -> NPSQueryType conn -> NPSResultStreamType conn -> NPSResultStreamType conn

-- instance for IDBConnection

newtype NPSDBConnection conn = NPSDBConnection conn

instance (IDBConnection0 conn) => IDBConnection0 (NPSDBConnection conn) where
    dbClose (NPSDBConnection conn) = dbClose conn
    dbBegin (NPSDBConnection conn) = dbBegin conn
    dbCommit (NPSDBConnection conn) = dbCommit conn
    dbPrepare (NPSDBConnection conn) = dbPrepare conn
    dbRollback (NPSDBConnection conn) = dbRollback conn

instance (INPSDBConnection conn) => IDBConnection (NPSDBConnection conn) where
    type QueryType (NPSDBConnection conn) = NPSQueryType conn
    type StatementType (NPSDBConnection conn) = NPSStatementType conn
    prepareQuery conn query = NPSDBStatement conn query

-- instance for IDBStatement

data NPSDBStatement conn query = NPSDBStatement conn query

instance (INPSDBConnection conn, IResultStream (NPSResultStreamType conn) (ResourceT IO), query ~ NPSQueryType conn) => IPreparedDBStatement (NPSDBStatement conn query) where
    type ResultStreamType (NPSDBStatement conn query) = NPSResultStreamType conn
    dbStmtExec (NPSDBStatement conn query) = npsdbStmtExec conn query
    dbStmtClose _ = return ()
