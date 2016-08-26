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

-- interface

class (IDatabase0 db) => INoConnectionDBDatabase db where
    type NoConnectionQueryType db
    type NoConnectionResultStreamType db
    noConnectionTranslateQuery :: db -> Set Var -> Query -> Set Var -> NoConnectionQueryType db
    noConnectionDBStmtExec :: db -> NoConnectionQueryType db -> NoConnectionResultStreamType db -> NoConnectionResultStreamType db

-- instance for IDatabase

newtype NoConnectionDatabase db = NoConnectionDatabase db

instance (IDatabase0 db) => (IDatabase0 (NoConnectionDatabase db)) where
    getName (NoConnectionDatabase db) = getName db
    getPreds (NoConnectionDatabase db) = getPreds db
    determinateVars (NoConnectionDatabase db) = determinateVars db
    supported (NoConnectionDatabase db) = supported db

instance (INoConnectionDatabase db) => IDatabase (NoConnectionDatabase db) where
    type ConnectionType (NoConnectionDatabase db) = NoConnectionDBConnection db (ResourceT IO)
    translateQuery _ retvars qu vars = noConnectionTranslateQuery retvars qu vars
    dbOpen (NoConnectionDatabase db) = return (NoConnectionDBConnection db)

-- instance for IDBConnection
newtype NoConnectionDBConnection db = NoConnectionDBConnection db

instance IDBConnection0 (NoConnectionDBConnection db) where
    dbClose _ = return ()
    dbBegin _ = return ()
    dbCommit _ = return True
    dbPrepare _ = return True
    dbRollback _ = return ()

instance (INoConnectionDatabase db) => IDBConnection (NoConnectionDBConnection db) where
    type QueryType (NoConnectionDBConnection db) = NoConnectionQueryType db
    type StatementType (NoConnectionDBConnection db) = NoConnectionDBStatement db
    prepareQuery (NoConnectionDatabase db) qu = return (NoConnectionDBStatement db qu)

-- instance for IDBStatement

data NoConnectionDBStatement db = NoConnectionDBStatement db (NoConnectionQueryType db)

instance (INoConnectionDatabase db) => IDBStatement (NoConnectionDBStatement db) where
    type ResultStreamType (NoConnectionStmt db) = NoConnectionResultStreamType db
    dbStmtClose _ = return ()
    dbStmtExec (NoConnectionDBStatement db  qu ) = noConnectionDBStmtExec db  qu
