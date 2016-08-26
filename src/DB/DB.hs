{-# LANGUAGE ExistentialQuantification, MultiParamTypeClasses, TypeSynonymInstances, FlexibleInstances, FlexibleContexts, TypeFamilies, ScopedTypeVariables #-}
module DB.DB where

import DB.ResultStream
import FO.Data
import FO.Domain
import ListUtils
import Algebra.SemiBoundedLattice

import Prelude  hiding (lookup, null)
import Data.Map.Strict (Map, empty, insert, lookup, intersectionWith, delete, singleton)
import Data.List (intercalate, find, elem, union, intersect, sortBy)
import qualified Data.List as List
import Control.Monad.Except
import Control.Applicative ((<|>))
import Data.Convertible.Base
import Data.Maybe
import Data.Monoid  ((<>))
import Data.Tree
import Data.Conduit
import Control.Monad.Trans.Control
import Control.Concurrent.Async.Lifted
import Control.Monad.Trans.Resource
import qualified Data.Text as T
import System.Log.Logger
import Algebra.Lattice
import Data.Set (Set, fromList, toAscList, null, isSubsetOf, member)
import Data.Ord (comparing, Down(..))
import Data.Dynamic
import Data.Typeable

-- result value
data ResultValue = StringValue T.Text | IntValue Int | Null deriving (Eq , Ord, Show)

instance Num ResultValue where
    IntValue a + IntValue b = IntValue (a + b)
    fromInteger i = IntValue (fromInteger i)

instance Convertible ResultValue Expr where
    safeConvert (StringValue s) = Right (StringExpr s)
    safeConvert (IntValue i) = Right (IntExpr i)
    safeConvert v = Left (ConvertError (show v) "ResultValue" "Expr" "")

instance Convertible Expr ResultValue where
    safeConvert (StringExpr s) = Right (StringValue s)
    safeConvert (IntExpr i) = Right (IntValue i)
    safeConvert v = Left (ConvertError (show v) "Expr" "ResultValue" "")

-- result row
type MapResultRow = Map Var ResultValue

instance ResultRow MapResultRow where
    type ElemType MapResultRow = ResultValue
    transform _ vars2 map1 = foldr (\var map2 -> case lookup var map1 of
                                                        Nothing -> insert var Null map2
                                                        Just rv -> insert var rv map2) empty vars2
    ext v map1 = fromMaybe (error ("cannot find key " ++ show v)) (lookup v map1)
    ret = singleton

class SubstituteResultValue a where
    substResultValue :: MapResultRow -> a -> a

instance SubstituteResultValue Expr where
    substResultValue varmap expr@(VarExpr var) = case lookup var varmap of
        Nothing -> expr
        Just res -> convert res
    substResultValue _ expr = expr

instance SubstituteResultValue Atom where
    substResultValue varmap (Atom thesign theargs) = Atom thesign newargs where
        newargs = map (substResultValue varmap) theargs
instance SubstituteResultValue Lit where
    substResultValue varmap (Lit thesign theatom) = Lit thesign newatom where
        newatom = substResultValue varmap theatom


-- query
newtype Query = Query Formula

data Commit = Commit

checkQuery :: Query -> Except String ()
checkQuery (Query form) = checkFormula form

instance Show Query where
    show (Query  disjs) = show disjs

{-
    class A a b | a -> b
    class B a b | a -> b

    data C a = C a

    instance B a b => A (C a) b
-}

class IResultStream (ResultStreamType stmt) (ResourceT IO) => IDBStatement stmt where
    type ResultStreamType stmt
    dbStmtExec :: stmt -> ResultStreamType stmt -> ResultStreamType stmt
    dbStmtClose :: stmt -> IO ()

-- connection
class IDBConnection0 conn where
    dbClose :: conn -> IO ()
    dbBegin :: conn -> IO ()
    dbPrepare :: conn -> IO Bool
    dbCommit :: conn -> IO Bool
    dbRollback :: conn -> IO ()

class (IDBConnection0 conn, IDBStatement (StatementType conn), Show (QueryType conn), Typeable (QueryType conn)) => DBConnection conn where
    type QueryType conn
    type StatementType conn
    prepareQuery :: conn -> QueryType conn -> IO (StatementType conn)

data AbstractDBConnection row = forall conn. (IDBConnection conn, row ~ RowType (ResultStreamType (StatementType conn))) => AbstractDBConnection { unAbstractDBConnection :: conn }

instance IDBConnection0 (AbstractDBConnection row) where
    dbClose (AbstractDBConnection conn) = dbClose conn
    dbBegin (AbstractDBConnection conn) = dbBegin conn
    dbPrepare (AbstractDBConnection conn) = dbPrepare conn
    dbCommit (AbstractDBConnection conn) = dbCommit conn
    dbRollback (AbstractDBConnection conn) = dbRollback conn

-- database
class IDatabase0 db where
    getName :: db -> String
    getPreds :: db -> [Pred]
    -- determinateVars function is a function from a given list of determined vars to vars determined by this atom
    determinateVars :: db -> Set Var -> Atom -> Set Var
    supported :: db -> Formula -> Set Var -> Bool

class (IDatabase0 db, IDBConnection (ConnectionType db)) => IDatabase db where
    type ConnectionType db
    dbOpen :: db -> IO (ConnectionType db)
    translateQuery :: db -> Set Var -> Query -> Set Var -> QueryType (ConnectionType db)

doQuery :: (IDatabase db) => db -> Set Var -> Query -> [Var] -> ResultStreamType (StatementType (ConnectionType db)) -> ResultStreamType (StatementType (ConnectionType db))
doQuery db vars2 qu vars rs = do
        let vars' = fromList vars
        conn <- liftIO $ dbOpen db
        stmt <- liftIO $ prepareQuery conn (translateQuery db vars2 qu vars')
        res <- dbStmtExec stmt vars rs
        liftIO $ dbClose conn
        return res

-- https://wiki.haskell.org/Existential_type#Dynamic_dispatch_mechanism_of_OOP
data AbstractDatabase row = forall db conn stmt query. (IDatabase db, row ~ RowType (ResultStreamType (StatementType (ConnectionType db)))) => AbstractDatabase { unAbstractDatabase :: db }

instance IDatabase0 (AbstractDatabase row) where
    getName (AbstractDatabase db) = getName db
    getPreds (AbstractDatabase db) = getPreds db
    determinateVars (AbstractDatabase db) = determinateVars db
    supported (AbstractDatabase db) = supported db
