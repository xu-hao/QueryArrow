{-# LANGUAGE ExistentialQuantification, MultiParamTypeClasses, TypeSynonymInstances, FlexibleInstances, FlexibleContexts, TypeFamilies, ScopedTypeVariables #-}
module QueryArrow.DB.DB where

import QueryArrow.DB.ResultStream
import QueryArrow.FO.Data

import Prelude  hiding (lookup, null)
import Data.Map.Strict (Map, empty, insert, lookup, singleton)
import Control.Monad.Except
import Data.Convertible.Base
import Data.Maybe
import Control.Monad.Trans.Resource
import Control.Monad.Trans.Reader
import qualified Data.Text as T
import Data.Set (Set)
import System.Log.Logger (errorM, noticeM)
import Control.Applicative ((<|>))
import Control.Exception (catch, SomeException)
import QueryArrow.FO.Types

-- result value
data ResultValue = StringValue T.Text | IntValue Int | Null deriving (Eq , Ord, Show, Read)

instance Num ResultValue where
    IntValue a + IntValue b = IntValue (a + b)
    fromInteger i = IntValue (fromInteger i)

instance Fractional ResultValue where
    IntValue a / IntValue b = IntValue (a `quot` b)

instance Convertible ResultValue Expr where
    safeConvert (StringValue s) = Right (StringExpr s)
    safeConvert (IntValue i) = Right (IntExpr i)
    safeConvert Null = Right (NullExpr)
    safeConvert v = Left (ConvertError (show v) "ResultValue" "Expr" "")

instance Convertible Expr ResultValue where
    safeConvert (StringExpr s) = Right (StringValue s)
    safeConvert (IntExpr i) = Right (IntValue i)
    safeConvert v = Left (ConvertError (show v) "Expr" "ResultValue" "")

typeOf :: ResultValue -> CastType
typeOf (IntValue _) = NumberType
typeOf (StringValue _) = TextType
typeOf (Null) = error "typeOf: null value"

-- result row
type MapResultRow = Map Var ResultValue

instance IResultRow MapResultRow where
    type ElemType MapResultRow = ResultValue
    transform vars2 map1 = foldr (\var map2 -> case lookup var map1 of
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

data Command = Begin | Prepare | Commit | Rollback | Execute Formula

{-
    class A a b | a -> b
    class B a b | a -> b

    data C a = C a

    instance B a b => A (C a) b
-}

type DBResultStream = ResultStream (ResourceT IO)

class IDBStatement stmt where
    type RowType stmt
    dbStmtExec :: stmt -> DBResultStream (RowType stmt) -> DBResultStream (RowType stmt)
    dbStmtClose :: stmt -> IO ()

data AbstractDBStatement row = forall stmt. (IDBStatement stmt, row ~ RowType stmt) => AbstractDBStatement {unAbstractDBStatement :: stmt}


-- connection
class IDBConnection0 conn where
    dbClose :: conn -> IO ()
    dbBegin :: conn -> IO ()
    dbPrepare :: conn -> IO ()
    dbCommit :: conn -> IO ()
    dbRollback :: conn -> IO ()

class (IDBConnection0 conn, IDBStatement (StatementType conn)) => IDBConnection conn where
    type QueryType conn
    type StatementType conn
    prepareQuery :: conn -> QueryType conn -> IO (StatementType conn)

data AbstractDBConnection = forall conn. (IDBConnection conn) => AbstractDBConnection { unAbstractDBConnection :: conn }

instance IDBConnection0 AbstractDBConnection where
    dbClose (AbstractDBConnection conn) = dbClose conn
    dbBegin (AbstractDBConnection conn) = dbBegin conn
    dbPrepare (AbstractDBConnection conn) = dbPrepare conn
    dbCommit (AbstractDBConnection conn) = dbCommit conn
    dbRollback (AbstractDBConnection conn) = dbRollback conn

-- database
class IDatabase0 db where
    type DBFormulaType db
    getName :: db -> String
    getPreds :: db -> [Pred]
    supported :: db -> Set Var -> DBFormulaType db -> Set Var -> Bool

class IDatabase0 db => IDatabase1 db where
    type DBQueryType db
    translateQuery :: db -> VarTypeMap -> DBFormulaType db -> VarTypeMap -> IO (DBQueryType db)

class (IDBConnection (ConnectionType db)) => IDatabase2 db where
    data ConnectionType db
    dbOpen :: db -> IO (ConnectionType db)

class (IDatabase0 db, IDatabase1 db, IDatabase2 db, DBQueryType db ~ QueryType (ConnectionType db)) => IDatabase db where

-- https://wiki.haskell.org/Existential_type#Dynamic_dispatch_mechanism_of_OOP
data AbstractDatabase row form = forall db. (IDatabase db, row ~ RowType (StatementType (ConnectionType db)), form ~ DBFormulaType db) => AbstractDatabase { unAbstractDatabase :: db }

instance IDatabase0 (AbstractDatabase row form) where
    type DBFormulaType (AbstractDatabase row form) = form
    getName (AbstractDatabase db) = getName db
    getPreds (AbstractDatabase db) = getPreds db
    supported (AbstractDatabase db) = supported db

doQuery :: (IDatabase db) => db -> VarTypeMap -> DBFormulaType db -> VarTypeMap -> DBResultStream (RowType (StatementType (ConnectionType db))) -> DBResultStream (RowType (StatementType (ConnectionType db)))
doQuery db vars2 qu vars rs =
        bracketPStream (dbOpen db) dbClose (\conn -> doQueryWithConn db conn vars2 qu vars rs)

doQueryWithConn :: (IDatabase db) => db -> ConnectionType db -> VarTypeMap -> DBFormulaType db -> VarTypeMap -> DBResultStream (RowType (StatementType (ConnectionType db))) -> DBResultStream (RowType (StatementType (ConnectionType db)))
doQueryWithConn db conn vars2 qu vars rs = do
  qu' <- liftIO $ translateQuery db vars2 qu vars
  stmt <- liftIO $ prepareQuery conn qu'
  dbStmtExec stmt rs <|> do
      liftIO $ catch (do
                    dbCommit conn
                    noticeM "QA" "doQuery: commit succeeded") (\e -> errorM "QA" ("doQuery: commit failed with exception " ++ show (e :: SomeException)))
      emptyResultStream

newtype QueryTypeIso conn = QueryTypeIso (QueryType conn)
newtype DBQueryTypeIso db = DBQueryTypeIso (DBQueryType db)
