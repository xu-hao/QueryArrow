{-# LANGUAGE TypeSynonymInstances, FlexibleInstances, MultiParamTypeClasses, FlexibleContexts, RankNTypes, FunctionalDependencies, GADTs #-}
module DBQuery where

import FO

import Prelude hiding (lookup, foldl)
import Control.Monad.IO.Class (liftIO)
import Control.Monad.Trans.State.Strict (StateT, get)
import Data.Map.Strict (Map, foldl, empty)
import Control.Applicative ((<$>))


data DBConnInfo = DBConnInfo {
    dbHost :: String, 
    dbPort :: Int,
    dbUser :: String, 
    dbPassword :: String, 
    dbDB :: String
    }

-- the cache to prepared statements

newtype DBAdapterState = DBAdapterState {
    preparedStatementMap :: Map [Var] PreparedStatement
    }

type DBAdapterMonad = StateT DBAdapterState IO

class PreparedStatement_ stmt where
    execWithParams :: stmt -> [Expr] -> ResultStream DBAdapterMonad seed MapResultRow
    closePreparedStatement :: stmt -> IO ()

data PreparedStatement = forall stmt. PreparedStatement_ stmt => PreparedStatement {
    unPreparedStatement :: stmt
}

class DBConnection conn query | conn -> query where
    execStatement :: conn -> query -> ResultStream DBAdapterMonad seed MapResultRow
    prepareStatement :: conn -> query -> IO PreparedStatement 
    dbClose :: conn -> IO ()    

class DBConnection  conn query => QueryDB connInfo conn query | connInfo -> conn query, conn -> connInfo query where
    dbConnect :: connInfo -> IO conn
    
class Translate db row query | db -> row query where
    translate :: db -> Query -> query
    translateWithParams :: db -> Query -> row -> (query, [Var])

liftDBAdapter :: DBAdapterMonad a -> ResultStream DBAdapterMonad seed a
liftDBAdapter a = ResultStream (\iteratee seed -> a >>= \b -> runResultStream (return b) iteratee seed)

-- call this to clear all dbs
closeAllCachedPreparedStatements :: DBAdapterMonad ()
closeAllCachedPreparedStatements = do
    (DBAdapterState cache) <- get
    let stmts = foldl (\stmts2 stmt -> stmts2 ++ [stmt]) [] cache
    liftIO $ mapM_ (\ ps -> case ps of PreparedStatement ps_ -> closePreparedStatement ps_) stmts