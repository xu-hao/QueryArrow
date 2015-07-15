{-# LANGUAGE TypeSynonymInstances, FlexibleInstances, MultiParamTypeClasses, FlexibleContexts, RankNTypes, FunctionalDependencies #-}
module DBQuery where

import FO

import Prelude hiding (lookup, foldl)
import Control.Monad.IO.Class (liftIO)
import Control.Monad.Trans.State.Strict (StateT, get)
import Data.Map.Strict (Map, foldl, empty)


data DBConnInfo = DBConnInfo {
    dbHost :: String, 
    dbUser :: String, 
    dbPassword :: String, 
    dbDB :: String
    }

-- the cache to prepared statements
newtype DBAdapterState stmt expr = DBAdapterState {
    preparedStatementMap :: Map [Var] (stmt expr)
    }

type DBAdapterMonad stmt expr = StateT (DBAdapterState stmt expr) IO

class PreparedStatement stmt expr where
    execWithParams :: stmt expr -> [expr] -> ResultStream (DBAdapterMonad stmt expr) seed MapResultRow
    closePreparedStatement :: stmt expr -> IO ()

class PreparedStatement stmt expr => DBConnection conn stmt query expr | conn -> stmt query expr where
    execStatement :: conn -> query -> ResultStream (DBAdapterMonad stmt expr) seed MapResultRow
    prepareStatement :: conn -> query -> IO (stmt expr) 
    dbClose :: conn -> IO ()    

class DBConnection  conn stmt query expr => QueryDB connInfo conn stmt query expr | connInfo -> conn stmt query expr, conn -> connInfo stmt query expr where
    dbConnect :: connInfo -> IO conn

liftDBAdapter :: PreparedStatement stmt expr => DBAdapterMonad stmt expr a -> ResultStream (DBAdapterMonad stmt expr) seed a
liftDBAdapter a = ResultStream (\iteratee seed -> a >>= \b -> runResultStream (return b) iteratee seed)

-- call this to clear all dbs
closeAllCachedPreparedStatements :: PreparedStatement stmt expr => DBAdapterMonad stmt expr ()
closeAllCachedPreparedStatements = do
    (DBAdapterState cache) <- get
    let stmts = foldl (\stmts2 stmt -> stmts2 ++ [stmt]) [] cache
    mapM_ (liftIO . closePreparedStatement) stmts