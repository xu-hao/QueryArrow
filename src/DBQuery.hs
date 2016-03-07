{-# LANGUAGE FlexibleInstances, MultiParamTypeClasses, FlexibleContexts, ExistentialQuantification, FunctionalDependencies, GADTs #-}
module DBQuery where

import FO
import QueryPlan
import ResultStream
import FO.Data
import FO.Domain

import Prelude hiding (lookup, foldl)
import Control.Monad.IO.Class (liftIO)
import Control.Monad.Trans.State.Strict (StateT, get, put)
import Data.Map.Strict (Map, foldl, empty, (!))
import Data.Convertible
import qualified Data.Text as T

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
    execWithParams :: stmt -> [Expr] -> ResultStream DBAdapterMonad MapResultRow
    closePreparedStatement :: stmt -> IO ()

data PreparedStatement = forall stmt. PreparedStatement_ stmt => PreparedStatement {
    unPreparedStatement :: stmt
}

class DBConnection conn query insert | conn -> query insert where
    execQueryStatement :: conn -> query -> ResultStream DBAdapterMonad MapResultRow
    prepareQueryStatement :: conn -> query -> IO PreparedStatement
    execInsertStatement :: conn -> insert -> ResultStream DBAdapterMonad MapResultRow
    prepareInsertStatement :: conn -> insert -> IO PreparedStatement
    connBegin :: conn -> IO ()
    connCommit :: conn -> IO ()
    connRollback :: conn -> IO ()
    connClose :: conn -> IO ()

class ConnectionDB m conn trans | conn -> m  where
    extractDomainSize :: conn -> trans -> DomainSizeMap -> Sign -> Atom -> m DomainSizeMap

class Translate trans row query insert | trans -> row query insert where
    translateQuery :: trans -> Query -> query
    translateQueryWithParams :: trans -> Query -> row -> (query, [Var])
    translateInsert :: trans -> Insert -> insert
    translateInsertWithParams :: trans -> Insert -> row ->  insert
    translateable :: trans -> Formula -> Bool


-- call this to clear all dbs
closeAllCachedPreparedStatements :: DBAdapterMonad ()
closeAllCachedPreparedStatements = do
    (DBAdapterState cache) <- get
    let stmts = foldl (\stmts2 stmt -> stmts2 ++ [stmt]) [] cache
    liftIO $ mapM_ (\ ps -> case ps of PreparedStatement ps_ -> closePreparedStatement ps_) stmts

data GenericDB conn trans where
    GenericDB :: (ConnectionDB DBAdapterMonad conn trans, DBConnection conn query insert, Translate trans MapResultRow query insert) => conn -> String -> [Pred] -> trans -> GenericDB conn trans

instance Database_ (GenericDB conn trans) DBAdapterMonad MapResultRow where
    dbBegin (GenericDB conn _ _ _) = do
        liftIO $ connBegin conn
        put (DBAdapterState empty)
    dbCommit (GenericDB conn _ _ _) = do
        liftIO $ connCommit conn
        closeAllCachedPreparedStatements
    dbRollback (GenericDB conn _ _ _) = do
        liftIO $ connRollback conn
        closeAllCachedPreparedStatements
    getName (GenericDB _ name _ _)= name
    getPreds (GenericDB _ _ preds _)= preds
    domainSize (GenericDB conn _  _ trans)= extractDomainSize conn trans
    doQuery (GenericDB conn _ _ trans) query = do
        let sqlquery = translateQuery trans query
        execQueryStatement conn sqlquery

    doFilter (GenericDB conn _ _ trans) query stream = do
        row <- stream
        let (sqlquery, params) = translateQueryWithParams trans query row
        (PreparedStatement stmt) <- liftIO $ prepareQueryStatement conn sqlquery
        execWithParams stmt (map (\param -> convert (row ! param)) params)
    doInsert (GenericDB conn _ _ trans) insert = do
        let sqlupdate = translateInsert trans insert
        rows <- getAllResultsInStream (execInsertStatement conn sqlupdate)
        return (map (\row -> case row ! Var "i" of
                IntValue i -> i
                _ -> error "unsupported result value") rows)
    supported (GenericDB _ _ _ trans) formula = translateable trans formula
