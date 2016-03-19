{-# LANGUAGE FlexibleInstances, MultiParamTypeClasses, FlexibleContexts, ExistentialQuantification, FunctionalDependencies, GADTs #-}
module DBQuery where

import QueryPlan
import ResultStream
import FO.Data
import FO.Domain

import Prelude hiding (lookup, foldl)
import Control.Monad.IO.Class (liftIO)
import Control.Monad.Trans.Class (lift)
import Control.Monad.Trans.State.Strict (StateT, get, put, modify)
import Data.Map.Strict (Map, foldl, empty, (!), singleton, lookup)
import Data.Convertible
import qualified Data.Text as T
import Data.Bifunctor
import Control.Monad.Trans.Resource

data DBConnInfo = DBConnInfo {
    dbHost :: String,
    dbPort :: Int,
    dbUser :: String,
    dbPassword :: String,
    dbDB :: String
    }

-- the cache to prepared statements

data DBAdapterState = DBAdapterState {
    preparedStatementMap :: Map [Var] PreparedStatement,
    nextidCache :: Maybe Int
    }

type DBAdapterMonad = StateT DBAdapterState (ResourceT IO)

class PreparedStatement_ stmt where
    execWithParams :: stmt -> [Expr] -> ResultStream DBAdapterMonad MapResultRow
    closePreparedStatement :: stmt -> IO ()

data PreparedStatement = forall stmt. PreparedStatement_ stmt => PreparedStatement {
    unPreparedStatement :: stmt
}

class DBConnection conn query insert | conn -> query insert where
    prepareQueryStatement :: conn -> query -> IO PreparedStatement
    prepareInsertStatement :: conn -> insert -> IO PreparedStatement
    connBegin :: conn -> IO ()
    connCommit :: conn -> IO ()
    connRollback :: conn -> IO ()
    connClose :: conn -> IO ()

class ConnectionDB m conn trans | conn -> m  where
    extractDomainSize :: conn -> trans -> DomainSizeMap -> Sign -> Atom -> m DomainSizeMap

class (Show query, Show insert) => Translate trans row query insert | trans -> row query insert where
    translateQueryWithParams :: trans -> Query -> row -> (query, [Var])
    translateInsertWithParams :: trans -> Insert -> row ->  (insert, [Var])
    translateable :: trans -> Formula -> Bool
    translateableInsert :: trans -> Formula -> [Lit] -> Bool

class (Show query) => TranslateSequence trans query | trans -> query where
    translateSequenceQuery :: trans -> (query, Var)

-- call this to clear all dbs
closeAllCachedPreparedStatements :: DBAdapterMonad ()
closeAllCachedPreparedStatements = do
    s <- get
    let stmts = foldl (\stmts2 stmt -> stmts2 ++ [stmt]) [] (preparedStatementMap s)
    liftIO $ mapM_ (\ ps -> case ps of PreparedStatement ps_ -> closePreparedStatement ps_) stmts

data GenericDB conn trans where
    GenericDB :: (ConnectionDB DBAdapterMonad conn trans, DBConnection conn query insert, Translate trans MapResultRow query insert) => conn -> String -> [Pred] -> trans -> GenericDB conn trans

data SequenceDB conn where
    SequenceDB :: (DBConnection conn query insert, TranslateSequence trans query) => conn -> String -> String -> trans -> SequenceDB conn

instance Database_ (GenericDB conn trans) DBAdapterMonad MapResultRow where
    dbBegin (GenericDB conn _ _ _) = do
        liftIO $ connBegin conn
        modify (\s -> s{preparedStatementMap = empty})
    dbCommit (GenericDB conn _ _ _) = do
        liftIO $ connCommit conn
        closeAllCachedPreparedStatements
    dbRollback (GenericDB conn _ _ _) = do
        liftIO $ connRollback conn
        closeAllCachedPreparedStatements
    dbBeginCommand _ = return ()
    dbEndCommand _ = return ()
    getName (GenericDB _ name _ _)= name
    getPreds (GenericDB _ _ preds _)= preds
    domainSize (GenericDB conn _  _ trans)= extractDomainSize conn trans
    doQuery (GenericDB conn _ _ trans) query _ stream = do
        row <- stream
        let (sqlquery, params) = translateQueryWithParams trans query row
        (PreparedStatement stmt) <- liftIO $ prepareQueryStatement conn sqlquery
        bracketPStream (return stmt) (closePreparedStatement) (\stmt -> execWithParams stmt (map (\param -> convert (row ! param)) params))
    doInsert (GenericDB conn _ _ trans) insert _ stream = do
        row <- stream
        let (sqlupdate, params) = translateInsertWithParams trans insert (row :: MapResultRow)
        (PreparedStatement stmt) <- liftIO $ prepareInsertStatement conn sqlupdate
        _ <- lift (depleteResultStream (bracketPStream (return stmt) closePreparedStatement (\stmt -> execWithParams stmt (map (\param -> convert (row ! param)) params))))
        return row
    supported (GenericDB _ _ _ trans) formula = translateable trans formula
    supportedInsert (GenericDB _ _ _ trans) formula lits = translateableInsert trans  formula lits
    translateQuery (GenericDB _ _ _ trans) qu vars row = first show (translateQueryWithParams trans qu row)
    translateInsert (GenericDB _ _ _ trans) qu vars row =  first show (translateInsertWithParams trans qu row)

instance Database_ (SequenceDB conn) DBAdapterMonad MapResultRow where
        dbBegin (SequenceDB _ _ _ _) = do
            s <- get
            put s{nextidCache = Nothing}
        dbCommit (SequenceDB _ _ _ _) = do
            s <- get
            put s{nextidCache = Nothing}
        dbRollback (SequenceDB _ _ _ _) = do
            s <- get
            put s{nextidCache = Nothing}
        dbBeginCommand (SequenceDB _ _ _ _) = do
            s <- get
            put s{nextidCache = Nothing}
        dbEndCommand (SequenceDB _ _ _ _) = do
            s <- get
            put s{nextidCache = Nothing}
        getName (SequenceDB _ name _ _)= name
        getPreds (SequenceDB _ _ predname _)= [Pred predname (PredType EssentialPred [Key "Any"])]
        domainSize (SequenceDB _ _ _ _)= \dsmap sign (Atom _ [VarExpr v]) -> return (singleton v (Bounded 1))
        doQuery (SequenceDB conn _ _ trans) (Query vars (Atomic (Atom _ [VarExpr v]))) _ stream = do -- only consider the case of var for now
            row <- stream
            s <- lift $ get
            nextid <- case (nextidCache s) of
                Nothing -> do
                    let (sqlquery, var) = translateSequenceQuery trans
                    (PreparedStatement stmt) <- liftIO $ prepareQueryStatement conn sqlquery
                    nextidrow : _ <- lift $ getAllResultsInStream (execWithParams stmt mempty)
                    let (IntValue nextid) = nextidrow ! var
                    lift $ put (s {nextidCache = Just nextid})
                    return nextid
                Just nextid ->
                    return nextid
            case lookup v row of
                Nothing -> case vars of
                    [] -> return mempty
                    [v2] | v == v2 -> return (mappend row (singleton v (IntValue nextid)))
                    _ -> error ("SequenceDB:doQuery: unsupported variables " ++ show vars)
                Just (IntValue i) ->
                    if i == nextid then
                        return mempty
                    else
                        emptyResultStream
                Just _ ->
                    emptyResultStream
        doQuery _ _ _ _ = error "not supported"
        doInsert _ _ _ _ = error "not supported"
        supported (SequenceDB _ predname _ _) (Atomic (Atom (Pred predname2 _) _)) = predname == predname2
        supported _ _ = False
        supportedInsert _ _ _ = False
        translateQuery (SequenceDB _ _ _ trans) qu _ row = ( show (fst (translateSequenceQuery trans )) ++ " or read from cache", [])
        translateInsert _ _ _ _ = error "not supported"
