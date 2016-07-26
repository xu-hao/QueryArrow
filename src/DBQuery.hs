{-# LANGUAGE FlexibleInstances, MultiParamTypeClasses, FlexibleContexts, ExistentialQuantification, FunctionalDependencies, GADTs #-}
module DBQuery where

import QueryPlan
import ResultStream
import FO.Data
import FO.Domain

import Prelude hiding (lookup, foldl)
import Control.Monad.IO.Class (liftIO)
import Control.Monad.Trans.Class (lift)
import Control.Monad.Trans.State.Strict (StateT)
import Data.Map.Strict (Map, (!), singleton, lookup)
import Data.Convertible
import qualified Data.Map as M
import Data.Bifunctor
import Control.Monad.Trans.Resource
import Data.Set (Set, toAscList)
import qualified Data.Set as Set

data DBConnInfo = DBConnInfo {
    dbHost :: String,
    dbPort :: Int,
    dbUser :: String,
    dbPassword :: String,
    dbDB :: String
    }

data DBAdapterState = DBAdapterState {
    uuid :: Maybe String
    }

type DBAdapterMonad = StateT DBAdapterState (ResourceT IO)

class PreparedStatement_ stmt where
    execWithParams :: stmt -> Map Var Expr -> ResultStream DBAdapterMonad MapResultRow
    closePreparedStatement :: stmt -> IO ()

data PreparedStatement = forall stmt. PreparedStatement_ stmt => PreparedStatement {
    unPreparedStatement :: stmt
}

instance DBStatementClose DBAdapterMonad (PreparedStatement, [Var])  where
    dbStmtClose (stmt, _) = case stmt of PreparedStatement s -> liftIO $ closePreparedStatement s

instance DBStatementExec DBAdapterMonad MapResultRow (PreparedStatement, [Var])  where
    dbStmtExec (PreparedStatement stmt, params) vars stream = do
        row <- stream
        bracketPStream (return stmt) closePreparedStatement (\stmt -> execWithParams stmt (M.map convert row))

class DBConnection conn  where
    prepareQueryStatement :: conn -> (Bool, [Var], String, [Var]) -> DBAdapterMonad PreparedStatement
    connBegin :: conn -> DBAdapterMonad ()
    connPrepare :: conn -> DBAdapterMonad Bool
    connCommit :: conn -> DBAdapterMonad Bool
    connRollback :: conn -> DBAdapterMonad ()
    connClose :: conn -> IO ()

class ExtractDomainSize m conn trans | conn -> m  where
    extractDomainSize :: conn -> trans -> Set Var -> Atom -> m (Set Var)

class Translate trans row  | trans -> row  where
    translateQueryWithParams :: trans -> Set Var -> Query -> Set Var -> (Bool, [Var], String, [Var])
    translateable :: trans -> Formula -> Set Var -> Bool

class TranslateSequence trans where
    translateSequenceQuery :: trans -> (String, Var)


data GenericDB conn trans where
    GenericDB :: (ExtractDomainSize DBAdapterMonad conn trans, DBConnection conn , Translate trans MapResultRow ) => conn -> String -> [Pred] -> trans -> GenericDB conn trans

instance Database_ (GenericDB conn trans) DBAdapterMonad MapResultRow (PreparedStatement, [Var]) where
    dbOpen _ = return ()
    dbClose (GenericDB conn _ _ _) =
        connClose conn
    dbBegin (GenericDB conn _ _ _) = do
        connBegin conn
    dbPrepare (GenericDB conn _ _ _) = do
        connPrepare conn
    dbCommit (GenericDB conn _ _ _) = do
        connCommit conn
    dbRollback (GenericDB conn _ _ _) = do
        connRollback conn
    getName (GenericDB _ name _ _)= name
    getPreds (GenericDB _ _ preds _)= preds
    determinateVars (GenericDB conn _  _ trans) = extractDomainSize conn trans
    prepareQuery (GenericDB conn _ _ _) vars2 query vars = do
        let (_, _, sqlquery, params) = query
        stmt <- prepareQueryStatement conn query
        return (stmt, params)

    supported (GenericDB _ _ _ trans) formula vars = translateable trans formula vars
    translateQuery (GenericDB _ _ _ trans) vars2 query vars = translateQueryWithParams trans vars2 query vars

{- instance DBStatementExec DBAdapterMonad MapResultRow (PreparedSequenceStatement conn trans)  where
    dbStmtExec (PreparedSequenceStatement conn trans vars v) vars2 stream = do
        row <- stream
        nextid <- do
                let (sqlquery, var) = translateSequenceQuery trans
                (PreparedStatement stmt) <- lift $ prepareQueryStatement conn sqlquery
                nextidrow : _ <- lift $ getAllResultsInStream (execWithParams stmt mempty)
                let (IntValue nextid) = nextidrow ! var
                return nextid
        case lookup v row of
            Nothing -> case toAscList vars of
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

instance Database_ (SequenceDB conn trans) DBAdapterMonad MapResultRow (PreparedSequenceStatement conn trans) where
        dbOpen _ = return ()
        dbClose _ = return ()
        dbBegin (SequenceDB _ _ _ _) = return ()
        dbPrepare (SequenceDB _ _ _ _) = return True
        dbCommit (SequenceDB _ _ _ _) = return True
        dbRollback (SequenceDB _ _ _ _) = return ()
        getName (SequenceDB _ name _ _)= name
        getPreds (SequenceDB _ name predname _)= [Pred (QPredName name [] predname) (PredType ObjectPred [Key "Any"])]
        determinateVars (SequenceDB _ _ _ _)= \ _ (Atom _ [VarExpr v]) -> return (Set.singleton v)
        prepareQuery (SequenceDB conn _ _ trans) vars (v, _) _ =
            return (PreparedSequenceStatement conn trans vars (Var v))
        prepareQuery _ _ _ _ = error "not supported"

        supported (SequenceDB _ name predname _) (FAtomic (Atom (Pred predname2 _) [VarExpr _])) _ =
            predNameMatches (QPredName name [] predname) predname2
        supported _ _ _ = False
        translateQuery (SequenceDB _ _ _ trans) _ (Query (FAtomic (Atom _ [VarExpr (Var v)]))) _ = ( v, [])
-}
