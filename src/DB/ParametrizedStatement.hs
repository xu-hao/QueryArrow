{-# LANGUAGE FlexibleInstances, MultiParamTypeClasses, FlexibleContexts, ExistentialQuantification, GADTs, TypeFamilies #-}
module DB.ParametrizedStatement where

import DB.DB
import DB.ResultStream
import FO.Data
import FO.Domain

import Prelude hiding (lookup, foldl)
import Control.Monad.IO.Class (liftIO)
import Data.Map.Strict (Map, (!), singleton, lookup)
import Data.Convertible
import qualified Data.Map as M
import Control.Monad.Trans.Resource
import Data.Set (Set, toAscList)
import qualified Data.Set as Set
import Data.Typeable

-- interface
class (Convertible (PSRowType stmt) (ParameterType stmt)) => IPSDBStatement stmt where
    type PSRowType stmt
    type ParameterType stmt   -- Map Var Expr
    execWithParams :: stmt -> ParameterType stmt -> DBResultStream (PSRowType stmt)
    psdbStmtClose :: stmt -> IO ()

-- statement

newtype PSDBStatement stmt = PSDBStatement stmt

instance IPSDBStatement stmt => IDBStatement (PSDBStatement stmt) where
    type RowType (PSDBStatement stmt) = PSRowType stmt
    dbStmtClose (PSDBStatement stmt) = psdbStmtClose stmt
    dbStmtExec (PSDBStatement stmt) stream = do
        row <- stream
        execWithParams stmt (convert row)


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
