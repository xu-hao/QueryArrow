{-# LANGUAGE FlexibleContexts, TypeFamilies, ScopedTypeVariables, PartialTypeSignatures, UndecidableInstances #-}
module QueryArrow.DB.ParametrizedStatement where

import QueryArrow.DB.DB
import QueryArrow.Semantics.ResultSet
import QueryArrow.Semantics.ResultSet.ResultSetResultStreamResultSet
import QueryArrow.Semantics.ResultStream

import Data.Convertible
import Control.Monad.IO.Class
import Data.Conduit
import QueryArrow.Data.Monoid.Action

-- interface
class (ResultSet (PSResultSetType stmt)) => IPSDBStatement stmt where
    type ParameterType stmt   -- Map Var Expr
    type PSResultSetType stmt
    execWithParams :: stmt -> ParameterType stmt -> IO (PSResultSetType stmt)
    psdbGetHeader :: stmt -> HeaderType (ResultSetRowType (PSResultSetType stmt))
    psdbStmtClose :: stmt -> IO ()

-- statement

newtype PSDBStatement stmt = PSDBStatement stmt

instance forall stmt . (IPSDBStatement stmt, Convertible (HeaderType (ResultSetRowType (PSResultSetType stmt)), ResultSetRowType (PSResultSetType stmt)) (ParameterType stmt)) => IDBStatement (PSDBStatement stmt) where
    type InputRowType (PSDBStatement stmt) = ResultSetRowType (PSResultSetType stmt)
    type ResultSetType (PSDBStatement stmt) = ResultSetResultStreamResultSet (ResultSetTransType (PSResultSetType stmt)) (PSResultSetType stmt)
    dbStmtClose (PSDBStatement stmt) = psdbStmtClose stmt
    dbStmtExec (PSDBStatement stmt) rset = do
        let hdr = getHeader rset
        let stream = toResultStream rset
        let hdr2 = psdbGetHeader stmt
        return (ResultSetResultStreamResultSet mempty hdr2 (ResultStream (case stream of
                                                        ResultStream rs -> rs =$=
                                                                  awaitForever (\row -> do
                                                                    -- liftIO $ infoM "QA" ("current row " ++ show row)
                                                                    -- liftIO $ infoM "QA" ("returns row")
                                                                    rset2 <- liftIO $ execWithParams stmt (convert (hdr, row :: ResultSetRowType (PSResultSetType stmt)))
                                                                    yield (act (combineRow hdr row hdr2 :: ResultSetTransType (PSResultSetType stmt)) rset2)))))


{- instance DBStatementExec DBAdapterMonad MapResultRow (PreparedSequenceStatement conn trans)  where
    dbStmtExec (PreparedSequenceStatement conn trans vars v) vars2 stream = do
        row <- stream
        nextid <- do
                let (sqlquery, var) = translateSequenceQuery trans
                (PreparedStatement stmt) <- lift $ prepareQueryStatement conn sqlquery
                nextidrow : _ <- lift $ getAllResultsInStream (execWithParams stmt mempty)
                let (Int64Value nextid) = nextidrow ! var
                return nextid
        case lookup v row of
            Nothing -> case toAscList vars of
                [] -> return mempty
                [v2] | v == v2 -> return (mappend row (singleton v (Int64Value nextid)))
                _ -> error ("SequenceDB:doQuery: unsupported variables " ++ show vars)
            Just (Int64Value i) ->
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
