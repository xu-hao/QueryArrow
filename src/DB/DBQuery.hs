{-# LANGUAGE FlexibleInstances, MultiParamTypeClasses, FlexibleContexts, ExistentialQuantification, GADTs, TypeFamilies #-}
module DBQuery where

import DB
import ResultStream
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

data DBConnInfo = DBConnInfo {
    dbHost :: String,
    dbPort :: Int,
    dbUser :: String,
    dbPassword :: String,
    dbDB :: String
    }

class PreparedStatement stmt where
    execWithParams :: stmt -> Map Var Expr -> ResultStream (ResourceT IO) MapResultRow
    closePreparedStatement :: stmt -> IO ()

newtype PreparedDBStatement stmt = PreparedDBStatement stmt

instance PreparedStatement stmt => DBStatement (PreparedDBStatement stmt) where
    type RowType (PreparedDBStatement stmt) = MapResultRow
    dbStmtClose (PreparedDBStatement stmt) = closePreparedStatement stmt
    dbStmtExec (PreparedDBStatement stmt) vars stream = do
        row <- stream
        execWithParams stmt (M.map convert row)

class Translate trans where
    type TranslateQueryType trans
    extractDomainSize :: trans -> Set Var -> Atom -> Set Var
    translateQueryWithParams :: trans -> Set Var -> Query -> Set Var -> TranslateQueryType trans
    translateable :: trans -> Formula -> Set Var -> Bool

class GenericDatabase db where
    type GenericDatabaseConnectionType db
    gdbOpen :: db -> IO (GenericDatabaseConnectionType db)

data GenericDB db trans where
    GenericDB :: db -> String -> [Pred] -> trans -> GenericDB db trans

instance Translate trans => Database0 (GenericDB db trans) where
    getName (GenericDB _ name _ _) = name
    getPreds (GenericDB _ _ preds _) = preds
    determinateVars (GenericDB conn _  _ trans) = extractDomainSize trans
    supported (GenericDB _ _ _ trans) formula vars = translateable trans formula vars

instance (GenericDatabase db,
          DBConnection (GenericDatabaseConnectionType db),
          Translate trans,
          QueryType (GenericDatabaseConnectionType db) ~ TranslateQueryType trans) => Database (GenericDB db trans) where
    type ConnectionType (GenericDB db trans) = GenericDatabaseConnectionType db
    dbOpen (GenericDB db _ _ _) = gdbOpen db
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
