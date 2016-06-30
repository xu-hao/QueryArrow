{-# LANGUAGE TypeFamilies, MultiParamTypeClasses, FunctionalDependencies, ExistentialQuantification, FlexibleInstances, OverloadedStrings,
   RankNTypes, FlexibleContexts, GADTs, DeriveGeneric, PatternSynonyms #-}
module InMemory where

import ResultStream
import FO.Data
import FO.Domain
import QueryPlan
import Rewriting
import Config
import Parser
import DBQuery
import Utils

import Prelude  hiding (lookup)
import Data.Map.Strict (Map, (!), empty, member, insert, foldrWithKey, foldlWithKey, alter, lookup, fromList, toList, unionWith, unionsWith, intersectionWith, elems, delete, singleton, keys, filterWithKey)
import qualified Data.Map.Strict
import Data.List ((\\), intercalate, union)
import Control.Monad.Trans.State.Strict (StateT, evalStateT,evalState, get, put, State, runState   )
import Control.Applicative ((<$>), liftA2)
import Data.Convertible.Base
import Control.Monad.Logic
import Control.Monad.Except
import Data.Maybe
import qualified Data.ByteString.Lazy as B
import qualified Data.ByteString.Lazy.Char8 as B8
import Text.ParserCombinators.Parsec hiding (State)
import Text.Regex.TDFA ((=~))
import Data.Text (Text, unpack)
import Control.Concurrent (threadDelay)
import Debug.Trace




-- example MapDB


limitvarsInRow :: [Var] -> MapResultRow -> MapResultRow
limitvarsInRow vars row = transform (keys row) vars row

data MapDB (m :: * -> * )= MapDB String String [(ResultValue, ResultValue)] deriving Show

data MapDBStmt m = MapDBStmt (MapDB m) [Var] Query
instance (Functor m, Monad m) => Database_ (MapDB m) m MapResultRow (MapDBStmt m) where
    dbBegin _ = return ()
    dbCommit _ = return True
    dbPrepare _ = return True
    dbRollback _ = return ()
    getName (MapDB name _ _) = name
    getPreds (MapDB name predname _) = [ Pred (QPredName name [] predname) (PredType ObjectPred [Key "String", Key "String"]) ]
    determinateVars db vars (Atom thepred args)
        | thepred `elem` getPreds db = return (concatMap (\arg -> case arg of
                                                        (VarExpr v) -> [v]
                                                        _ -> []) args \\ vars)
             -- this just look up each var from the varDomainSize
    determinateVars _ _ _ = return []
    prepareQuery db vars qu _ = return (MapDBStmt db vars qu)
    supported (MapDB name predname _) (FAtomic (Atom (Pred p _) _)) _ | predNameMatches (QPredName name [] predname) p = True
    supported _ _ _ = False
    translateQuery _ _ qu vars = (show qu, vars)

instance (Monad m) => DBStatementClose m (MapDBStmt m) where
    dbStmtClose _ = return ()

instance (Monad m) => DBStatementExec m MapResultRow (MapDBStmt m) where
    dbStmtExec  (MapDBStmt (MapDB _ _ rows) vars (Query  form@(FAtomic _))) _ stream  = do
        row2 <- mapDBFilterResults rows stream form
        return (limitvarsInRow vars row2)


-- update mapdb

data StateMapDB (m :: * -> * )= StateMapDB String String deriving Show

data StateMapDBStmt m = StateMapDBStmt (StateMapDB m) [Var] Query

instance (Monad m) => Database_ (StateMapDB m) (StateT (Map String [(ResultValue, ResultValue)]) m) MapResultRow (StateMapDBStmt m) where
    dbBegin _ = return ()
    dbPrepare _ = return True
    dbCommit _ = return True
    dbRollback _ = return ()
    getName (StateMapDB name _) = name
    getPreds (StateMapDB name predname) = [ Pred (QPredName name [] predname) (PredType ObjectPred [Key "String", Key "String"]) ]
    determinateVars db vars  (Atom thepred args)
        | thepred `elem` getPreds db = return (concatMap (\arg -> case arg of
                                                        (VarExpr v) -> [v]
                                                        _ -> []) args \\ vars)
    determinateVars _ _ _ = return []
    prepareQuery db vars2 qu _ = return (StateMapDBStmt db vars2 qu)
    supported _ (FAtomic _) _ = True
    supported _ (FInsert _) _ = True
    supported _ _ _ = False
    translateQuery _ _ qu vars = (show qu, vars)

instance (Monad m) => DBStatementClose (StateT (Map String [(ResultValue, ResultValue)]) m) (StateMapDBStmt m) where
    dbStmtClose _ = return ()

instance (Monad m) => DBStatementExec (StateT (Map String [(ResultValue, ResultValue)]) m) MapResultRow (StateMapDBStmt m) where
    dbStmtExec (StateMapDBStmt (StateMapDB name _) vars (Query  form@(FAtomic _))) _ stream  = do
        rowsmap <- lift get
        let rows = rowsmap ! name
        row2 <- mapDBFilterResults rows stream form
        return (limitvarsInRow vars row2)
    dbStmtExec (StateMapDBStmt (StateMapDB name _) vars (Query  (FInsert lit@(Lit thesign _))))  rsvars stream = do
        rowsmap <- lift get
        let rows = rowsmap ! name
        let freevars = freeVars lit
        let add rows1 row = rows1 `union` [row]
        let remove rows1 row = rows1 \\ [row]
        let arg12 (Lit _ (Atom _ [a,b])) = (convert a, convert b)
            arg12 _ = error "wrong number of args"
        row1 <- stream
        let rows2 = (case thesign of
                        Pos -> add
                        Neg -> remove) rows (arg12 (substResultValue row1 lit))
        lift $ put (insert name rows2 rowsmap)
        return row1



mapDBFilterResults :: (Functor m, Monad m) => [(ResultValue, ResultValue)] -> ResultStream m MapResultRow -> Formula -> ResultStream m MapResultRow
mapDBFilterResults rows  results (FAtomic (Atom thepred args)) = do
    resrow <- results
    trace (show resrow) $ case args of
        VarExpr var1 : (VarExpr var2 : _)
            | var1 `member` resrow ->
                if var2 `member` resrow then do
                    guard ((resrow ! var1, resrow ! var2) `elem` rows)
                    return mempty
                else
                    listResultStream [singleton var2 (snd x) | x <- rows, fst x == resrow ! var1]

            | var2 `member` resrow ->
                listResultStream [singleton var1 (fst x) | x <- rows, snd x == resrow ! var2]
            | otherwise ->
                listResultStream [fromList [(var1, fst x), (var2, snd x)] | x <- rows]
        VarExpr var1 : StringExpr str2 : _ ->
            if var1 `member` resrow then do
                guard ((resrow ! var1, StringValue str2) `elem` rows)
                return mempty
            else
                listResultStream [singleton var1 (fst x) | x <- rows, snd x == StringValue str2]
        StringExpr str1 : VarExpr var2 : _ ->
            if var2 `member` resrow then do
                guard ((StringValue str1, resrow ! var2) `elem` rows)
                return mempty
            else
                listResultStream [singleton var2 (snd x) | x <- rows, snd x == StringValue str1]
        StringExpr str1 : StringExpr str2 : _ -> do
                guard ((StringValue str1, StringValue str2) `elem` rows)
                return mempty
        _ -> do
            guard False
            return mempty


-- example RegexDB

data RegexDB (m :: * -> *) = RegexDB String

pattern RegexPred ns = Pred (QPredName ns [] "like_regex") (PredType ObjectPred [Key "String", Key "Pattern"])

data RegexDBStmt = RegexDBStmt Query
instance (Monad m) => Database_ (RegexDB m) m MapResultRow RegexDBStmt where
    dbBegin _ = return ()
    dbPrepare _ = return True
    dbCommit _ = return True
    dbRollback _ = return ()
    getName (RegexDB name) = name
    getPreds db = [ RegexPred (getName db)]
    determinateVars _ _ _ = return []
    prepareQuery _ _ qu _ = return (RegexDBStmt qu)
    translateQuery _ _ qu vars = (show qu, vars)

    supported _ (FAtomic (Atom (RegexPred _) _)) _ = True
    supported _ _ _ = False

instance (Monad m) => DBStatementClose m (RegexDBStmt) where
    dbStmtClose _ = return ()

extractStringFromExpr :: ResultValue -> String
extractStringFromExpr (StringValue s) = unpack s
extractStringFromExpr a = error "cannot extract string from nonstring"

instance (Monad m) => DBStatementExec m MapResultRow (RegexDBStmt) where
    dbStmtExec (RegexDBStmt (Query (FAtomic (Atom _ [a, b])))) rsvars stream = do
        row <- stream
        if extractStringFromExpr (evalExpr row a) =~ extractStringFromExpr (evalExpr row b)
            then return mempty
            else emptyResultStream

    dbStmtExec (RegexDBStmt (Query  (Not (FAtomic (Atom _ [a, b]))))) rsvars stream = do
        row <- stream
        if not (extractStringFromExpr (evalExpr row a) =~ extractStringFromExpr (evalExpr row b))
            then return mempty
            else emptyResultStream

    dbStmtExec (RegexDBStmt qu) rsvars stream = error ("dqdb: unsupported query " ++ show qu)

-- example EqDB

data EqDB (m :: * -> *) = EqDB String

pattern EqPred ns = Pred (QPredName ns [] "eq") (PredType ObjectPred [Key "Any", Key "Any"])

data EqDBStmt = EqDBStmt Query
instance (Monad m) => Database_ (EqDB m) m MapResultRow EqDBStmt where
    dbBegin _ = return ()
    dbPrepare _ = return True
    dbCommit _ = return True
    dbRollback _ = return ()
    getName (EqDB name) = name
    getPreds db = [ EqPred (getName db)]
    determinateVars _ _ _ = return []
    prepareQuery _ _ qu _ = return (EqDBStmt qu)
    translateQuery _ _ qu vars = (show qu, vars)

    supported _ (FAtomic (Atom (EqPred _) _)) _ = True
    supported _ (Not (FAtomic (Atom (EqPred _) _))) _ = True
    supported _ _ _ = False

instance (Monad m) => DBStatementClose m (EqDBStmt) where
    dbStmtClose _ = return ()

evalExpr :: MapResultRow -> Expr -> ResultValue
evalExpr row (StringExpr s) = StringValue s
evalExpr row (IntExpr s) = IntValue s
evalExpr row (VarExpr v) = case lookup v row of
    Nothing -> Null
    Just r -> r

instance (Monad m) => DBStatementExec m MapResultRow (EqDBStmt) where
    dbStmtExec (EqDBStmt (Query (FAtomic (Atom _ [a, b])))) rsvars stream = do
        row <- stream
        if evalExpr row a == evalExpr row b
            then return mempty
            else emptyResultStream

    dbStmtExec (EqDBStmt (Query (Not (FAtomic (Atom _ [a, b]))))) rsvars stream = do
        row <- stream
        if evalExpr row a /= evalExpr row b
            then return mempty
            else emptyResultStream

    dbStmtExec (EqDBStmt qu) rsvars stream = error ("dqdb: unsupported query " ++ show qu)

-- example UtilsDB

data UtilsDB (m :: * -> *) = UtilsDB String

pattern SleepPred ns = Pred (QPredName ns [] "sleep") (PredType ObjectPred [Key "Number"])

data UtilsDBStmt = UtilsDBStmt Query

instance (MonadIO m) => Database_ (UtilsDB m) m MapResultRow UtilsDBStmt where
    dbBegin _ = return ()
    dbPrepare _ = return True
    dbCommit _ = return True
    dbRollback _ = return ()
    getName (UtilsDB name) = name
    getPreds db = [ SleepPred (getName db)]
    determinateVars _ _ _ = return []
    prepareQuery _ _ qu _ = return (UtilsDBStmt qu)
    translateQuery _ _ qu vars = (show qu, vars)

    supported _ (FAtomic (Atom (SleepPred _) _)) _ = True
    supported _ _ _ = False

instance (MonadIO m) => DBStatementClose m (UtilsDBStmt) where
    dbStmtClose _ = return ()

instance (MonadIO m) => DBStatementExec m MapResultRow (UtilsDBStmt) where
    dbStmtExec (UtilsDBStmt (Query (FAtomic (Atom (SleepPred _) [a])))) rsvars stream = do
        row <- stream
        let (IntValue i) = evalExpr row a
        liftIO $ threadDelay i
        return mempty

    dbStmtExec (UtilsDBStmt qu) rsvars stream = error ("dqdb: unsupported query " ++ show qu)
