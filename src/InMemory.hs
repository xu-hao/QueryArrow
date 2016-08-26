{-# LANGUAGE TypeFamilies, MultiParamTypeClasses, FunctionalDependencies, ExistentialQuantification, FlexibleInstances, OverloadedStrings,
   RankNTypes, FlexibleContexts, GADTs, DeriveGeneric, PatternSynonyms #-}
module InMemory where

import ResultStream
import FO.Data
import FO.Domain
import Rewriting
import Config
import Parser
import DBQuery
import Utils
import DB
import NoConnection

import Prelude  hiding (lookup)
import Data.Map.Strict (Map, (!), empty, member, insert, foldrWithKey, foldlWithKey, alter, lookup, fromList, toList, unionWith, unionsWith, intersectionWith, elems, delete, singleton, keys, filterWithKey)
import qualified Data.Map.Strict
import Data.List ((\\), intercalate, union)
import Control.Monad.Trans.State.Strict (StateT, evalStateT,evalState, get, put, State, runState   )
import Control.Applicative ((<$>), liftA2)
import Data.Convertible.Base
import Control.Monad.Logic
import Control.Monad.Except
import Control.Monad.Trans.Resource
import Data.Maybe
import qualified Data.ByteString.Lazy as B
import qualified Data.ByteString.Lazy.Char8 as B8
import Text.ParserCombinators.Parsec hiding (State)
import Text.Regex.TDFA ((=~))
import Data.Text (Text, unpack)
import Control.Concurrent (threadDelay)
import Algebra.Lattice
import Algebra.SemiBoundedLattice
import Data.Set (Set, toAscList)
import qualified Data.Set as Set
import Data.IORef
import Debug.Trace


-- example MapDB


limitvarsInRow :: [Var] -> MapResultRow -> MapResultRow
limitvarsInRow vars row = transform (keys row) vars row

data MapDB = MapDB String String [(ResultValue, ResultValue)] deriving Show

instance Database0 MapDB where
    getName (MapDB name _ _) = name
    getPreds (MapDB name predname _) = [ Pred (QPredName name [] predname) (PredType ObjectPred [Key "String", Key "String"]) ]
    determinateVars db vars (Atom thepred args)
        | thepred `elem` getPreds db = (Set.unions (map (\arg -> case arg of
                                                        (VarExpr v) -> Set.singleton v
                                                        _ -> bottom) args) \\\ vars)
             -- this just look up each var from the varDomainSize
    determinateVars _ _ _ = bottom
    supported (MapDB name predname _) (FAtomic (Atom (Pred p _) _)) _ | predNameMatches (QPredName name [] predname) p = True
    supported _ _ _ = False


instance NoConnectionDB MapDB where
    noConnectionDBStmtExec (MapDB _ _ rows) vars (Query (FAtomic (Atom _ args))) _ stream  = do
        row2 <- mapDBFilterResults rows stream args
        return (limitvarsInRow (toAscList vars) row2)
    noConnectionDBStmtExec _ _ qu _ _ = error ("dqdb: unsupported query " ++ show qu)



-- update mapdb

instance Show (IORef a) where
    show _ = "ioref"

data StateMapDB = StateMapDB String String (IORef ([(ResultValue, ResultValue)])) deriving Show

instance Database0 StateMapDB where
    getName (StateMapDB name _ _) = name
    getPreds (StateMapDB name predname _) = [ Pred (QPredName name [] predname) (PredType ObjectPred [Key "String", Key "String"]) ]
    determinateVars db vars  (Atom thepred args)
        | thepred `elem` getPreds db = (Set.unions (map (\arg -> case arg of
                                                        (VarExpr v) -> Set.singleton v
                                                        _ -> bottom) args) \\\ vars)
    determinateVars _ _ _ = bottom
    supported _ (FAtomic _) _ = True
    supported _ (FInsert _) _ = True
    supported _ _ _ = False

instance NoConnectionDB StateMapDB where
    noConnectionDBStmtExec (StateMapDB name _ map1) vars (Query (form@(FAtomic (Atom _ args)))) _ stream  = do
        rows <- liftIO $ readIORef map1
        row2 <- mapDBFilterResults rows stream args
        return (limitvarsInRow (toAscList vars) row2)
    noConnectionDBStmtExec (StateMapDB name _ map1) _ (Query (FInsert lit@(Lit thesign _))) _ stream = do
        rows <- liftIO $ readIORef map1
        let freevars = freeVars lit
        let add rows1 row = rows1 `union` [row]
        let remove rows1 row = rows1 \\ [row]
        let arg12 (Lit _ (Atom _ [a,b])) = (convert a, convert b)
            arg12 _ = error "wrong number of args"
        row1 <- stream
        let rows2 = (case thesign of
                        Pos -> add
                        Neg -> remove) rows (arg12 (substResultValue row1 lit))
        liftIO $ writeIORef map1 rows2
        return row1
    noConnectionDBStmtExec (StateMapDB name _ map1) _ qu _ stream = error ("dqdb: unsupported query " ++ show qu)



mapDBFilterResults :: (Functor m, Monad m) => [(ResultValue, ResultValue)] -> ResultStream m MapResultRow -> [Expr] -> ResultStream m MapResultRow
mapDBFilterResults rows  results args = do
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

data RegexDB = RegexDB String

pattern RegexPred ns = Pred (QPredName ns [] "like_regex") (PredType ObjectPred [Key "String", Key "Pattern"])

instance Database0 RegexDB where
    getName (RegexDB name) = name
    getPreds db = [ RegexPred (getName db)]
    determinateVars _ _ _ = bottom
    supported _ (FAtomic (Atom (RegexPred _) _)) _ = True
    supported _ _ _ = False

extractStringFromExpr :: ResultValue -> String
extractStringFromExpr (StringValue s) = unpack s
extractStringFromExpr a = error "cannot extract string from nonstring"

instance NoConnectionDB RegexDB where
    noConnectionDBStmtExec (RegexDB _) _ (Query (FAtomic (Atom _ [a, b]))) _ stream = do
        row <- stream
        if extractStringFromExpr (evalExpr row a) =~ extractStringFromExpr (evalExpr row b)
            then return mempty
            else emptyResultStream

    noConnectionDBStmtExec (RegexDB _) _ qu _ stream = error ("dqdb: unsupported query " ++ show qu)

-- example EqDB

data EqDB = EqDB String

pattern EqPred ns = Pred (QPredName ns [] "eq") (PredType ObjectPred [Key "Any", Key "Any"])

instance Database0 EqDB where
    getName (EqDB name) = name
    getPreds db = [ EqPred (getName db)]
    determinateVars _ _ _ = bottom
    supported _ (FAtomic (Atom (EqPred _) _)) _ = True
    supported _ _ _ = False

evalExpr :: MapResultRow -> Expr -> ResultValue
evalExpr row (StringExpr s) = StringValue s
evalExpr row (IntExpr s) = IntValue s
evalExpr row (VarExpr v) = case lookup v row of
    Nothing -> Null
    Just r -> r
evalExpr row expr = error ("evalExpr: unsupported expr " ++ show expr)

instance NoConnectionDB EqDB where
    noConnectionDBStmtExec (EqDB _) _ (Query (FAtomic (Atom _ [a, b]))) _ stream = do
        row <- stream
        if evalExpr row a == evalExpr row b
            then return mempty
            else emptyResultStream

    noConnectionDBStmtExec (EqDB _) _ qu _ stream = error ("dqdb: unsupported query " ++ show qu)

-- example UtilsDB

data UtilsDB = UtilsDB String

pattern SleepPred ns = Pred (QPredName ns [] "sleep") (PredType ObjectPred [Key "Number"])

instance Database0 UtilsDB where
    getName (UtilsDB name) = name
    getPreds db = [ SleepPred (getName db)]
    determinateVars _ _ _ = bottom
    supported _ (FAtomic (Atom (SleepPred _) [_])) _ = True
    supported _ _ _ = False

instance NoConnectionDB UtilsDB where
    noConnectionDBStmtExec (UtilsDB _) _ (Query (FAtomic (Atom (SleepPred _) [qu]))) _ stream = do
        row <- stream
        let (IntValue i) = evalExpr row qu
        liftIO $ threadDelay i
        return mempty

    noConnectionDBStmtExec (UtilsDB _) _ qu _ stream = error ("dqdb: unsupported query " ++ show qu)
