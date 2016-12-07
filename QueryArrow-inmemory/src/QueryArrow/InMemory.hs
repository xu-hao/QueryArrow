{-# LANGUAGE TypeFamilies, MultiParamTypeClasses, FlexibleInstances, FlexibleContexts, PatternSynonyms #-}
module QueryArrow.InMemory where

import QueryArrow.DB.ResultStream
import QueryArrow.FO.Data
import QueryArrow.DB.DB
import QueryArrow.DB.NoConnection

import Prelude  hiding (lookup)
import Data.Map.Strict ((!), member,   lookup, fromList,  singleton)
import Data.List ((\\), union)
import Data.Convertible.Base
import Text.Regex.TDFA ((=~))
import Control.Monad.IO.Class
import Control.Monad
import Data.Text (unpack)
import Control.Concurrent (threadDelay)
import Algebra.Lattice
import Data.Set (Set)
import qualified Data.Set as Set
import Data.IORef
import Debug.Trace


-- example MapDB


data MapDB = MapDB String String [(ResultValue, ResultValue)] deriving Show

instance IDatabase0 MapDB where
    type DBFormulaType MapDB = Formula

    getName (MapDB name _ _) = name
    getPreds (MapDB name predname _) = [ Pred (QPredName name [] predname) (PredType ObjectPred [Key "String", Key "String"]) ]
    determinateVars db = mempty
    supported (MapDB name predname _) (FAtomic (Atom p _)) _ | predNameMatches (QPredName name [] predname) p = True
    supported _ _ _ = False

instance IDatabase1 MapDB where
    type DBQueryType MapDB = (Set Var, Formula, Set Var)
    translateQuery _ vars qu vars2 = return (vars, qu, vars2)


instance INoConnectionDatabase2 MapDB where
    type NoConnectionQueryType MapDB = (Set Var, Formula, Set Var)
    type NoConnectionRowType MapDB = MapResultRow
    noConnectionDBStmtExec (MapDB _ _ rows) (vars, ( (FAtomic (Atom _ args))), _) stream  = do
        row2 <- mapDBFilterResults rows stream args
        return (transform vars row2)
    noConnectionDBStmtExec _ qu _ = error ("dqdb: unsupported Formula " ++ show qu)

-- update mapdb

instance Show (IORef a) where
    show _ = "ioref"

data StateMapDB = StateMapDB String String (IORef ([(ResultValue, ResultValue)])) deriving Show

instance IDatabase0 StateMapDB where
    type DBFormulaType StateMapDB = Formula

    getName (StateMapDB name _ _) = name
    getPreds (StateMapDB name predname _) = [ Pred (QPredName name [] predname) (PredType ObjectPred [Key "String", Key "String"]) ]
    determinateVars db = mempty
    supported _ (FAtomic _) _ = True
    supported _ (FInsert _) _ = True
    supported _ _ _ = False

instance IDatabase1 StateMapDB where
    type DBQueryType StateMapDB = (Set Var, Formula, Set Var)
    translateQuery _ vars qu vars2 = return (vars, qu, vars2)

instance INoConnectionDatabase2 StateMapDB where
    type NoConnectionQueryType StateMapDB = (Set Var, Formula, Set Var)
    type NoConnectionRowType StateMapDB = MapResultRow
    noConnectionDBStmtExec (StateMapDB name _ map1) (vars,  FAtomic (Atom _ args), _) stream  = do
        rows <- liftIO $ readIORef map1
        row2 <- mapDBFilterResults rows stream args
        return (transform vars row2)
    noConnectionDBStmtExec (StateMapDB name _ map1) (_, FInsert lit@(Lit thesign _), _) stream = do
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
    noConnectionDBStmtExec (StateMapDB name _ map1) qu stream = error ("dqdb: unsupported Formula " ++ show qu)



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

pattern RegexPredName ns = QPredName ns [] "like_regex"
pattern RegexPred ns = Pred (RegexPredName ns) (PredType ObjectPred [Key "String", Key "Pattern"])

instance IDatabase0 RegexDB where
    type DBFormulaType RegexDB = Formula
    getName (RegexDB name) = name
    getPreds db = [ RegexPred (getName db)]
    determinateVars db = fromList [ (RegexPredName (getName db), [])]
    supported _ (FAtomic (Atom (RegexPredName _) _)) _ = True
    supported _ _ _ = False
instance IDatabase1 RegexDB where
    type DBQueryType RegexDB = (Set Var, Formula, Set Var)
    translateQuery _ vars qu vars2 = return (vars, qu, vars2)

extractStringFromExpr :: ResultValue -> String
extractStringFromExpr (StringValue s) = unpack s
extractStringFromExpr a = error "cannot extract string from nonstring"

instance INoConnectionDatabase2 RegexDB where
    type NoConnectionQueryType RegexDB = (Set Var, Formula, Set Var)
    type NoConnectionRowType RegexDB = MapResultRow
    noConnectionDBStmtExec (RegexDB _) (_,  (FAtomic (Atom _ [a, b])), _) stream = do
        row <- stream
        if extractStringFromExpr (evalExpr row a) =~ extractStringFromExpr (evalExpr row b)
            then return mempty
            else emptyResultStream

    noConnectionDBStmtExec (RegexDB _) qu stream = error ("dqdb: unsupported Formula " ++ show qu)

-- example EqDB

data EqDB = EqDB String

pattern EqPredName ns = QPredName ns [] "eq"
pattern EqPred ns = Pred (EqPredName ns) (PredType ObjectPred [Key "Any", Key "Any"])

instance IDatabase0 EqDB where
    type DBFormulaType EqDB = Formula
    getName (EqDB name) = name
    getPreds db = [ EqPred (getName db)]
    determinateVars db = fromList [ (EqPredName (getName db), [])]
    supported _ (FAtomic (Atom (EqPredName _) _)) _ = True
    supported _ _ _ = False

instance IDatabase1 EqDB where
    type DBQueryType EqDB = (Set Var, Formula, Set Var)
    translateQuery _ vars qu vars2 = return (vars, qu, vars2)

evalExpr :: MapResultRow -> Expr -> ResultValue
evalExpr row (StringExpr s) = StringValue s
evalExpr row (IntExpr s) = IntValue s
evalExpr row (VarExpr v) = case lookup v row of
    Nothing -> Null
    Just r -> r
evalExpr row expr = error ("evalExpr: unsupported expr " ++ show expr)

instance INoConnectionDatabase2 EqDB where
    type NoConnectionQueryType EqDB = (Set Var, Formula, Set Var)
    type NoConnectionRowType EqDB = MapResultRow
    noConnectionDBStmtExec (EqDB _) (_,  (FAtomic (Atom _ [a, b])), _) stream = do
        row <- stream
        if evalExpr row a == evalExpr row b
            then return mempty
            else emptyResultStream

    noConnectionDBStmtExec (EqDB _) qu stream = error ("dqdb: unsupported Formula " ++ show qu)

-- example UtilsDB

data UtilsDB = UtilsDB String

pattern SleepPredName ns = QPredName ns [] "sleep"
pattern SleepPred ns = Pred (SleepPredName ns) (PredType ObjectPred [Key "Number"])

instance IDatabase0 UtilsDB where
    type DBFormulaType UtilsDB = Formula
    getName (UtilsDB name) = name
    getPreds db = [ SleepPred (getName db)]
    determinateVars db = fromList [ (SleepPredName (getName db), [])]
    supported _ (FAtomic (Atom (SleepPredName _) [_])) _ = True
    supported _ _ _ = False
instance IDatabase1 UtilsDB where
    type DBQueryType UtilsDB = (Set Var, Formula, Set Var)
    translateQuery _ vars qu vars2 = return (vars, qu, vars2)

instance INoConnectionDatabase2 UtilsDB where
    type NoConnectionQueryType UtilsDB = (Set Var, Formula, Set Var)
    type NoConnectionRowType UtilsDB = MapResultRow
    noConnectionDBStmtExec (UtilsDB _) (_,  (FAtomic (Atom (SleepPredName _) [qu])), _) stream = do
        row <- stream
        let (IntValue i) = evalExpr row qu
        liftIO $ threadDelay i
        return mempty

    noConnectionDBStmtExec (UtilsDB _) qu stream = error ("dqdb: unsupported Formula " ++ show qu)
