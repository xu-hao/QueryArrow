{-# LANGUAGE TypeFamilies, MultiParamTypeClasses, FlexibleInstances, FlexibleContexts, PatternSynonyms, DeriveGeneric, RankNTypes, GADTs #-}
module QueryArrow.InMemory where

import QueryArrow.DB.ResultStream
import QueryArrow.FO.Data
import QueryArrow.FO.Types
import QueryArrow.FO.Utils
import QueryArrow.DB.DB
import QueryArrow.DB.NoConnection
import QueryArrow.Utils
import QueryArrow.Plugin
import QueryArrow.Data.Heterogeneous.List
import QueryArrow.DB.AbstractDatabaseList
import QueryArrow.Config

import Prelude  hiding (lookup)
import Data.Map.Strict ((!), member,   lookup, fromList,  singleton, keysSet)
import Data.List ((\\), union)
import Data.Convertible.Base
import Text.Regex.TDFA ((=~))
import Control.Monad.IO.Class
import Control.Monad
import Data.Text (unpack)
import Control.Concurrent (threadDelay)
import Data.Set (Set)
import qualified Data.Set as Set
import Data.IORef
import Data.Text.Encoding
import Data.Aeson
import GHC.Generics
import Data.Maybe
import Debug.Trace


-- example MapDB


data MapDB = MapDB String String String [(ResultValue, ResultValue)] deriving Show

instance IDatabase0 MapDB where
    type DBFormulaType MapDB = Formula

    getName (MapDB name _ _ _) = name
    getPreds (MapDB name ns predname _) = [ Pred (QPredName ns [] predname) (PredType ObjectPred [ParamType True True True TextType, ParamType True True True TextType]) ]
    supported (MapDB name ns predname _) _ (FAtomic (Atom p _)) _ | predNameMatches (QPredName ns [] predname) p = True
    supported _ _ _ _ = False
    checkQuery _ _ _ _ = return (Right ())

instance IDatabase1 MapDB where
    type DBQueryType MapDB = ( Set Var, Formula,  Set Var)
    translateQuery _ vars qu vars2 = return (vars, qu, vars2)


instance INoConnectionDatabase2 MapDB where
    type NoConnectionQueryType MapDB = (Set Var, Formula, Set Var)
    type NoConnectionRowType MapDB = MapResultRow
    noConnectionDBStmtExec (MapDB _ _ _ rows) (vars, ( (FAtomic (Atom _ args))), _) stream  = do
        row2 <- mapDBFilterResults rows stream args
        return (transform vars row2)
    noConnectionDBStmtExec _ qu _ = error ("dqdb: unsupported Formula " ++ show qu)

-- update mapdb

instance Show (IORef a) where
    show _ = "ioref"

data StateMapDB = StateMapDB String String String (IORef ([(ResultValue, ResultValue)])) deriving Show

instance IDatabase0 StateMapDB where
    type DBFormulaType StateMapDB = Formula

    getName (StateMapDB name _ _ _) = name
    getPreds (StateMapDB name ns predname _) = [ Pred (QPredName ns [] predname) (PredType ObjectPred [ParamType True True True TextType, ParamType True True True TextType]) ]
    supported _ _ (FAtomic _) _ = True
    supported _ _ (FInsert _) _ = True
    supported _ _ _ _ = False
    checkQuery _ _ _ _ = return (Right ())

instance IDatabase1 StateMapDB where
    type DBQueryType StateMapDB = ( Set Var, Formula,  Set Var)
    translateQuery _ vars qu vars2 = return (vars, qu, vars2)

instance INoConnectionDatabase2 StateMapDB where
    type NoConnectionQueryType StateMapDB = (Set Var, Formula, Set Var)
    type NoConnectionRowType StateMapDB = MapResultRow
    noConnectionDBStmtExec (StateMapDB _ _ _ map1) (vars,  FAtomic (Atom _ args), _) stream  = do
        rows <- liftIO $ readIORef map1
        row2 <- mapDBFilterResults rows stream args
        return (transform vars row2)
    noConnectionDBStmtExec (StateMapDB _ _ _ map1) (_, FInsert lit@(Lit thesign _), _) stream = do
        rows <- liftIO $ readIORef map1
        let freevars = freeVars lit
        let add rows1 row = rows1 `union` [row]
        let remove rows1 row = rows1 \\ [row]
        let arg12 row1 (Lit _ (Atom _ [a,b])) = (evalExpr row1 a, evalExpr row1 b)
            arg12 _ _ = error "wrong number of args"
        row1 <- stream
        let rows2 = (case thesign of
                        Pos -> add
                        Neg -> remove) rows (arg12 row1 lit)
        liftIO $ writeIORef map1 rows2
        return row1
    noConnectionDBStmtExec (StateMapDB _ _ _ map1) qu stream = error ("dqdb: unsupported Formula " ++ show qu)



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

data RegexDB = RegexDB String String

pattern RegexPredName ns = QPredName ns [] "like_regex"
pattern RegexPred ns = Pred (RegexPredName ns) (PredType ObjectPred [ParamType True True False TextType, ParamType True True False TextType])

instance IDatabase0 RegexDB where
    type DBFormulaType RegexDB = Formula
    getName (RegexDB name _) = name
    getPreds (RegexDB _ ns) = [ RegexPred ns]
    supported _ _ (FAtomic (Atom (RegexPredName _) _)) _ = True
    supported _ _ _ _ = False
    checkQuery _ _ _ _ = return (Right ())
instance IDatabase1 RegexDB where
    type DBQueryType RegexDB = ( Set Var, Formula,  Set Var)
    translateQuery _ vars qu vars2 = return (vars, qu, vars2)

extractStringFromExpr :: ResultValue -> String
extractStringFromExpr (StringValue s) = unpack s
extractStringFromExpr a = error "cannot extract string from nonstring"

instance INoConnectionDatabase2 RegexDB where
    type NoConnectionQueryType RegexDB = (Set Var, Formula, Set Var)
    type NoConnectionRowType RegexDB = MapResultRow
    noConnectionDBStmtExec (RegexDB _ _) (_,  (FAtomic (Atom _ [a, b])), _) stream = do
        row <- stream
        if extractStringFromExpr (evalExpr row a) =~ extractStringFromExpr (evalExpr row b)
            then return mempty
            else emptyResultStream

    noConnectionDBStmtExec (RegexDB _ _) qu stream = error ("dqdb: unsupported Formula " ++ show qu)

-- example EqDB

data EqDB = EqDB String String

pattern EqPredName ns = QPredName ns [] "eq"
pattern EqPred ns = Pred (EqPredName ns) (PredType ObjectPred [ParamType True True False (TypeVar "a"), ParamType True True True (TypeVar "a")])

instance IDatabase0 EqDB where
    type DBFormulaType EqDB = Formula
    getName (EqDB name _) = name
    getPreds (EqDB _ ns) = [ EqPred ns]
    supported _ _ (FAtomic (Atom (EqPredName _) _)) _ = True
    supported _ _ _ _ = False
    checkQuery _ _ _ _ = return (Right ())

instance IDatabase1 EqDB where
    type DBQueryType EqDB = ( Set Var, Formula,  Set Var)
    translateQuery _ vars qu vars2 = return (vars, qu, vars2)

instance INoConnectionDatabase2 EqDB where
    type NoConnectionQueryType EqDB = (Set Var, Formula, Set Var)
    type NoConnectionRowType EqDB = MapResultRow
    noConnectionDBStmtExec (EqDB _ _) (_,  (FAtomic (Atom _ [a, VarExpr b])), env) stream | not (b `Set.member` env) = do
        row <- stream
        let aval = evalExpr row a
        return (singleton b aval)
    noConnectionDBStmtExec (EqDB _ _) (_,  (FAtomic (Atom _ [a, b])), _) stream = do
        row <- stream
        if evalExpr row a == evalExpr row b
            then return mempty
            else emptyResultStream

    noConnectionDBStmtExec (EqDB _ _) qu stream = error ("dqdb: unsupported Formula " ++ show qu)

-- example UtilsDB

data UtilsDB = UtilsDB String String

pattern SleepPredName ns = QPredName ns [] "sleep"
pattern SleepPred ns = Pred (SleepPredName ns) (PredType ObjectPred [ParamType True True False NumberType])

instance IDatabase0 UtilsDB where
    type DBFormulaType UtilsDB = Formula
    getName (UtilsDB name _) = name
    getPreds (UtilsDB _ ns) = [ SleepPred ns]
    supported _ _ (FAtomic (Atom (SleepPredName _) [_])) _ = True
    supported _ _ _ _ = False
    checkQuery _ _ _ _ = return (Right ())
instance IDatabase1 UtilsDB where
    type DBQueryType UtilsDB = (Set Var, Formula, Set Var)
    translateQuery _ vars qu vars2 = return (vars, qu, vars2)

instance INoConnectionDatabase2 UtilsDB where
    type NoConnectionQueryType UtilsDB = (Set Var, Formula, Set Var)
    type NoConnectionRowType UtilsDB = MapResultRow
    noConnectionDBStmtExec (UtilsDB _ _) (_,  (FAtomic (Atom (SleepPredName _) [qu])), _) stream = do
        row <- stream
        let (IntValue i) = evalExpr row qu
        liftIO $ threadDelay i
        return mempty

    noConnectionDBStmtExec (UtilsDB _ _) qu stream = error ("dqdb: unsupported Formula " ++ show qu)

-- example TextDB

data TextDB = TextDB String String

pattern EncodePredName ns = QPredName ns [] "encode"
pattern EncodePred ns = Pred (EncodePredName ns) (PredType PropertyPred [PTKeyIO TextType, PTKeyI TextType, PTPropIO ByteStringType])

instance IDatabase0 TextDB where
    type DBFormulaType TextDB = Formula
    getName (TextDB name ns) = name
    getPreds (TextDB name ns) = [ EncodePred ns]
    supported _ _ (FAtomic (Atom (EncodePredName _) [_, _, VarExpr var])) env | var `Set.member` env = True
    supported _ _ (FAtomic (Atom (EncodePredName _) [VarExpr var, _, _])) env | var `Set.member` env = True
    supported _ _ _ _ = False
    checkQuery _ _ _ _ = return (Right ())
instance IDatabase1 TextDB where
    type DBQueryType TextDB = (Set Var, Formula, Set Var)
    translateQuery _ vars qu vars2 = return (vars, qu, vars2)

instance INoConnectionDatabase2 TextDB where
    type NoConnectionQueryType TextDB = (Set Var, Formula, Set Var)
    type NoConnectionRowType TextDB = MapResultRow
    noConnectionDBStmtExec (TextDB _ _) (_,  (FAtomic (Atom (EncodePredName _) [arg1, arg2, arg3])), env) stream = do
        row <- stream
        let (StringValue codec0) = evalExpr row arg2
            codec = unpack codec0
            val1 = evalExpr row arg1
            val3 = evalExpr row arg3
            decoded =
                let (ByteStringValue bs) = val3
                in
                    case codec of
                      "utf8" -> decodeUtf8 bs
                      "utf16be" -> decodeUtf16BE bs
                      "utf16le" -> decodeUtf16LE bs
                      "utf32be" -> decodeUtf32BE bs
                      "utf32le" -> decodeUtf32LE bs
            encoded =
                let (StringValue bs) = val1
                in
                    case codec of
                      "utf8" -> encodeUtf8 bs
                      "utf16be" -> encodeUtf16BE bs
                      "utf16le" -> encodeUtf16LE bs
                      "utf32be" -> encodeUtf32BE bs
                      "utf32le" -> encodeUtf32LE bs
        case arg1 of
          VarExpr var1 | not (var1 `Set.member` env) ->
            return (singleton var1 (StringValue decoded))
          _ ->
            case arg3 of
              VarExpr var3 | not (var3 `Set.member` env) ->
                return (singleton var3 (ByteStringValue encoded))
              _ -> do
                let (ByteStringValue bs3) = val3
                if bs3 == encoded
                  then
                    return mempty
                  else
                    emptyResultStream



    noConnectionDBStmtExec (TextDB _ _) qu stream = error ("dqdb: unsupported Formula " ++ show qu)



data ICATDBInfo = ICATDBInfo {
  db_namespace :: String
} deriving (Show, Generic)

instance ToJSON ICATDBInfo
instance FromJSON ICATDBInfo


data ICATMapDBInfo = ICATMapDBInfo {
  db_namespace2 :: String,
  predicate_name :: String,
  db_map:: Value
} deriving (Show, Generic)

instance ToJSON ICATMapDBInfo
instance FromJSON ICATMapDBInfo

data NoConnectionDatabasePlugin db = (IDatabase0 db, IDatabase1 db, INoConnectionDatabase2 db, DBQueryType db ~ NoConnectionQueryType db, NoConnectionRowType db ~ MapResultRow, DBFormulaType db ~ Formula) => NoConnectionDatabasePlugin (String -> String -> db)

instance Plugin (NoConnectionDatabasePlugin db) MapResultRow where
  getDB (NoConnectionDatabasePlugin db) ps (AbstractDBList HNil) =
    case fromJSON (fromJust (db_config ps)) of
      Error err -> error err
      Success fsconf ->
        return (AbstractDatabase (NoConnectionDatabase (db (qap_name ps) (db_namespace fsconf))))

data NoConnectionDatabasePlugin2 db a = (IDatabase0 db, IDatabase1 db, INoConnectionDatabase2 db, DBQueryType db ~ NoConnectionQueryType db, NoConnectionRowType db ~ MapResultRow, DBFormulaType db ~ Formula) => NoConnectionDatabasePlugin2 (String -> String -> String -> a -> db)

instance Convertible Value (IO a) => Plugin (NoConnectionDatabasePlugin2 db a) MapResultRow where
  getDB (NoConnectionDatabasePlugin2 db) ps (AbstractDBList HNil) =
    case fromJSON (fromJust (db_config ps)) of
      Error err -> error err
      Success fsconf -> do
        dbdata <- convert (db_map fsconf)
        return (AbstractDatabase (NoConnectionDatabase (db (qap_name ps) (db_namespace2 fsconf) (predicate_name fsconf) dbdata)))

instance Convertible Value (IO [(ResultValue, ResultValue)]) where
  safeConvert a = case fromJSON a of
    Error err -> error err
    Success b -> Right (return (map (\(a,b) -> (StringValue a, StringValue b)) b))

instance Convertible Value (IO (IORef [(ResultValue, ResultValue)])) where
  safeConvert a = Right (convert a >>= newIORef)
