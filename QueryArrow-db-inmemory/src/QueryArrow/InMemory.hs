{-# LANGUAGE TypeFamilies, MultiParamTypeClasses, FlexibleInstances, FlexibleContexts, PatternSynonyms, DeriveGeneric, RankNTypes, GADTs #-}
module QueryArrow.InMemory where

import QueryArrow.FO.Data
import QueryArrow.DB.DB
import QueryArrow.DB.NoConnection
import QueryArrow.Plugin
import QueryArrow.Binding.Binding
import QueryArrow.Config

import Prelude  hiding (lookup)
import Data.List ((\\), union)
import Data.Convertible.Base
import Text.Regex.TDFA ((=~))
import Control.Monad.IO.Class
import Data.Text (unpack)
import qualified Data.Text as T
import Control.Concurrent (threadDelay)
import Data.IORef
import Data.Text.Encoding
import Data.Aeson
import GHC.Generics
import Text.Regex (subRegex, mkRegex)
import Debug.Trace


-- example MapDB


data MapBinding = MapBinding String String [(ResultValue, ResultValue)] deriving Show
instance Binding MapBinding where
    bindingPred (MapBinding ns predname _) = Pred (QPredName ns [] predname) (PredType ObjectPred [ParamType True True True TextType, ParamType True True True TextType])
    bindingSupport _ [_,_] = True
    bindingSupport _ _ = False
    bindingSupportInsert _ = False
    bindingSupportDelete _ = False
    bindingExec (MapBinding _ _ rows) [I,I] [aval, bval] =
      return (if (aval, bval) `elem` rows
        then [[]]
        else [])
    bindingExec (MapBinding _ _ rows) [I,O] [aval] =
        return [[snd x] | x <- rows, fst x == aval]
    bindingExec (MapBinding _ _ rows) [O,I] [bval] =
        return [[fst x] | x <- rows, snd x == bval]
    bindingExec (MapBinding _ _ rows) [O,O] [] =
        return (map (\(a,b)->[a,b]) rows)

mapDB :: String -> String -> String -> [(ResultValue, ResultValue)] -> BindingDatabase
mapDB dbname ns n rows = BindingDatabase dbname [AbstractBinding (MapBinding ns n rows)]
-- update mapdb

instance Show (IORef a) where
    show _ = "ioref"

data StateMapBinding = StateMapBinding String String (IORef [(ResultValue, ResultValue)]) deriving Show

instance Binding StateMapBinding where
    bindingPred (StateMapBinding ns predname _) = Pred (QPredName ns [] predname) (PredType ObjectPred [ParamType True True True TextType, ParamType True True True TextType])
    bindingSupport _ [_,_] = True
    bindingSupport _ _ = False
    bindingSupportInsert _ = True
    bindingSupportDelete _ = True
    bindingExec (StateMapBinding _ _ map1) [I,I] [aval, bval] = do
      rows <- liftIO $ readIORef map1
      return (if (aval, bval) `elem` rows
        then [[]]
        else [])
    bindingExec (StateMapBinding _ _ map1) [I,O] [aval] = do
      rows <- liftIO $ readIORef map1
      return [[snd x] | x <- rows, fst x == aval]
    bindingExec (StateMapBinding _ _ map1) [O,I] [bval] = do
      rows <- liftIO $ readIORef map1
      return [[fst x] | x <- rows, snd x == bval]
    bindingExec (StateMapBinding _ _ map1) [O,O] [] = do
      rows <- liftIO $ readIORef map1
      return (map (\(a,b)->[a,b]) rows)
    bindingInsert (StateMapBinding _ _ map1) [aval, bval] = do
        rows <- liftIO $ readIORef map1
        let add rows1 row = rows1 `union` [row]
        let rows2 = add rows (aval, bval)
        liftIO $ writeIORef map1 rows2
    bindingDelete (StateMapBinding _ _ map1) [aval, bval] = do
        rows <- liftIO $ readIORef map1
        let remove rows1 row = rows1 \\ [row]
        let rows2 = remove rows (aval, bval)
        liftIO $ writeIORef map1 rows2

stateMapDB :: String -> String -> String ->  IORef [(ResultValue, ResultValue)] -> BindingDatabase
stateMapDB dbname ns n map1 =
  BindingDatabase dbname [AbstractBinding (StateMapBinding ns n map1)]

-- example LikeRegexDB

likeRegexBinding :: String -> BinaryBoolean
likeRegexBinding ns = BinaryBoolean ns "like_regex" TextType TextType (\(StringValue a) (StringValue b) -> T.unpack a =~ T.unpack b)

-- example NotLikeRegexDB

notLikeRegexBinding :: String -> BinaryBoolean
notLikeRegexBinding ns = BinaryBoolean ns "not_like_regex" TextType TextType (\(StringValue a) (StringValue b) -> not (T.unpack a =~ T.unpack b))

-- example EqDB

eqBinding :: String -> UnaryIso
eqBinding ns = UnaryIso ns "eq" (TypeVar "a") (TypeVar "a") id id

-- example NeDB

neBinding :: String -> BinaryBoolean
neBinding ns = BinaryBoolean ns "ne" (TypeVar "a") (TypeVar "a") (/=)

-- example LeDB

leBinding :: String -> BinaryBoolean
leBinding ns = BinaryBoolean ns "le" NumberType NumberType (<=)

-- example GeDB

geBinding :: String -> BinaryBoolean
geBinding ns = BinaryBoolean ns "ge" NumberType NumberType (/=)

-- example LtDB

ltBinding :: String -> BinaryBoolean
ltBinding ns = BinaryBoolean ns "lt" NumberType NumberType (<)

-- example GtDB

gtBinding :: String -> BinaryBoolean
gtBinding ns = BinaryBoolean ns "gt" NumberType NumberType (>)

-- example SleepDB

sleepBinding :: String -> UnaryProcedure
sleepBinding ns = UnaryProcedure ns "sleep" NumberType  (\ (IntValue i) -> threadDelay i)

-- example EncodeDB

encodeBinding :: String -> BinaryParamIso
encodeBinding ns = BinaryParamIso ns "encode" TextType TextType ByteStringType (\(StringValue bs) (StringValue codec0) ->
      let codec = unpack codec0 in ByteStringValue (encoded codec bs)) (\(ByteStringValue bs3) (StringValue codec0)  ->
      let codec = unpack codec0 in StringValue (decoded codec bs3)) where
          encoded codec bs = case codec of
                    "utf8" -> encodeUtf8 bs
                    "utf16be" -> encodeUtf16BE bs
                    "utf16le" -> encodeUtf16LE bs
                    "utf32be" -> encodeUtf32BE bs
                    "utf32le" -> encodeUtf32LE bs
                    _ -> error ("unsupported encoding " ++ codec)
          decoded codec bs = case codec of
                    "utf8" -> decodeUtf8 bs
                    "utf16be" -> decodeUtf16BE bs
                    "utf16le" -> decodeUtf16LE bs
                    "utf32be" -> decodeUtf32BE bs
                    "utf32le" -> decodeUtf32LE bs
                    _ -> error ("unsupported encoding " ++ codec)

-- example StrlenDB

strlenBinding :: String -> UnaryFunction
strlenBinding ns = UnaryFunction ns "strlen" TextType NumberType ( \(StringValue s) -> IntValue (T.length s))

-- example AddDB

addBinding :: String -> BinaryIso
addBinding ns = BinaryIso ns "add" NumberType NumberType NumberType (+) (\a c -> c - a) (\b c -> c - b)

-- example ExpDB

expBinding :: String -> BinaryFunction
expBinding ns = BinaryFunction ns "exp" NumberType NumberType NumberType (^)

-- example MulDB

mulBinding :: String -> BinaryMono
mulBinding ns = BinaryMono ns "mul" NumberType NumberType NumberType (*) (\a c ->
                                if c `mod` a == 0
                                  then Just (c `div` a)
                                  else Nothing) (\b c ->
                                if c `mod` b == 0
                                    then Just (c `div` b)
                                    else Nothing)

-- example DivDB

divBinding :: String -> BinaryFunction
divBinding ns = BinaryFunction ns "div" NumberType NumberType NumberType div

-- example ModDB

modBinding :: String -> BinaryFunction
modBinding ns = BinaryFunction ns "mod" NumberType NumberType NumberType mod

-- example SubDB

subBinding :: String -> BinaryIso
subBinding ns = BinaryIso ns "sub" NumberType NumberType NumberType (-) (-) (+)

-- example substrBinding

substrBinding :: String -> TernaryFunction
substrBinding ns = TernaryFunction ns "substr" TextType NumberType NumberType TextType (\(StringValue a) (IntValue b) (IntValue c) -> StringValue (T.drop b (T.take c a)))

-- example replaceBinding

replaceBinding :: String -> TernaryFunction
replaceBinding ns = TernaryFunction ns "replace" TextType TextType TextType TextType (\(StringValue a) (StringValue b) (StringValue c) -> StringValue (T.replace b c a))

-- example regexReplaceBinding

regexReplaceBinding :: String -> TernaryFunction
regexReplaceBinding ns = TernaryFunction ns "regex_replace" TextType TextType TextType TextType (\(StringValue a) (StringValue b) (StringValue c) -> StringValue (T.pack (subRegex (mkRegex (T.unpack b)) (T.unpack c) (T.unpack a))))

-- example ConcatDB

concatBinding :: String -> BinaryMono
concatBinding ns = BinaryMono ns "concat" NumberType NumberType NumberType (\(StringValue a) (StringValue b) -> StringValue (T.append a b))
                    (\(StringValue b) (StringValue c) ->
                        if T.length c >= T.length b && T.drop (T.length c - T.length b) c == b
                            then Just (StringValue (T.take (T.length c - T.length b) c))
                            else Nothing) (\(StringValue a) (StringValue c) ->
                        if T.length c >= T.length a && T.take (T.length a) c == a
                            then Just (StringValue (T.drop (T.length a) c))
                            else Nothing)

builtInDB :: String -> String -> BindingDatabase
builtInDB dbname ns = BindingDatabase dbname [
                                AbstractBinding (addBinding ns),
                                AbstractBinding (subBinding ns),
                                AbstractBinding (divBinding ns),
                                AbstractBinding (mulBinding ns),
                                AbstractBinding (modBinding ns),
                                AbstractBinding (expBinding ns),
                                AbstractBinding (strlenBinding ns),
                                AbstractBinding (encodeBinding ns),
                                AbstractBinding (geBinding ns),
                                AbstractBinding (leBinding ns),
                                AbstractBinding (gtBinding ns),
                                AbstractBinding (ltBinding ns),
                                AbstractBinding (eqBinding ns),
                                AbstractBinding (neBinding ns),
                                AbstractBinding (likeRegexBinding ns),
                                AbstractBinding (notLikeRegexBinding ns),
                                AbstractBinding (sleepBinding ns),
                                AbstractBinding (concatBinding ns),
                                AbstractBinding (substrBinding ns)]

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
  getDB (NoConnectionDatabasePlugin db) _ ps = do
      let fsconf = getDBSpecificConfig ps
      return (AbstractDatabase (NoConnectionDatabase (db (qap_name ps) (db_namespace fsconf))))

data NoConnectionDatabasePlugin2 db a = (IDatabase0 db, IDatabase1 db, INoConnectionDatabase2 db, DBQueryType db ~ NoConnectionQueryType db, NoConnectionRowType db ~ MapResultRow, DBFormulaType db ~ Formula) => NoConnectionDatabasePlugin2 (String -> String -> String -> a -> db)

instance Convertible Value (IO a) => Plugin (NoConnectionDatabasePlugin2 db a) MapResultRow where
  getDB (NoConnectionDatabasePlugin2 db) _ ps = do
      let fsconf = getDBSpecificConfig ps
      dbdata <- convert (db_map fsconf)
      return (AbstractDatabase (NoConnectionDatabase (db (qap_name ps) (db_namespace2 fsconf) (predicate_name fsconf) dbdata)))

instance Convertible Value (IO [(ResultValue, ResultValue)]) where
  safeConvert a = case fromJSON a of
    Error err -> error err
    Success b -> Right (return (map (\(a,b) -> (StringValue a, StringValue b)) b))

instance Convertible Value (IO (IORef [(ResultValue, ResultValue)])) where
  safeConvert a = Right (convert a >>= newIORef)

builtInPlugin :: NoConnectionDatabasePlugin BindingDatabase
builtInPlugin = NoConnectionDatabasePlugin builtInDB

mapPlugin :: NoConnectionDatabasePlugin2 BindingDatabase [(ResultValue, ResultValue)]
mapPlugin = NoConnectionDatabasePlugin2 mapDB

stateMapPlugin :: NoConnectionDatabasePlugin2 BindingDatabase (IORef [(ResultValue, ResultValue)])
stateMapPlugin = NoConnectionDatabasePlugin2 stateMapDB
