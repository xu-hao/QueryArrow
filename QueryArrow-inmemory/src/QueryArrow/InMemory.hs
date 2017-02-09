{-# LANGUAGE TypeFamilies, MultiParamTypeClasses, FlexibleInstances, FlexibleContexts, PatternSynonyms, DeriveGeneric, RankNTypes, GADTs #-}
module QueryArrow.InMemory where

import QueryArrow.FO.Data
import QueryArrow.DB.DB
import QueryArrow.DB.NoConnection
import QueryArrow.Plugin
import QueryArrow.Binding.Binding
import QueryArrow.Data.Heterogeneous.List
import QueryArrow.DB.AbstractDatabaseList
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
import Data.Maybe
import Debug.Trace


-- example MapDB


data MapDB = MapDB String String String [(ResultValue, ResultValue)] deriving Show

instance Binding MapDB where
    dbName (MapDB name _ _ _) = name
    bindingPred (MapDB _ ns predname _) = Pred (QPredName ns [] predname) (PredType ObjectPred [ParamType True True True TextType, ParamType True True True TextType])

instance BinaryBinding MapDB where
    supportII _ = True
    supportIO _ = True
    supportOO _ = True
    supportOI _ = True
    supportInsertII _ = False
    supportDeleteII _ = False

instance EffectFreeBinaryBinding MapDB where
    execII (MapDB _ _ _ rows) _ aval bval =
      if (aval, bval) `elem` rows
        then [mempty]
        else []
    execIO (MapDB _ _ _ rows) _ aval _ =
        [snd x | x <- rows, fst x == aval]
    execOI (MapDB _ _ _ rows) _ _ bval =
        [fst x | x <- rows, snd x == bval]
    execOO (MapDB _ _ _ rows) _ _ _ = rows

-- update mapdb

instance Show (IORef a) where
    show _ = "ioref"

data StateMapDB = StateMapDB String String String (IORef [(ResultValue, ResultValue)]) deriving Show

instance Binding StateMapDB where
    dbName (StateMapDB name _ _ _) = name
    bindingPred (StateMapDB _ ns predname _) = Pred (QPredName ns [] predname) (PredType ObjectPred [ParamType True True True TextType, ParamType True True True TextType])

instance BinaryBinding StateMapDB where
    supportII _ = True
    supportIO _ = True
    supportOO _ = True
    supportOI _ = True
    supportInsertII _ = True
    supportDeleteII _ = True

instance EffectfulBinaryBinding StateMapDB where
    execIIE (StateMapDB _ _ _ map1) _ aval bval = do
      rows <- liftIO $ readIORef map1
      return (if (aval, bval) `elem` rows
        then [mempty]
        else [])
    execIOE (StateMapDB _ _ _ map1) _ aval _ = do
      rows <- liftIO $ readIORef map1
      return [snd x | x <- rows, fst x == aval]
    execOIE (StateMapDB _ _ _ map1) _ _ bval = do
      rows <- liftIO $ readIORef map1
      return [fst x | x <- rows, snd x == bval]
    execOOE (StateMapDB _ _ _ map1) _ _ _ =
      liftIO $ readIORef map1
    insertIIE (StateMapDB _ _ _ map1) _ aval bval = do
        rows <- liftIO $ readIORef map1
        let add rows1 row = rows1 `union` [row]
        let rows2 = add rows (aval, bval)
        liftIO $ writeIORef map1 rows2
    deleteIIE (StateMapDB _ _ _ map1) _ aval bval = do
        rows <- liftIO $ readIORef map1
        let remove rows1 row = rows1 \\ [row]
        let rows2 = remove rows (aval, bval)
        liftIO $ writeIORef map1 rows2

-- example LikeRegexDB

likeRegexDB :: String -> String -> BinaryBoolean
likeRegexDB n ns = BinaryBoolean n ns "like_regex" TextType TextType (\(StringValue a) (StringValue b) -> T.unpack a =~ T.unpack b)

-- example NotLikeRegexDB

notLikeRegexDB :: String -> String -> BinaryBoolean
notLikeRegexDB n ns = BinaryBoolean n ns "not_like_regex" TextType TextType (\(StringValue a) (StringValue b) -> not (T.unpack a =~ T.unpack b))

-- example EqDB

eqDB :: String -> String -> UnaryIso
eqDB n ns = UnaryIso n ns "eq" (TypeVar "a") (TypeVar "a") id id

-- example NeDB

neDB :: String -> String -> BinaryBoolean
neDB n ns = BinaryBoolean n ns "ne" (TypeVar "a") (TypeVar "a") (/=)

-- example LeDB

leDB :: String -> String -> BinaryBoolean
leDB n ns = BinaryBoolean n ns "le" NumberType NumberType (<=)

-- example GeDB

geDB :: String -> String -> BinaryBoolean
geDB n ns = BinaryBoolean n ns "ge" NumberType NumberType (/=)

-- example LtDB

ltDB :: String -> String -> BinaryBoolean
ltDB n ns = BinaryBoolean n ns "lt" NumberType NumberType (<)

-- example GtDB

gtDB :: String -> String -> BinaryBoolean
gtDB n ns = BinaryBoolean n ns "gt" NumberType NumberType (>)

-- example SleepDB

sleepDB :: String -> String -> UnaryProcedure
sleepDB n ns = UnaryProcedure n ns "sleep" NumberType  (\ (IntValue i) -> threadDelay i)

-- example EncodeDB

encodeDB :: String -> String -> BinaryParamIso
encodeDB n ns = BinaryParamIso n ns "encode" TextType TextType ByteStringType (\(StringValue bs) (StringValue codec0) ->
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

strlenDB :: String -> String -> UnaryFunction
strlenDB n ns = UnaryFunction n ns "strlen" TextType NumberType ( \(StringValue s) -> IntValue (T.length s))

-- example AddDB

addDB :: String -> String -> BinaryIso
addDB n ns = BinaryIso n ns "add" NumberType NumberType NumberType (+) (\a c -> c - a) (\b c -> c - b)

-- example ExpDB

expDB :: String -> String -> BinaryFunction
expDB n ns = BinaryFunction n ns "exp" NumberType NumberType NumberType (^)

-- example MulDB

mulDB :: String -> String -> BinaryMono
mulDB n ns = BinaryMono n ns "mul" NumberType NumberType NumberType (*) (\a c ->
                                if c `mod` a == 0
                                  then Just (c `div` a)
                                  else Nothing) (\b c ->
                                if c `mod` b == 0
                                    then Just (c `div` b)
                                    else Nothing)

-- example DivDB

divDB :: String -> String -> BinaryFunction
divDB n ns = BinaryFunction n ns "div" NumberType NumberType NumberType div

-- example ModDB

modDB :: String -> String -> BinaryFunction
modDB n ns = BinaryFunction n ns "mod" NumberType NumberType NumberType mod

-- example SubDB

subDB :: String -> String -> BinaryIso
subDB n ns = BinaryIso n ns "sub" NumberType NumberType NumberType (-) (-) (+)

-- example ConcatDB

concatDB :: String -> String -> BinaryMono
concatDB n ns = BinaryMono n ns "concat" NumberType NumberType NumberType (\(StringValue a) (StringValue b) -> StringValue (T.append a b))
                    (\(StringValue b) (StringValue c) ->
                        if T.length c >= T.length b && T.drop (T.length c - T.length b) c == b
                            then Just (StringValue (T.take (T.length c - T.length b) c))
                            else Nothing) (\(StringValue a) (StringValue c) ->
                        if T.length c >= T.length a && T.take (T.length a) c == a
                            then Just (StringValue (T.drop (T.length a) c))
                            else Nothing)

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
