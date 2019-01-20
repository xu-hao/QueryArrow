{-# LANGUAGE TypeFamilies, MultiParamTypeClasses, FlexibleInstances, FlexibleContexts, PatternSynonyms, DeriveGeneric, RankNTypes, GADTs #-}
module QueryArrow.DB.InMemory.BuiltIn where

import QueryArrow.Syntax.Term
import QueryArrow.Syntax.Type
import QueryArrow.Semantics.TypeChecker
import QueryArrow.Semantics.Value
import QueryArrow.DB.DB
import QueryArrow.DB.NoConnection
import QueryArrow.Plugin
import QueryArrow.DB.Binding.Binding
import QueryArrow.Config

import Prelude  hiding (lookup)
import Text.Regex.TDFA ((=~))
import Data.Text (unpack)
import qualified Data.Text as T
import Control.Concurrent (threadDelay)
import Data.Text.Encoding
import Data.Aeson
import GHC.Generics
import Text.Regex (subRegex, mkRegex)

-- example LikeRegexDB

likeRegexBinding :: String -> BinaryBoolean
likeRegexBinding ns = BinaryBoolean ns "like_regex" TextType TextType (\(StringValue a) (StringValue b) -> T.unpack a =~ T.unpack b)

-- example NotLikeRegexDB

notLikeRegexBinding :: String -> BinaryBoolean
notLikeRegexBinding ns = BinaryBoolean ns "not_like_regex" TextType TextType (\(StringValue a) (StringValue b) -> not (T.unpack a =~ T.unpack b))

-- example LikeRegexDB

wildcardToRegex :: Char -> String -> String
wildcardToRegex esc wc = ('^' : concatMap (\c -> case c of
                                        '%' -> ".*"
                                        '_' -> "."
                                        '^' -> "\\^"
                                        _ -> ['[',  c, ']']) wc) ++ "$"

likeBinding :: String -> BinaryBoolean
likeBinding ns = BinaryBoolean ns "like" TextType TextType (\(StringValue a) (StringValue b) -> T.unpack a =~ wildcardToRegex '\\' (T.unpack b))

-- example NotLikeRegexDB

notLikeBinding :: String -> BinaryBoolean
notLikeBinding ns = BinaryBoolean ns "not_like" TextType TextType (\(StringValue a) (StringValue b) -> not (T.unpack a =~ wildcardToRegex '\\' (T.unpack b)))

-- example EqDB

eqBinding :: String -> UnaryIso
eqBinding ns = UnaryIso ns "eq" (TypeVar "a") (TypeVar "a") id id

-- example NeDB

neBinding :: String -> BinaryBoolean
neBinding ns = BinaryBoolean ns "ne" (TypeVar "a") (TypeVar "a") (/=)

-- example LeDB

leBinding :: String -> BinaryBoolean
leBinding ns = BinaryBoolean ns "le" Int64Type Int64Type (<=)

-- example GeDB

geBinding :: String -> BinaryBoolean
geBinding ns = BinaryBoolean ns "ge" Int64Type Int64Type (/=)

-- example LtDB

ltBinding :: String -> BinaryBoolean
ltBinding ns = BinaryBoolean ns "lt" Int64Type Int64Type (<)

-- example GtDB

gtBinding :: String -> BinaryBoolean
gtBinding ns = BinaryBoolean ns "gt" Int64Type Int64Type (>)

-- example SleepDB

sleepBinding :: String -> UnaryProcedure
sleepBinding ns = UnaryProcedure ns "sleep" Int64Type  (\ (Int64Value i) -> threadDelay (fromIntegral i))

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
strlenBinding ns = UnaryFunction ns "strlen" TextType Int64Type ( \(StringValue s) -> Int64Value (fromIntegral (T.length s)))

-- example AddDB

addBinding :: String -> BinaryIso
addBinding ns = BinaryIso ns "add" Int64Type Int64Type Int64Type (+) (\a c -> c - a) (\b c -> c - b)

-- example ExpDB

expBinding :: String -> BinaryFunction
expBinding ns = BinaryFunction ns "exp" Int64Type Int64Type Int64Type (^)

-- example MulDB

mulBinding :: String -> BinaryMono
mulBinding ns = BinaryMono ns "mul" Int64Type Int64Type Int64Type (*) (\a c ->
                                if c `mod` a == 0
                                  then Just (c `div` a)
                                  else Nothing) (\b c ->
                                if c `mod` b == 0
                                    then Just (c `div` b)
                                    else Nothing)

-- example DivDB

divBinding :: String -> BinaryFunction
divBinding ns = BinaryFunction ns "div" Int64Type Int64Type Int64Type div

-- example ModDB

modBinding :: String -> BinaryFunction
modBinding ns = BinaryFunction ns "mod" Int64Type Int64Type Int64Type mod

-- example SubDB

subBinding :: String -> BinaryIso
subBinding ns = BinaryIso ns "sub" Int64Type Int64Type Int64Type (-) (-) (+)

-- example substrBinding

substrBinding :: String -> TernaryFunction
substrBinding ns = TernaryFunction ns "substr" TextType Int64Type Int64Type TextType (\(StringValue a) (Int64Value b) (Int64Value c) -> StringValue (T.drop (fromIntegral b) (T.take (fromIntegral c) a)))

-- example replaceBinding

replaceBinding :: String -> TernaryFunction
replaceBinding ns = TernaryFunction ns "replace" TextType TextType TextType TextType (\(StringValue a) (StringValue b) (StringValue c) -> StringValue (T.replace b c a))

-- example regexReplaceBinding

regexReplaceBinding :: String -> TernaryFunction
regexReplaceBinding ns = TernaryFunction ns "regex_replace" TextType TextType TextType TextType (\(StringValue a) (StringValue b) (StringValue c) -> StringValue (T.pack (subRegex (mkRegex (T.unpack b)) (T.unpack c) (T.unpack a))))

-- example inBinding

inBinding :: String -> BinaryBoolean
inBinding ns = BinaryBoolean ns "in" TextType TextType (\(StringValue a) (StringValue b) -> not (T.null (snd (T.breakOn (T.snoc (T.cons '\'' a) '\'') b))))

-- example ConcatDB

concatBinding :: String -> BinaryMono
concatBinding ns = BinaryMono ns "concat" TextType TextType TextType (\(StringValue a) (StringValue b) -> StringValue (T.append a b))
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
                                AbstractBinding (geBinding ns),
                                AbstractBinding (leBinding ns),
                                AbstractBinding (gtBinding ns),
                                AbstractBinding (ltBinding ns),
                                AbstractBinding (eqBinding ns),
                                AbstractBinding (neBinding ns),
                                AbstractBinding (likeBinding ns),
                                AbstractBinding (notLikeBinding ns),
                                AbstractBinding (likeRegexBinding ns),
                                AbstractBinding (notLikeRegexBinding ns),
                                AbstractBinding (replaceBinding ns),
                                AbstractBinding (regexReplaceBinding ns),
                                AbstractBinding (concatBinding ns),
                                AbstractBinding (strlenBinding ns),
                                AbstractBinding (substrBinding ns),
                                AbstractBinding (inBinding ns),
                                AbstractBinding (encodeBinding ns),
                                AbstractBinding (sleepBinding ns)]

data ICATDBInfo = ICATDBInfo {
  db_namespace :: String
} deriving (Show, Generic)

instance ToJSON ICATDBInfo
instance FromJSON ICATDBInfo

data NoConnectionDatabasePlugin db = (IDatabase0 db, IDatabase1 db, INoConnectionDatabase2 db, DBQueryType db ~ NoConnectionQueryType db, NoConnectionRowType db ~ MapResultRow, DBFormulaType db ~ DBFormulaType1 db, DBFormulaType db ~ FormulaT) => NoConnectionDatabasePlugin (String -> String -> db)

instance Plugin (NoConnectionDatabasePlugin db) MapResultRow where
  getDB (NoConnectionDatabasePlugin db) _ ps = do
      let fsconf = getDBSpecificConfig ps
      return (AbstractDatabase (NoConnectionDatabase (db (qap_name ps) (db_namespace fsconf))))


builtInPlugin :: NoConnectionDatabasePlugin BindingDatabase
builtInPlugin = NoConnectionDatabasePlugin builtInDB
