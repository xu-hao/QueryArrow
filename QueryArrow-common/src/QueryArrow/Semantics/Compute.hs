module QueryArrow.Semantics.Compute where

import QueryArrow.DB.ResultStream
import QueryArrow.Syntax.Term
import QueryArrow.ListUtils
import QueryArrow.DB.DB
import QueryArrow.Semantics.Value
import QueryArrow.Syntax.Type

import Prelude  hiding (lookup)
import Data.Map.Strict (Map, empty, insert, alter, lookup, mapKeys)
import qualified Data.Map.Strict as M
import Data.List ((\\), intercalate, transpose)
import Data.Maybe
import Data.Tree
import Data.Text (Text, pack, unpack)
import Data.Text.Encoding
import Data.Int (Int64)

        
evalExpr :: MapResultRow -> Expr -> ResultValue
evalExpr _ (StringExpr s) =  (StringValue s)
evalExpr _ (IntExpr s) =  (Int64Value (fromIntegral s))
evalExpr row (VarExpr v) = case lookup v row of
    Nothing ->  Null
    Just r -> r
evalExpr row (CastExpr TextType e) =
     (StringValue (case evalExpr row e of
        Int64Value i -> pack (show i)
        StringValue s -> s
        ByteStringValue bs -> decodeUtf8 bs
        _ -> error "cannot cast"))
evalExpr row (CastExpr ByteStringType e) =
     (ByteStringValue (case evalExpr row e of
        Int64Value i -> encodeUtf8 (pack (show i))
        StringValue s -> encodeUtf8 s
        ByteStringValue bs -> bs
        _ -> error "cannot cast"))
evalExpr row (CastExpr Int64Type e) =
     (Int64Value (case evalExpr row e of
        Int64Value i -> i
        StringValue s -> read (unpack s)
        ByteStringValue bs -> read (unpack (decodeUtf8 bs))
        _ -> error "cannot cast"))


evalExpr _ expr = error ("evalExpr: unsupported expr " ++ show expr)