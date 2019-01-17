module QueryArrow.Semantics.Compute where

import QueryArrow.Syntax.Term
import QueryArrow.DB.DB
import QueryArrow.Semantics.Value
import QueryArrow.Syntax.Type

import Prelude  hiding (lookup)
import Data.Map.Strict (lookup)
import Data.Text (pack, unpack)
import Data.Text.Encoding

        
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