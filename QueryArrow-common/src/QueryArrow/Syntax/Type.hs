{-# LANGUAGE PatternSynonyms #-} 

module QueryArrow.Syntax.Type where
    
-- types
data CastType = AnyType | TypeCons String | TypeApp CastType CastType | TypeVar String | TypeUniv String CastType deriving (Eq, Ord, Show, Read)
pattern TextType = TypeCons "text"
pattern Int64Type = TypeCons "int64"
pattern Int32Type = TypeCons "int32"
pattern ByteStringType = TypeCons "bytestring"
pattern ListType a = TypeApp (TypeCons "[]") a
pattern FuncType a b = TypeApp (TypeApp (TypeCons "->") a) b
