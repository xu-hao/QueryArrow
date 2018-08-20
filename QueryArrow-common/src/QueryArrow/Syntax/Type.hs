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

-- predicate kinds
data PredKind = ObjectPred | PropertyPred deriving (Eq, Ord, Show, Read)

-- predicate types
data PredType = PredType {predKind :: PredKind, paramsType :: [ParamType]} deriving (Eq, Ord, Show, Read)

data ParamType = ParamType {isKey :: Bool,  isInput :: Bool,  isOutput :: Bool, isReference:: Bool, paramType :: CastType} deriving (Eq, Ord, Show, Read)

keyComponents :: PredType -> [a] -> [a]
keyComponents (PredType _ paramtypes) = map snd . filter (\(ParamType type1 _ _ _ _, _) -> type1) . zip paramtypes

propComponents :: PredType -> [a] -> [a]
propComponents (PredType _ paramtypes) = map snd . filter (\(ParamType type1 _ _ _ _, _) -> not type1) . zip paramtypes

keyComponentsParamType :: PredType -> [a] -> [(ParamType, a)]
keyComponentsParamType (PredType _ paramtypes) = filter (\(ParamType type1 _ _ _ _, _) -> type1) . zip paramtypes

propComponentsParamType :: PredType -> [a] -> [(ParamType, a)]
propComponentsParamType (PredType _ paramtypes) = filter (\(ParamType type1 _ _ _ _, _) -> not type1) . zip paramtypes

outputComponents :: PredType -> [a] -> [a]
outputComponents (PredType _ paramtypes) = map snd . filter (\(ParamType  _ _ type1 _ _, _) -> type1) . zip paramtypes

outputOnlyComponents :: PredType -> [a] -> [a]
outputOnlyComponents (PredType _ paramtypes) = map snd . filter (\(ParamType  _ type1 _ _ _, _) -> not type1) . zip paramtypes

