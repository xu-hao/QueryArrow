{-# LANGUAGE TypeFamilies, MultiParamTypeClasses, ExistentialQuantification, FlexibleInstances, StandaloneDeriving, DeriveFunctor, UndecidableInstances, DeriveGeneric,
   RankNTypes, FlexibleContexts, GADTs, PatternSynonyms, ScopedTypeVariables #-}

module QueryArrow.Syntax.Data where

import Prelude hiding (lookup)
import Data.Map.Strict (Map, empty, insert, alter, lookup, fromList, foldrWithKey)
import Data.List (intercalate, union, unwords)
import Control.Monad.Trans.State.Strict (evalState,get, put, State)
import Data.Set (Set, singleton)
import qualified Data.Set as Set
import Data.Maybe
import qualified Data.Map as Map
import qualified Data.Text as T
import Data.Namespace.Path
import Data.Namespace.Namespace
import Algebra.Lattice
import Control.Monad (foldM)
import Data.Monoid ((<>))
import GHC.Generics


-- predicate kinds
data PredKind = ObjectPred | PropertyPred deriving (Eq, Ord, Show, Read)

-- predicate types
data PredType = PredType PredKind [ParamType] deriving (Eq, Ord, Show, Read)

data ParamType = ParamType {isKey :: Bool,  isInput :: Bool,  isOutput :: Bool, paramType:: CastType} deriving (Eq, Ord, Show, Read)

-- predicate
type PredName = ObjectPath String

deriving instance (Key a, Read a) => Read (ObjectPath a)
deriving instance (Key a, Read a) => Read (NamespacePath a)

data Pred = Pred {  predName :: PredName, predType :: PredType} deriving (Eq, Ord, Show, Read)

-- variables
newtype Var = Var {unVar :: String} deriving (Eq, Ord, Show, Read)

-- types
data GenCastType a = TextType | Int64Type | Int32Type | RefType String | ByteStringType | TypeVar a deriving (Eq, Ord, Show, Read, Generic)

type CastType = GenCastType String
-- http://stackoverflow.com/questions/27157717/boilerplate-free-annotation-of-asts-in-haskell
newtype Tie f = Tie (f (Tie f))

data Annotated a f = Annotated a (f (Annotated a f))

class Unannotate (f :: (* -> *) -> *) (g :: * -> *) where
  unannotate :: f g -> g (f g)
  mapA :: (forall a . g a -> g a) ->  f g -> f g

instance Functor f => Unannotate Tie f where
  unannotate (Tie a) = a
  mapA f (Tie a) = Tie (f (fmap (mapA f) a))

instance Functor f => Unannotate (Annotated a) f where
  unannotate (Annotated _ a) = a
  mapA f (Annotated a b) = Annotated a (f (fmap (mapA f) b))

-- expression
data Expr0 a = VarExpr0 Var | IntExpr0 Integer | StringExpr0 T.Text | PatternExpr0 T.Text | NullExpr0 | CastExpr0 CastType a deriving (Eq, Ord, Show, Read, Functor)

type Expr1 a = a Expr0

type Expr = Expr1 Tie

deriving instance Eq Expr
deriving instance Ord Expr
deriving instance Show Expr
deriving instance Read Expr

pattern VarExpr a = Tie (VarExpr0 a)
pattern IntExpr a = Tie (IntExpr0 a)
pattern StringExpr a = Tie (StringExpr0 a)
pattern PatternExpr a = Tie (PatternExpr0 a)
pattern NullExpr = Tie NullExpr0
pattern CastExpr a b = Tie (CastExpr0 a b)

-- atoms
data Atom1 a = Atom1 { atomPred :: PredName, atomArgs :: [Expr1 a] }

type Atom = Atom1 Tie

pattern Atom a b = Atom1 a b

deriving instance Eq (Expr1 a) => Eq (Atom1 a)
deriving instance Ord (Expr1 a) => Ord (Atom1 a)
deriving instance Show (Expr1 a) => Show (Atom1 a)
deriving instance Read (Expr1 a) => Read (Atom1 a)

-- sign of literal
data Sign = Pos | Neg deriving (Eq, Ord, Show, Read)

-- literals
data Lit1 a = Lit1 { sign :: Sign,  atom :: Atom1 a }

type Lit = Lit1 Tie

pattern Lit a b = Lit1 a b

deriving instance Eq (Expr1 a) => Eq (Lit1 a)
deriving instance Ord (Expr1 a) => Ord (Lit1 a)
deriving instance Show (Expr1 a) => Show (Lit1 a)
deriving instance Read (Expr1 a) => Read (Lit1 a)

data Summary = Max Var | Min Var | Sum Var | Average Var | CountDistinct Var | Count deriving (Eq, Ord, Show, Read)
data Aggregator = Summarize [(Var, Summary)] [Var] | Limit Int | OrderByAsc Var | OrderByDesc Var | Distinct | Not | Exists | FReturn [Var] deriving (Eq, Ord, Show, Read)

data Formula0 a f = FAtomic0 (Atom1 a)
             | FInsert0 (Lit1 a)
             | FChoice0 f f
             | FSequencing0 f f
             | FPar0 f f
             | FOne0
             | FZero0
             | Aggregate0 Aggregator f deriving Functor

deriving instance (Eq f, Eq (Expr1 a)) => Eq (Formula0 a f)
deriving instance (Ord f, Ord (Expr1 a)) => Ord (Formula0 a f)
deriving instance (Show f, Show (Expr1 a)) => Show (Formula0 a f)
deriving instance (Read f, Read (Expr1 a)) => Read (Formula0 a f)

deriving instance Eq (Formula)
deriving instance Ord (Formula)
deriving instance Show (Formula)
deriving instance Read (Formula)

type Formula1 a b = b (Formula0 a)

type Formula = Formula1 Tie Tie

pattern FAtomic a = Tie (FAtomic0 a)
pattern FInsert a = Tie (FInsert0 a)
pattern FChoice a b = Tie (FChoice0 a b)
pattern FSequencing a b = Tie (FSequencing0 a b)
pattern FPar a b = Tie (FPar0 a b)
pattern FZero = Tie FZero0
pattern FOne = Tie FOne0
pattern Aggregate a b = Tie (Aggregate0 a b)

pattern FAtomic2 d a = Annotated d (FAtomic0 a)
pattern FInsert2 d a = Annotated d (FInsert0 a)
pattern FChoice2 d a b = Annotated d (FChoice0 a b)
pattern FSequencing2 d a b = Annotated d (FSequencing0 a b)
pattern FPar2 d a b = Annotated d (FPar0 a b)
pattern FZero2 d = Annotated d FZero0
pattern FOne2 d = Annotated d FOne0
pattern Aggregate2 d a b = Annotated d (Aggregate0 a b)

{-
isConst :: Expr -> Bool
isConst (IntExpr _) = True
isConst (StringExpr _) = True
isConst _ = False
-}
-- free variables


class FreeVars a where
    freeVars :: a -> Set Var

instance Unannotate a Expr0 => FreeVars (Expr1 a) where
    freeVars e1 = case unannotate e1 of
      CastExpr0 _ e -> freeVars e
      VarExpr0 var0 -> Set.singleton var0
      IntExpr0 _ -> bottom
      StringExpr0 _ -> bottom
      PatternExpr0 _ -> bottom
      NullExpr0 -> bottom

instance Unannotate a Expr0 => FreeVars (Atom1 a) where
    freeVars (Atom _ theargs) = freeVars theargs

instance Unannotate a Expr0 => FreeVars (Lit1 a) where
    freeVars (Lit _ theatom) = freeVars theatom

instance (Unannotate a Expr0, Unannotate b (Formula0 a)) => FreeVars (Formula1 a b) where
    freeVars f1 = case unannotate f1 of
      FAtomic0 atom1 ->
        freeVars atom1
      FInsert0 lits ->
        freeVars lits
      FSequencing0 form1 form2 ->
        freeVars form1 \/ freeVars form2
      FChoice0 form1 form2 ->
        freeVars form1 \/ freeVars form2
      FPar0 form1 form2 ->
        freeVars form1 \/ freeVars form2
      Aggregate0 agg form ->
        freeVars agg \/ freeVars form
      FZero0 -> bottom
      FOne0 -> bottom

instance FreeVars Aggregator where
    freeVars agg = case agg of
      (FReturn vars) ->
        Set.fromList vars
      Not ->
        bottom
      Exists ->
        bottom
      (Summarize funcs groupby) ->
        freeVars funcs \/ Set.fromList groupby
      (Limit _) ->
        bottom
      Distinct ->
        bottom
      (OrderByAsc var) ->
        singleton var
      (OrderByDesc var) ->
        singleton var

instance FreeVars a => FreeVars [a] where
    freeVars = Set.unions . map freeVars

instance FreeVars a => FreeVars (Set a) where
    freeVars sa = Set.unions (map freeVars (Set.toAscList sa))

instance (FreeVars a, FreeVars b) => FreeVars (a, b) where
    freeVars (a,b) = freeVars a \/ freeVars b

instance FreeVars Var where
    freeVars = singleton

instance FreeVars Summary where
  freeVars (Max a) = singleton a
  freeVars (Min a) = singleton a
  freeVars (Sum a) = singleton a
  freeVars (Average a) = singleton a
  freeVars (CountDistinct a) = singleton a
  freeVars Count = bottom

class Unify a where
    unify :: a -> a -> Maybe Substitution

instance Unify Expr where
    unify e1 e2 = case e1 of
      VarExpr var0 -> Just (Map.singleton var0 e2)
      _ -> case e2 of
                VarExpr var0 -> Just (Map.singleton var0 e2)
                _ -> Nothing

instance (Unify a, Subst a) => Unify [a] where
    unify as1 as2 | length as1 == length as2 =
        foldM (\s (a1, a2) -> do
            s' <- unify (subst s a1) (subst s a2)
            return (subst s' s)) empty (zip as1 as2)
    unify _ _ = Nothing

instance Unify Atom where
    unify (Atom p1 args1) (Atom p2 args2) | p1 == p2 = unify args1 args2
    unify _ _ = Nothing

{-
pushNegations :: Sign -> Formula -> Formula
pushNegations Pos (Conjunction formulas) = conj (map (pushNegations Pos) formulas)
pushNegations Pos (Disjunction formulas) = disj (map (pushNegations Pos) formulas)
pushNegations Pos (Not formula) = pushNegations Neg formula
pushNegations Pos (Exists var formula) = Exists var (pushNegations Pos formula)
pushNegations Pos (Forall var formula) = Forall var (pushNegations Pos formula)
pushNegations Pos (FClassical formula) = FClassical (pushNegations Pos formula)
pushNegations Pos (FSeq formula formula) = FClassical (pushNegations Pos formula)
pushNegations Pos (FInsert formula) = FClassical (pushNegations Pos formula)
pushNegations Pos a@(Atomic _ ) = a
pushNegations Neg (Conjunction formulas) = disj (map (pushNegations Neg) formulas)
pushNegations Neg (Disjunction formulas) = conj (map (pushNegations Neg) formulas)
pushNegations Neg (Not formula) = pushNegations Pos formula
pushNegations Neg (Exists var formula) = Forall var (pushNegations Neg formula)
pushNegations Neg (Forall var formula) = Exists var (pushNegations Neg formula)
pushNegations Neg (FClassical formula) = Not (FClassical (pushNegations Pos formula))
pushNegations Neg a@(Atomic _) = Not a

convertForall' :: Sign -> PureFormula -> PureFormula
convertForall' Pos (Conjunction form1 form2) = Conjunction (convertForall' Pos form1) (convertForall' Pos form2)
convertForall' Pos (Disjunction form1 form2) = Disjunction (convertForall' Pos form1) (convertForall' Pos form2)
convertForall' Pos (Not form) = convertForall' Neg form
convertForall' Pos (Exists var form) = Exists var (convertForall' Pos form)
convertForall' Pos (Forall var form) = Not (Exists var (convertForall' Neg form))
convertForall' Pos a@(Atomic _ ) = a
convertForall' Neg (Conjunction form1 form2) = Disjunction (convertForall' Neg form1) (convertForall' Neg form2)
convertForall' Neg (Disjunction form1 form2) = Conjunction (convertForall' Neg form1) (convertForall' Neg form2)
convertForall' Neg (Not form) = convertForall' Pos form
convertForall' Neg (Exists var form) = Not (Exists var (convertForall' Pos form))
convertForall' Neg (Forall var form) = Exists var (convertForall' Neg form)
convertForall' Neg a@(Atomic _) = Not a
convertForall' _ form = form

convertForall :: Formula -> Formula
convertForall (FTransaction form) = FTransaction (convertForall form)
convertForall (FClassical form) = FClassical (convertForall' Pos form)
convertForall (FSequencing form1 form2) = FSequencing (convertForall form1) (convertForall form2)
convertForall (FChoice form1 form2) = FChoice (convertForall form1) (convertForall form2)
convertForall form@(FInsert _) = form
convertForall form@(FAtomic _) = form
convertForall form = form
-}
splitPosNegLits :: [Lit1 a] -> ([Atom1 a], [Atom1 a])
splitPosNegLits = foldr (\ (Lit thesign theatom) (pos, neg) -> case thesign of
    Pos -> (theatom : pos, neg)
    Neg -> (pos, theatom : neg)) ([],[])

keyComponents :: PredType -> [a] -> [a]
keyComponents (PredType _ paramtypes) = map snd . filter (\(ParamType type1 _ _ _, _) -> type1) . zip paramtypes

propComponents :: PredType -> [a] -> [a]
propComponents (PredType _ paramtypes) = map snd . filter (\(ParamType type1 _ _ _, _) -> not type1) . zip paramtypes

outputComponents :: PredType -> [a] -> [a]
outputComponents (PredType _ paramtypes) = map snd . filter (\(ParamType  _ _ type1 _, _) -> type1) . zip paramtypes

outputOnlyComponents :: PredType -> [a] -> [a]
outputOnlyComponents (PredType _ paramtypes) = map snd . filter (\(ParamType  _ type1 _ _, _) -> not type1) . zip paramtypes

isObjectPred2 :: Pred -> Bool
isObjectPred2 (Pred _ (PredType predKind _)) = case predKind of
            ObjectPred -> True
            PropertyPred -> False

isObjectPred :: PredTypeMap -> PredName -> Bool
isObjectPred ptm pn  = case lookup pn ptm of
  Nothing -> error ("isObjectPred: cannot find predicate " ++ show pn)
  Just (PredType predKind _) -> case predKind of
            ObjectPred -> True
            PropertyPred -> False

constructPredMap :: [Pred] -> PredMap
constructPredMap = foldl (\map1 pred1@(Pred name _) -> insertObject name pred1 map1) mempty

type PredTypeMap = Map PredName PredType

sortByKey :: Ord (Expr1 a) => PredTypeMap -> [Lit1 a] -> Map [Expr1 a] [Lit1 a]
sortByKey ptm = foldl insertByKey empty where
    insertByKey map1 lit@(Lit _ (Atom pred1 args)) =
        let keyargs = keyComponents (fromMaybe (error ("sortByKey: cannot find predicate " ++ show pred1)) (lookup pred1 ptm)) args in
            alter (\l -> case l of
                Nothing -> Just [lit]
                Just lits -> Just (lits ++ [lit])) keyargs map1

isObjectPredAtom :: PredTypeMap -> Atom1 a -> Bool
isObjectPredAtom ptm (Atom pn _) =
  case fromMaybe (error ("isObjectPredAtom: cannot find predicate " ++ show pn)) (lookup pn ptm) of
    PredType ObjectPred _ -> True
    _ -> False

isObjectPredLit :: PredTypeMap -> Lit1 a -> Bool
isObjectPredLit ptm (Lit _ a) = isObjectPredAtom ptm a

sortAtomByPred :: [Atom1 a] -> Map PredName [Atom1 a]
sortAtomByPred = foldl insertAtomByPred empty where
    insertAtomByPred map1 a@(Atom pred1 _) =
        alter (\asmaybe -> case asmaybe of
            Nothing -> Just [a]
            Just as -> Just (as ++ [a])) pred1 map1

pattern PredName ks n = ObjectPath (NamespacePath ks) n
pattern QPredName k ks n = ObjectPath (NamespacePath (k : ks)) n
pattern UQPredName n = ObjectPath (NamespacePath []) n

instance Serialize PredName where
  serialize = predNameToString

instance Serialize ParamType where
  serialize (ParamType key input output t) = (if key then "key " else ""  ) ++ (if input then "input " else "") ++ (if output then "output " else "") ++ serialize t

predNameToString :: PredName -> String
predNameToString (PredName mns n)= intercalate "." (mns ++ [n])

predNameToString2 :: PredName -> String
predNameToString2 (PredName _ n) = n

predNameMatches :: PredName -> PredName -> Bool
predNameMatches (UQPredName n1) (UQPredName n2) = n1 == n2
predNameMatches (QPredName _ _ n1) (UQPredName n2) = n1 == n2
predNameMatches (QPredName k1 ks1 n1) (QPredName k2 ks2 n2) = k1 == k2 && ks1 == ks2 && n1 == n2
predNameMatches _ _ = False

constructPredTypeMap :: [Pred] -> PredTypeMap
constructPredTypeMap =
  fromList . map (\(Pred pn pt) -> (pn, pt))

instance Key String where

setNamespace :: String -> PredName -> PredName
setNamespace ns (UQPredName n) = QPredName ns [] n
setNamespace ns n = error ("cannot set namespace to a qualified predicate name " ++ ns ++ " " ++ show n)

setPredNamespace :: String -> Pred -> Pred
setPredNamespace ns (Pred name paramtypes) = Pred (setNamespace ns name) paramtypes

{- setFormulaNamespace :: String -> Formula -> Formula
setFormulaNamespace ns (FAtomic (Atom (Pred n pts) args)) = FAtomic (Atom (Pred (setNamespace ns n) pts) args)
setFormulaNamespace ns (FInsert (Lit sign1 (Atom (Pred n pts) arg))) = (FInsert (Lit sign1 (Atom (Pred (setNamespace ns n) pts) arg)))
setFormulaNamespace ns (FTransaction ) = FTransaction
setFormulaNamespace ns (FClassical form) = FClassical (setPureFormulaNamespace ns form)
setFormulaNamespace ns (FChoice form1 form2) = FChoice (setFormulaNamespace ns form1) (setFormulaNamespace ns form2)
setFormulaNamespace ns (FSequencing form1 form2) = FSequencing (setFormulaNamespace ns form1) (setFormulaNamespace ns form2)
setFormulaNamespace ns FOne = FOne
setFormulaNamespace ns FZero = FZero

setPureFormulaNamespace :: String -> PureFormula -> PureFormula
setPureFormulaNamespace ns (Atomic (Atom (Pred n pts) args)) = Atomic (Atom (Pred (setNamespace ns n) pts) args)
setPureFormulaNamespace ns (Conjunction form1 form2) = Conjunction (setPureFormulaNamespace ns form1) (setPureFormulaNamespace ns form2)
setPureFormulaNamespace ns (Disjunction form1 form2) = Disjunction (setPureFormulaNamespace ns form1) (setPureFormulaNamespace ns form2)
setPureFormulaNamespace ns (Not form) = Not (setPureFormulaNamespace ns form)
setPureFormulaNamespace ns (Exists var form) = Exists var (setPureFormulaNamespace ns form)
setPureFormulaNamespace ns CTrue = CTrue
setPureFormulaNamespace ns CFalse = CFalse -}

-- instance Show Pred where
--     show (Pred name t) = name ++ show t
--
-- instance Show PredType where
--     show (PredType ObjectPred types) = "OP" ++ "(" ++ intercalate "," (map show types) ++ ")"
--     show (PredType PropertyPred types) = "PP" ++ "(" ++ intercalate "," (map show types) ++ ")"
--
-- instance Show ParamType where
--     show (Key type1) = "KEY " ++ type1
--     show (Property type1) = "PROP " ++ type1

class Serialize a where
    serialize :: a -> String

class SerializeAnnotation (f :: (* -> *) -> *) where
    serializeAnnotation :: f g -> String

instance SerializeAnnotation Tie where
    serializeAnnotation _ = ""

instance Serialize a => SerializeAnnotation (Annotated a) where
    serializeAnnotation (Annotated a _) = "[" ++ serialize a ++ "]"

instance (SerializeAnnotation a, Unannotate a Expr0) => Serialize (Atom1 a) where
    serialize (Atom name args) = predNameToString name ++ "(" ++ intercalate "," (map serialize args) ++ ")"

instance (SerializeAnnotation a, Unannotate a Expr0) => Serialize (Expr1 a) where
    serialize e1 = serializeAnnotation e1 ++ case unannotate e1 of
      VarExpr0 var0 -> serialize var0
      IntExpr0 i -> show i
      StringExpr0 s -> show s
      PatternExpr0 p -> show p
      NullExpr0 -> "null"
      CastExpr0 t v -> serialize v ++ " " ++ serialize t

instance Serialize CastType where
    serialize TextType = "Text"
    serialize Int64Type = "Integer"
    serialize ByteStringType = "ByteString"
    serialize (RefType ty) = "ref " ++ ty
    serialize (TypeVar v) = v

instance Serialize Var where
    serialize (Var s) = s

instance (SerializeAnnotation a, Unannotate a Expr0) => Serialize (Lit1 a) where
    serialize (Lit thesign theatom) = case thesign of
        Pos -> serialize theatom
        Neg -> "¬" ++ serialize theatom

instance Serialize Summary where
    serialize (Max v) = "max " ++ serialize v
    serialize (Min v) = "min " ++ serialize v
    serialize Count = "count"
    serialize (Sum v) = "sum " ++ serialize v
    serialize (Average v) = "average " ++ serialize v
    serialize (CountDistinct v) = "count distinct(" ++ serialize v ++ ")"

instance (SerializeAnnotation a, SerializeAnnotation b, Unannotate a Expr0, Unannotate b (Formula0 a)) => Serialize (Formula1 a b) where
    serialize f1 = "[" ++ serializeAnnotation f1 ++ "]" ++ case unannotate f1 of
      FAtomic0 a -> serialize a
      FInsert0 lits -> "(insert " ++ serialize lits ++ ")"
      FSequencing0 _ _ -> "(" ++ intercalate " ⊗ " (map serialize (getFsequencings' f1)) ++ ")"
      FChoice0 _ _ -> "(" ++ intercalate " ⊕ " (map serialize (getFchoices' f1)) ++ ")"
      FPar0 _ _ -> "(" ++ intercalate " ‖ " (map serialize (getFpars' f1)) ++ ")"
      FOne0 -> "𝟏"
      FZero0 -> "𝟎"
      Aggregate0 (FReturn vars) form -> "(" ++ serialize form ++ " return " ++ unwords (map serialize vars) ++ ")"
      Aggregate0 Exists form -> "∃" ++ serialize form
      Aggregate0 Not form -> "¬" ++ serialize form
      Aggregate0 Distinct form -> "(distinct " ++ serialize form ++ ")"
      Aggregate0 (Summarize funcs groupby) form -> "(let " ++ unwords (map (\(var1, func1) -> serialize var1 ++ " = " ++ serialize func1) funcs) ++ " " ++ (if null groupby then "" else "group by " ++ unwords (map serialize groupby) ++ " ") ++ serialize form ++ ")"
      Aggregate0 (Limit n) form -> "(limit " ++ show n ++ " " ++ serialize form ++ ")"
      Aggregate0 (OrderByAsc var1) form -> "(order by " ++ serialize var1 ++ " asc " ++ serialize form ++ ")"
      Aggregate0 (OrderByDesc var1) form -> "(order by " ++ serialize var1 ++ " desc " ++ serialize form ++ ")"

instance (Serialize a, Serialize b) => Serialize (Map a b) where
  serialize m = foldrWithKey (\ a b s -> serialize a ++ "=" ++ serialize b ++ if null s then s else "," ++ s ) "" m
-- rule

type Rule = Set Lit

cartProd :: Ord a => Set (Set a) -> Set (Set a) -> Set (Set a)
cartProd as as2 = Set.fromList [a `Set.union` a2 | a <- Set.toList as, a2 <- Set.toList as2 ]

cartProds :: Ord a => [Set (Set a)] -> Set (Set a)
cartProds = foldl cartProd (Set.singleton bottom)

type NewEnv = State (Map String Int, [String])
-- assume that not has been pushed
class New a b where
    new :: b -> NewEnv a

newtype StringWrapper = StringWrapper String

instance New String StringWrapper where
    new (StringWrapper var0) = do
        (vidmap, vars) <- get
        let findVN vid =
                let vn = var0 ++ if vid == negate 1 then "" else show vid in
                    if vn `elem` vars
                        then findVN (vid + 1)
                        else (vn, vid)
        let (vn, vid) = findVN (fromMaybe (negate 1) (lookup var0 vidmap))
        put (insert var0 (vid + 1) vidmap, vars)
        return vn

instance New Var Var where
    new (Var var0) = Var <$> new (StringWrapper var0)
instance New Expr Expr where
    new expr = VarExpr <$> new expr

instance New Var Expr where
    new (VarExpr v) = new v
    new _ = new (Var "var")

instance New a b => New [a] [b] where
    new = mapM new

runNew :: NewEnv a -> a
runNew a = evalState a (empty, [])

register :: [String] -> NewEnv ()
register vars0 = do
    (vmap, vars) <- get
    put (vmap, vars `union` vars0)

registerVars :: [Var] -> NewEnv ()
registerVars vars0 = register (map unVar vars0)

{-
toCNF :: Formula -> Set Rule
toCNF formula = runNew (toCNF' formula) where
    toCNF' :: Formula -> NewEnv (Set Rule)
    toCNF' (Conjunction formulas) =
        Set.unions <$> mapM toCNF' formulas
    toCNF' (Disjunction formulas) =
        cartProds <$> mapM toCNF' formulas
    toCNF' (Exists _ _) = error "not supported" -- we ignore existential quantification to avoid going to full FO
    toCNF' (Forall var a) = do
        newvar <- new var
        toCNF' (subst (singleton var newvar) a)
    toCNF' (Atomic a) = return (Set.singleton (Set.singleton (Lit Pos a)))
    toCNF' (Not (Atomic a)) = return (Set.singleton (Set.singleton (Lit Neg a)))
    toCNF' _ = error "not has not been pushed"
    -}

type Substitution = Map Var Expr

class Subst a where
    subst :: Substitution -> a -> a

instance Subst Expr where
    subst s e1 = case e1 of
      VarExpr v -> fromMaybe e1 (lookup v s)
      CastExpr t e -> CastExpr t (subst s e)
      e -> e

instance Subst Substitution where
    subst s = Map.map (subst s)

instance Subst Lit where
    subst s (Lit sign' a) = Lit sign' (subst s a)

instance Subst Atom where
    subst s (Atom pred' args) = Atom pred' (subst s args)

extractVar :: Unannotate a Expr0 => Expr1 a -> Var
extractVar e1 = case unannotate e1 of
                    VarExpr0 var0 -> var0
                    _ -> error "this is not a var expr"

instance Subst Var where
    subst s v = extractVar (subst s (VarExpr v))

instance Subst Summary where
    subst s (Max v) = Max (subst s v)
    subst s (Min v) = Min (subst s v)
    subst _ Count = Count
    subst s (Average v) = Average (subst s v)
    subst s (Sum v) = Sum (subst s v)
    subst s (CountDistinct v) = CountDistinct (subst s v)

instance (Unannotate b (Formula0 a), Subst (Atom1 a)) => Subst (Formula1 a b) where
    subst s = mapA (\f -> case f of
      FAtomic0 a -> FAtomic0 (subst s a)
      FInsert0 lits -> FInsert0 lits
      FSequencing0 form1 form2 -> FSequencing0 form1 form2
      FChoice0 form1 form2 -> FChoice0 form1 form2
      FPar0 form1 form2 -> FPar0 form1 form2
      Aggregate0 (FReturn vars) form -> Aggregate0 (FReturn (subst s vars)) form
      Aggregate0 Not a -> Aggregate0 Not a
      Aggregate0 Exists a -> Aggregate0 Exists a
      Aggregate0 (Summarize funcs groupby) a -> Aggregate0 (Summarize (subst s funcs) (map (subst s) groupby)) a
      Aggregate0 (Limit n) a -> Aggregate0 (Limit n) a
      Aggregate0 Distinct a -> Aggregate0 Distinct a
      Aggregate0 (OrderByAsc var1) a -> Aggregate0 (OrderByAsc (subst s var1)) a
      Aggregate0 (OrderByDesc var1) a -> Aggregate0 (OrderByAsc (subst s var1)) a
      FZero0 -> FZero0
      FOne0 -> FOne0)

instance Subst a => Subst [a] where
    subst s = map (subst s)

instance (Subst a, Subst b) => Subst (a, b) where
    subst s (a, b) = (subst s a, subst s b)

instance (Subst a, Ord a) => Subst (Set a) where
    subst s = Set.map (subst s)

comp :: Substitution -> Substitution -> Substitution
comp s1 s2 = subst s2 s1 `Map.union` s2

-- instance Monoid Substitution where
--    mempty = empty
--    s1 `mappend` s2 = comp
{-
class GatherConstants a where
    gatherConstants :: a -> Set Expr

instance GatherConstants Expr where
    gatherConstants e@(IntExpr _) = Set.singleton e
    gatherConstants e@(StringExpr _) = Set.singleton e
    gatherConstants _ = bottom

instance GatherConstants Atom where
    gatherConstants (Atom _ args) = Set.unions (map gatherConstants args)

instance GatherConstants Lit where
    gatherConstants (Lit _ atom) = gatherConstants atom

instance GatherConstants a => GatherConstants (Set a) where
    gatherConstants sa = gatherConstants (Set.toList sa)

instance GatherConstants a => GatherConstants [a] where
    gatherConstants la = Set.unions (map gatherConstants la)

instance GatherConstants Formula where
    gatherConstants (Atomic a) = gatherConstants a
    gatherConstants (Conjunction as) = gatherConstants as
    gatherConstants (Disjunction as) = gatherConstants as
    gatherConstants (Not a) = gatherConstants a
    gatherConstants (Exists _ a) = gatherConstants a
    gatherConstants (Forall _ a) = gatherConstants a
    gatherConstants (FClassical a) = gatherConstants a
    gatherConstants (FSeq as) = gatherConstants as
    gatherConstants (FInsert lits) = Set.unions (map gatherConstants lits)
-}
pureF :: Unannotate f (Formula0 a) => Formula1 a f -> Bool
pureF f1 = case unannotate f1 of
  FAtomic0 _ -> True
  FSequencing0 form1 form2 -> pureF form1 &&  pureF form2
  FChoice0 form1 form2 -> pureF form1 && pureF form2
  FPar0 form1 form2 -> pureF form1 && pureF form2
  FInsert0 _ -> False
  Aggregate0 _ form -> pureF form
  FOne0 -> True
  FZero0 -> True

layeredF :: Unannotate f (Formula0 a) => Formula1 a f -> Bool
layeredF f1 = case unannotate f1 of
  FAtomic0 _ -> True
  FSequencing0 form1 form2 -> layeredF form1 &&  layeredF form2
  FChoice0 form1 form2 -> layeredF form1 && layeredF form2
  FPar0 form1 form2 -> layeredF form1 && layeredF form2
  FInsert0 _ -> True
  Aggregate0 _ form -> pureF form
  FOne0 -> True
  FZero0 -> True

infixl 5 @@@
infixl 5 @@
infixl 5 @@+
infixl 5 @@-
infixl 4 .*.
infixl 3 .+.
infixl 3 .|.

(@@@) :: PredName -> [Expr] -> Formula
pred' @@@ args =  (FAtomic (Atom pred' args))

(@@) :: String -> [Expr] -> Formula
label @@ args =  (FAtomic (Atom (UQPredName label) args))

(@@+) :: String -> [Expr] -> Formula
label @@+ args =  (FInsert (Lit Pos (Atom (UQPredName label) args)))

(@@-) :: String -> [Expr] -> Formula
label @@- args =  (FInsert (Lit Neg (Atom (UQPredName label) args)))

(.*.) :: Formula -> Formula -> Formula
FOne .*. b = b
FZero .*. _ =  FZero
a .*.  FOne = a
a .*. b =  (FSequencing a b)

(.+.) :: Formula -> Formula -> Formula
FZero .+. b = b
a .+.  FZero = a
a .+. b =  (FChoice a b)

(.|.) :: Formula -> Formula -> Formula
FZero .|. b = b
a .|.  FZero = a
a .|. b =  (FPar a b)

-- get the top level conjucts
getFsequencings :: Unannotate f (Formula0 a) => Formula1 a f -> [Formula1 a f]
getFsequencings f1 = case unannotate f1 of
  (FSequencing0 form1 form2) ->  getFsequencings form1 ++ getFsequencings form2
  _ -> [f1]

getFchoices :: Unannotate f (Formula0 a) => Formula1 a f -> [Formula1 a f]
getFchoices f1 = case unannotate f1 of
  (FChoice0 form1 form2) -> getFchoices form1 ++ getFchoices form2
  _ -> [f1]

getFpars :: Unannotate f (Formula0 a) => Formula1 a f -> [Formula1 a f]
getFpars f1 = case unannotate f1 of
  (FPar0 form1 form2) -> getFpars form1 ++ getFpars form2
  _ -> [f1]

getFsequencings' :: Unannotate f (Formula0 a) => Formula1 a f -> [Formula1 a f]
getFsequencings' f1 = case unannotate f1 of
  (FSequencing0 form1 form2) -> getFsequencings' form1 ++ [form2]
  _ -> [f1]

getFchoices' :: Unannotate f (Formula0 a) => Formula1 a f -> [Formula1 a f]
getFchoices' f1 = case unannotate f1 of
  (FChoice0 form1 form2) -> getFchoices' form1 ++  [form2]
  _ -> [f1]

getFpars' :: Unannotate f (Formula0 a) => Formula1 a f -> [Formula1 a f]
getFpars' f1 = case unannotate f1 of
  (FPar0 form1 form2) -> getFpars' form1 ++  [form2]
  _ -> [f1]

fsequencing :: [Formula] -> Formula
fsequencing = foldl (.*.) ( FOne)

fsequencing1 :: Monoid b => [Formula1 a (Annotated b)] -> Formula1 a (Annotated b)
fsequencing1 = foldl1 (\ f1@(Annotated b1 _)  f2@(Annotated b2 _) -> Annotated (b1 <> b2) (FSequencing0 f1 f2))

fchoice :: [Formula] -> Formula
fchoice = foldl (.+.) ( FZero)

fchoice1 :: Monoid b => [Formula1 a (Annotated b)] -> Formula1 a (Annotated b)
fchoice1 = foldl1 (\ f1@(Annotated b1 _)  f2@(Annotated b2 _) -> Annotated (b1 <> b2) (FChoice0 f1 f2))

fpar :: [Formula] -> Formula
fpar = foldl (.|.) ( FZero)

fpar1 :: Monoid b => [Formula1 a (Annotated b)] -> Formula1 a (Annotated b)
fpar1 = foldl1 (\ f1@(Annotated b1 _)  f2@(Annotated b2 _) -> Annotated (b1 <> b2) (FPar0 f1 f2))

notE :: Formula -> Formula
notE =
    Aggregate Not

existsE :: Formula -> Formula
existsE =
    Aggregate Exists

var :: String -> Expr
var = VarExpr . Var

{- class SubstPred a where
    substPred :: Map Pred Pred -> a -> a

instance SubstPred Formula where
    substPred _ form@(FReturn _) = form
    substPred pmap (FAtomic a) = FAtomic (substPred pmap a)
    substPred pmap (FTransaction) = FTransaction
    substPred pmap (FSequencing form1 form2) = FSequencing (substPred pmap form1) (substPred pmap form2)
    substPred pmap (FChoice form1 form2) = FChoice (substPred pmap form1) (substPred pmap form2)
    substPred pmap (FPar form1 form2) = FPar (substPred pmap form1) (substPred pmap form2)
    substPred pmap (Not a) = Not (substPred pmap a)
    substPred pmap (Exists v a) = Exists v (substPred pmap a)
    substPred pmap (FInsert lits) = FInsert (substPred pmap lits)
    substPred _ form = form

instance SubstPred Atom where
  substPred pmap (Atom pred0 args) = Atom (case lookup pred0 pmap of
        Just p2 -> p2
        _ -> pred0) args

instance SubstPred Lit where
    substPred pmap (Lit sign0 atom0) = Lit sign0 (substPred pmap atom0)

instance SubstPred a => SubstPred [a] where
    substPred pmap = map (substPred pmap) -}


-- predicate map
type PredMap = Namespace String Pred

type Location = [String]
