{-# LANGUAGE TypeFamilies, MultiParamTypeClasses, ExistentialQuantification, FlexibleInstances, StandaloneDeriving,
   RankNTypes, FlexibleContexts, GADTs, PatternSynonyms #-}

module QueryArrow.FO.Data where

import Prelude hiding (lookup)
import Data.Map.Strict (Map, empty, insert, alter, lookup, fromList)
import Data.List (intercalate, union, unwords)
import Control.Monad.Trans.State.Strict (evalState,get, put, State)
import Data.Set (Set)
import qualified Data.Set as Set
import Data.Maybe
import qualified Data.Map as Map
import qualified Data.Text as T
import Data.Namespace.Path
import Data.Namespace.Namespace
import Algebra.Lattice
import Control.Monad (foldM)
import Data.ByteString (ByteString)

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
data CastType = TextType | NumberType | RefType String | ByteStringType | TypeVar String deriving (Eq, Ord, Show, Read)

-- expression
data Expr = VarExpr Var | IntExpr Int | StringExpr T.Text | PatternExpr T.Text | NullExpr | CastExpr CastType Expr deriving (Eq, Ord, Show, Read)

-- atoms
data Atom = Atom { atomPred :: PredName, atomArgs :: [Expr] } deriving (Eq, Ord, Show, Read)

-- sign of literal
data Sign = Pos | Neg deriving (Eq, Ord, Show, Read)

-- literals
data Lit = Lit { sign :: Sign,  atom :: Atom } deriving (Eq, Ord, Show, Read)

data Summary = Max Var | Min Var | Sum Var | Average Var | CountDistinct Var | Count deriving (Eq, Ord, Show, Read)
data Aggregator = Summarize [(Var, Summary)] [Var] | Limit Int | OrderByAsc Var | OrderByDesc Var | Distinct | Not | Exists | FReturn [Var] deriving (Eq, Ord, Show, Read)

data Formula = FAtomic Atom
             | FInsert Lit
             | FChoice Formula Formula
             | FSequencing Formula Formula
             | FPar Formula Formula
             | FOne
             | FZero
             | Aggregate Aggregator Formula deriving (Eq, Ord, Show, Read)

{-
isConst :: Expr -> Bool
isConst (IntExpr _) = True
isConst (StringExpr _) = True
isConst _ = False
-}
-- free variables


class FreeVars a where
    freeVars :: a -> Set Var

instance FreeVars Expr where
    freeVars (CastExpr _ e) = freeVars e
    freeVars (VarExpr var0) = Set.singleton var0
    freeVars (IntExpr _) = bottom
    freeVars (StringExpr _) = bottom
    freeVars (PatternExpr _) = bottom
    freeVars NullExpr = bottom

instance FreeVars Atom where
    freeVars (Atom _ theargs) = freeVars theargs

instance FreeVars Lit where
    freeVars (Lit _ theatom) = freeVars theatom

instance FreeVars Formula where
    freeVars (FAtomic atom1) =
        freeVars atom1
    freeVars (FInsert lits) =
        freeVars lits
    freeVars (FSequencing form1 form2) =
        freeVars form1 \/ freeVars form2
    freeVars (FChoice form1 form2) =
        freeVars form1 \/ freeVars form2
    freeVars (FPar form1 form2) =
        freeVars form1 \/ freeVars form2
    freeVars (Aggregate (FReturn _) formula1) =
        freeVars formula1
    freeVars (Aggregate Not formula1) =
        freeVars formula1
    freeVars (Aggregate Exists formula1) =
        freeVars formula1
    freeVars (Aggregate (Summarize _ _) form) =
        freeVars form
    freeVars (Aggregate (Limit _) form) =
        freeVars form
    freeVars (Aggregate Distinct form) =
        freeVars form
    freeVars (Aggregate (OrderByAsc _) form) =
        freeVars form
    freeVars (Aggregate (OrderByDesc _) form) =
        freeVars form
    freeVars FZero = bottom
    freeVars FOne = bottom

instance FreeVars a => FreeVars [a] where
    freeVars = Set.unions . map freeVars

class Unify a where
    unify :: a -> a -> Maybe Substitution

instance Unify Expr where
    unify (VarExpr var0) e = Just (Map.singleton var0 e)
    unify e (VarExpr var0) = Just (Map.singleton var0 e)
    unify _ _ = Nothing

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
splitPosNegLits :: [Lit] -> ([Atom], [Atom])
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

sortByKey :: PredTypeMap -> [Lit] -> Map [Expr] [Lit]
sortByKey ptm = foldl insertByKey empty where
    insertByKey map1 lit@(Lit _ (Atom pred1 args)) =
        let keyargs = keyComponents (fromMaybe (error ("sortByKey: cannot find predicate " ++ show pred1)) (lookup pred1 ptm)) args in
            alter (\l -> case l of
                Nothing -> Just [lit]
                Just lits -> Just (lits ++ [lit])) keyargs map1

isObjectPredAtom :: PredTypeMap -> Atom -> Bool
isObjectPredAtom ptm (Atom pn _) =
  case fromMaybe (error ("isObjectPredAtom: cannot find predicate " ++ show pn)) (lookup pn ptm) of
    PredType ObjectPred _ -> True
    _ -> False

isObjectPredLit :: PredTypeMap -> Lit -> Bool
isObjectPredLit ptm (Lit _ a) = isObjectPredAtom ptm a

sortAtomByPred :: [Atom] -> Map PredName [Atom]
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

instance Serialize Atom where
    serialize (Atom name args) = predNameToString name ++ "(" ++ intercalate "," (map serialize args) ++ ")"

instance Serialize Expr where
    serialize (VarExpr var0) = serialize var0
    serialize (IntExpr i) = show i
    serialize (StringExpr s) = show s
    serialize (PatternExpr p) = show p
    serialize NullExpr = "null"
    serialize (CastExpr t v) = serialize v ++ " " ++ serialize t

instance Serialize CastType where
    serialize TextType = "Text"
    serialize NumberType = "Integer"
    serialize ByteStringType = "ByteString"
    serialize (RefType ty) = "ref " ++ ty
    serialize (TypeVar v) = v

instance Serialize Var where
    serialize (Var s) = s

instance Serialize Lit where
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

instance Serialize Formula where
    serialize (FAtomic a) = serialize a
    serialize (FInsert lits) = "(insert " ++ serialize lits ++ ")"
    serialize form@(FSequencing _ _) = "(" ++ intercalate " ⊗ " (map serialize (getFsequencings' form)) ++ ")"
    serialize form@(FChoice _ _) = "(" ++ intercalate " ⊕ " (map serialize (getFchoices' form)) ++ ")"
    serialize form@(FPar _ _) = "(" ++ intercalate " ‖ " (map serialize (getFpars' form)) ++ ")"
    serialize FOne = "𝟏"
    serialize FZero = "𝟎"
    serialize (Aggregate (FReturn vars) form) = "(" ++ serialize form ++ " return " ++ unwords (map serialize vars) ++ ")"
    serialize (Aggregate Exists form) = "∃" ++ serialize form
    serialize (Aggregate Not form) = "¬" ++ serialize form
    serialize (Aggregate Distinct form) = "(distinct " ++ serialize form ++ ")"
    serialize (Aggregate (Summarize funcs groupby) form) = "(let " ++ unwords (map (\(var1, func1) -> serialize var1 ++ " = " ++ serialize func1) funcs) ++ " " ++ (if null groupby then "" else "group by " ++ unwords (map serialize groupby) ++ " ") ++ serialize form ++ ")"
    serialize (Aggregate (Limit n) form) = "(limit " ++ show n ++ " " ++ serialize form ++ ")"
    serialize (Aggregate (OrderByAsc var1) form) = "(order by " ++ serialize var1 ++ " asc " ++ serialize form ++ ")"
    serialize (Aggregate (OrderByDesc var1) form) = "(order by " ++ serialize var1 ++ " desc " ++ serialize form ++ ")"

-- rule

type Rule = Set Lit

cartProd :: Ord a => Set (Set a) -> Set (Set a) -> Set (Set a)
cartProd as as2 = Set.fromList [a `Set.union` a2 | a <- Set.toList as, a2 <- Set.toList as2 ]

cartProds :: Ord a => [Set (Set a)] -> Set (Set a)
cartProds = foldl cartProd (Set.singleton Set.empty)

type NewEnv = State (Map String Int, [String])
-- assume that not has been pushed
class New a b where
    new :: b -> NewEnv a

newtype StringWrapper = StringWrapper String
instance New Expr Var where
    new var0 = VarExpr <$> new (VarExpr var0)

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
    subst s ve@(VarExpr v) = fromMaybe ve (lookup v s)
    subst s (CastExpr t e) = CastExpr t (subst s e)
    subst _ e = e

instance Subst Substitution where
    subst s = Map.map (subst s)

instance Subst Lit where
    subst s (Lit sign' a) = Lit sign' (subst s a)

instance Subst Atom where
    subst s (Atom pred' args) = Atom pred' (subst s args)

extractVar :: Expr -> Var
extractVar (VarExpr var0) = var0
extractVar _ = error "this is not a var expr"

instance Subst Var where
    subst s v = extractVar (subst s (VarExpr v))

instance Subst Summary where
    subst s (Max v) = Max (subst s v)
    subst s (Min v) = Min (subst s v)
    subst _ Count = Count
    subst s (Average v) = Average (subst s v)
    subst s (Sum v) = Sum (subst s v)
    subst s (CountDistinct v) = CountDistinct (subst s v)

instance Subst Formula where
    subst s (FAtomic a) = FAtomic (subst s a)
    subst s (FInsert lits) = FInsert (subst s lits)
    subst s (FSequencing form1 form2) = FSequencing (subst s form1) (subst s form2)
    subst s (FChoice form1 form2) = FChoice (subst s form1) (subst s form2)
    subst s (FPar form1 form2) = FPar (subst s form1) (subst s form2)
    subst s (Aggregate (FReturn vars) form) = Aggregate (FReturn (subst s vars)) (subst s form)
    subst s (Aggregate Not a) = Aggregate Not (subst s a)
    subst s (Aggregate Exists a) = Aggregate Exists (subst s a)
    subst s (Aggregate (Summarize funcs groupby) a) = Aggregate (Summarize (subst s funcs) (map (\var1 -> extractVar (subst s (VarExpr var1))) groupby)) (subst s a)
    subst s (Aggregate (Limit n) a) = Aggregate (Limit n) (subst s a)
    subst s (Aggregate Distinct a) = Aggregate Distinct (subst s a)
    subst s (Aggregate (OrderByAsc var1) a) = Aggregate (OrderByAsc (extractVar (subst s (VarExpr var1)))) (subst s a)
    subst s (Aggregate (OrderByDesc var1) a) = Aggregate (OrderByAsc (extractVar (subst s (VarExpr var1)))) (subst s a)
    subst _ FZero = FZero
    subst _ FOne = FOne

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
    gatherConstants _ = Set.empty

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
instance FreeVars a => FreeVars (Set a) where
    freeVars sa = Set.unions (map freeVars (Set.toAscList sa))

pureF :: Formula -> Bool
pureF (FAtomic _) = True
pureF (FSequencing form1 form2) =  pureF form1 &&  pureF form2
pureF (FChoice form1 form2) = pureF form1 && pureF form2
pureF (FPar form1 form2) = pureF form1 && pureF form2
pureF (FInsert _) = False
pureF (Aggregate _ form) =  pureF form
pureF FOne = True
pureF FZero = True

layeredF :: Formula -> Bool
layeredF (FAtomic _) = True
layeredF (FSequencing form1 form2) =  layeredF form1 &&  layeredF form2
layeredF (FChoice form1 form2) = layeredF form1 && layeredF form2
layeredF (FPar form1 form2) = layeredF form1 && layeredF form2
layeredF (FInsert _) = True
layeredF (Aggregate _ form) = pureF form
layeredF FOne = True
layeredF FZero = True

infixl 5 @@@
infixl 5 @@
infixl 5 @@+
infixl 5 @@-
infixl 4 .*.
infixl 3 .+.
infixl 3 .|.

(@@@) :: PredName -> [Expr] -> Formula
pred' @@@ args = FAtomic (Atom pred' args)

(@@) :: String -> [Expr] -> Formula
label @@ args = FAtomic (Atom (UQPredName label) args)

(@@+) :: String -> [Expr] -> Formula
label @@+ args = FInsert (Lit Pos (Atom (UQPredName label) args))

(@@-) :: String -> [Expr] -> Formula
label @@- args = FInsert (Lit Neg (Atom (UQPredName label) args))

(.*.) :: Formula -> Formula -> Formula
FOne .*. b = b
FZero .*. _ = FZero
a .*. FOne = a
a .*. b = FSequencing a b

(.+.) :: Formula -> Formula -> Formula
FZero .+. b = b
a .+. FZero = a
a .+. b = FChoice a b

(.|.) :: Formula -> Formula -> Formula
FZero .|. b = b
a .|. FZero = a
a .|. b = FPar a b

-- get the top level conjucts
getFsequencings :: Formula -> [Formula]
getFsequencings (FSequencing form1 form2) =  getFsequencings form1 ++ getFsequencings form2
getFsequencings a = [a]

getFchoices :: Formula -> [Formula]
getFchoices (FChoice form1 form2) = getFchoices form1 ++ getFchoices form2
getFchoices a = [a]

getFpars :: Formula -> [Formula]
getFpars (FPar form1 form2) = getFpars form1 ++ getFpars form2
getFpars a = [a]

getFsequencings' :: Formula -> [Formula]
getFsequencings' (FSequencing form1 form2) =  getFsequencings' form1 ++ [form2]
getFsequencings' a = [a]

getFchoices' :: Formula -> [Formula]
getFchoices' (FChoice form1 form2) = getFchoices' form1 ++  [form2]
getFchoices' a = [a]

getFpars' :: Formula -> [Formula]
getFpars' (FPar form1 form2) = getFpars' form1 ++  [form2]
getFpars' a = [a]

fsequencing :: [Formula] -> Formula
fsequencing = foldl (.*.) FOne

fchoice :: [Formula] -> Formula
fchoice = foldl (.+.) FZero

fpar :: [Formula] -> Formula
fpar = foldl (.|.) FZero

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


-- result value
data ResultValue = StringValue T.Text | IntValue Int | ByteStringValue ByteString | RefValue String Location String | Null deriving (Eq , Ord, Show, Read)

typeOf :: ResultValue -> CastType
typeOf (IntValue _) = NumberType
typeOf (StringValue _) = TextType
typeOf (ByteStringValue _) = ByteStringType
typeOf (RefValue reftype _ _) = RefType reftype
typeOf Null = error "typeOf: null value"

instance Num ResultValue where
    IntValue a + IntValue b = IntValue (a + b)
    IntValue a * IntValue b = IntValue (a * b)
    abs (IntValue a) = IntValue (abs a)
    signum (IntValue a) = IntValue (signum a)
    negate (IntValue a) = IntValue (negate a)
    fromInteger i = IntValue (fromInteger i)

instance Fractional ResultValue where
    IntValue a / IntValue b = IntValue (round (fromIntegral a / fromIntegral b))
    fromRational a = IntValue (round (fromRational a))

instance Real ResultValue where
  toRational (IntValue a) = fromIntegral a

instance Enum ResultValue where
  toEnum = IntValue
  fromEnum (IntValue a) = a

instance Integral ResultValue where
  IntValue a `quotRem` IntValue b = let (q, r) = a `quotRem` b in (IntValue q, IntValue r)
  toInteger (IntValue a) = toInteger a
