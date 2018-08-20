{-# LANGUAGE TypeFamilies, MultiParamTypeClasses, ExistentialQuantification, FlexibleInstances, StandaloneDeriving, DeriveFunctor, UndecidableInstances, DeriveGeneric,
   RankNTypes, FlexibleContexts, GADTs, PatternSynonyms, ScopedTypeVariables, TemplateHaskell #-}

module QueryArrow.FO.Data where

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
import Control.Comonad
import Control.Comonad.Cofree
import Data.Eq.Deriving (deriveEq1)
import Data.Ord.Deriving (deriveOrd1)
import Text.Read.Deriving (deriveRead1)
import Text.Show.Deriving (deriveShow1)    
import Data.Functor.Classes (Show1)
import QueryArrow.Syntax.Type
import Debug.Trace

-- predicate kinds
data PredKind = ObjectPred | PropertyPred deriving (Eq, Ord, Show, Read)

-- predicate types
data PredType = PredType {predKind :: PredKind, paramsType :: [ParamType]} deriving (Eq, Ord, Show, Read)

data ParamType = ParamType {isKey :: Bool,  isInput :: Bool,  isOutput :: Bool, isReference:: Bool, paramType :: CastType} deriving (Eq, Ord, Show, Read)

-- predicate
type PredName = ObjectPath String

instance Key String where

deriving instance (Key a, Read a) => Read (ObjectPath a)
deriving instance (Key a, Read a) => Read (NamespacePath a)

data Pred = Pred {  predName :: PredName, predType :: PredType} deriving (Eq, Ord, Show, Read)

-- variables
newtype Var = Var {unVar :: String} deriving (Eq, Ord, Show, Read)


-- http://stackoverflow.com/questions/27157717/boilerplate-free-annotation-of-asts-in-haskell

-- expression
data ExprF a = VarExprF Var | ConsExprF String | IntExprF Integer | StringExprF T.Text | AppExprF a a | NullExprF | CastExprF CastType a deriving (Functor)

$(deriveEq1 ''ExprF)
$(deriveOrd1 ''ExprF)
$(deriveShow1 ''ExprF)
$(deriveRead1 ''ExprF)

type Expr1 = Cofree ExprF

type Expr = Expr1 () 


pattern VarExpr a = () :< VarExprF a
pattern ConsExpr a = () :< ConsExprF a
pattern IntExpr a = () :< IntExprF a
pattern StringExpr a = () :< StringExprF a
pattern AppExpr a b = () :< AppExprF a b
pattern NullExpr = () :< NullExprF
pattern CastExpr a b = () :< CastExprF a b
pattern ListConsExpr a b = AppExpr (AppExpr (ConsExpr "(:)") a) b
pattern ListNilExpr = ConsExpr "[]"

listExpr :: [Expr] -> Expr
listExpr = foldr ListConsExpr ListNilExpr

exprListFromExpr :: Expr -> [Expr]
exprListFromExpr (ListConsExpr a b) = let tl = exprListFromExpr b in
                                      a:tl
exprListFromExpr ListNilExpr = []
exprListFromExpr a = error ("exprListFromExpr: malformatted list " ++ show a)

-- atoms
data Atom1 a = Atom1 { atomPred :: PredName, atomArgs :: [Expr1 a] } deriving (Eq, Ord, Show, Read)

type Atom = Atom1 ()

pattern Atom a b = Atom1 a b


-- sign of literal
data Sign = Pos | Neg deriving (Eq, Ord, Show, Read)

-- literals
data Lit1 a = Lit1 { sign :: Sign,  atom :: Atom1 a } deriving (Eq, Ord, Show, Read)

type Lit = Lit1 ()

pattern Lit a b = Lit1 a b


data Summary = Random Var | Max Var | Min Var | Sum Var | Average Var | CountDistinct Var | Count deriving (Eq, Ord, Show, Read)
data Aggregator = Summarize [(Var, Summary)] [Var] | Limit Int | OrderByAsc Var | OrderByDesc Var | Distinct | Not | Exists | FReturn [Var] deriving (Eq, Ord, Show, Read)

data FormulaF a f = FAtomicF (Atom1 a)
             | FInsertF (Lit1 a)
             | FChoiceF f f
             | FSequencingF f f
             | FParF f f
             | FOneF
             | FZeroF
             | AggregateF Aggregator f deriving Functor

$(deriveEq1 ''FormulaF)
$(deriveOrd1 ''FormulaF)
$(deriveShow1 ''FormulaF)
$(deriveRead1 ''FormulaF)

type Formula2 a = Cofree (FormulaF a)

type Formula = Formula2 () ()


pattern FAtomic a = () :< FAtomicF a
pattern FInsert a = () :< FInsertF a
pattern FChoice a b = () :< FChoiceF a b
pattern FSequencing a b = () :< FSequencingF a b
pattern FPar a b = () :< FParF a b
pattern FZero = () :< FZeroF
pattern FOne = () :< FOneF
pattern Aggregate a b = () :< AggregateF a b

pattern FAtomicA ann a = ann  :< FAtomicF a
pattern FInsertA ann a = ann  :< FInsertF a
pattern FChoiceA ann a b = ann  :< FChoiceF a b
pattern FSequencingA ann a b = ann  :< FSequencingF a b
pattern FParA ann a b = ann  :< FParF a b
pattern FZeroA ann = ann  :< FZeroF
pattern FOneA ann = ann  :< FOneF
pattern AggregateA ann a b = ann  :< AggregateF a b
{-
isConst :: Expr -> Bool
isConst (IntExpr _) = True
isConst (StringExpr _) = True
isConst _ = False
-}
-- free variables


class FreeVars a where
    freeVars :: a -> Set Var

instance FreeVars (Expr1 a) where
    freeVars e1 = case unwrap e1 of
      CastExprF _ e -> freeVars e
      VarExprF var0 -> Set.singleton var0
      ConsExprF var0 -> bottom
      IntExprF _ -> bottom
      StringExprF _ -> bottom
      AppExprF a b -> freeVars a \/ freeVars b
      NullExprF -> bottom

instance FreeVars (Atom1 a) where
    freeVars (Atom _ theargs) = freeVars theargs

instance FreeVars (Lit1 a) where
    freeVars (Lit _ theatom) = freeVars theatom

instance FreeVars (Formula2 a b) where
    freeVars f1 = case unwrap f1 of
      FAtomicF atom1 ->
        freeVars atom1
      FInsertF lits ->
        freeVars lits
      FSequencingF form1 form2 ->
        freeVars form1 \/ freeVars form2
      FChoiceF form1 form2 ->
        freeVars form1 \/ freeVars form2
      FParF form1 form2 ->
        freeVars form1 \/ freeVars form2
      AggregateF agg form ->
        freeVars agg \/ freeVars form
      FZeroF -> bottom
      FOneF -> bottom

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
  freeVars (Random a) = singleton a

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

extractVar :: (Show (Expr1 a)) => Expr1 a -> Var
extractVar e1 = case unwrap e1 of
                    VarExprF var0 -> var0
                    _ -> error ("extractVar: this is not a var expr " ++ show e1)

instance Subst Var where
    subst s v = extractVar (subst s (VarExpr v))

instance Subst Summary where
    subst s (Max v) = Max (subst s v)
    subst s (Min v) = Min (subst s v)
    subst _ Count = Count
    subst s (Average v) = Average (subst s v)
    subst s (Sum v) = Sum (subst s v)
    subst s (CountDistinct v) = CountDistinct (subst s v)
    subst s (Random v) = Random (subst s v)

instance Subst (Atom1 a) => Subst (Lit1 a) where
    subst s (Lit1 sign0 a) = Lit1 sign0 (subst s a)

instance (Subst (Atom1 a), Subst b) => Subst (Formula2 a b) where
    subst s a = (subst s (extract a)) :< case unwrap a of
                        FAtomicF a -> FAtomicF (subst s a)
                        FInsertF lits -> FInsertF (subst s lits)
                        FSequencingF form1 form2 -> FSequencingF form1 form2
                        FChoiceF form1 form2 -> FChoiceF form1 form2
                        FParF form1 form2 -> FParF form1 form2
                        AggregateF (FReturn vars) form -> AggregateF (FReturn (subst s vars)) form
                        AggregateF Not a -> AggregateF Not a
                        AggregateF Exists a -> AggregateF Exists a
                        AggregateF (Summarize funcs groupby) a -> AggregateF (Summarize (subst s funcs) (map (subst s) groupby)) a
                        AggregateF (Limit n) a -> AggregateF (Limit n) a
                        AggregateF Distinct a -> AggregateF Distinct a
                        AggregateF (OrderByAsc var1) a -> AggregateF (OrderByAsc (subst s var1)) a
                        AggregateF (OrderByDesc var1) a -> AggregateF (OrderByDesc (subst s var1)) a
                        FZeroF -> FZeroF
                        FOneF -> FOneF

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
pureF :: Formula2 a f -> Bool
pureF f1 = case unwrap f1 of
  FAtomicF _ -> True
  FSequencingF form1 form2 -> pureF form1 &&  pureF form2
  FChoiceF form1 form2 -> pureF form1 && pureF form2
  FParF form1 form2 -> pureF form1 && pureF form2
  FInsertF _ -> False
  AggregateF _ form -> pureF form
  FOneF -> True
  FZeroF -> True

layeredF :: Formula2 a f -> Bool
layeredF f1 = case unwrap f1 of
  FAtomicF _ -> True
  FSequencingF form1 form2 -> layeredF form1 &&  layeredF form2
  FChoiceF form1 form2 -> layeredF form1 && layeredF form2
  FParF form1 form2 -> layeredF form1 && layeredF form2
  FInsertF _ -> True
  AggregateF _ form -> pureF form
  FOneF -> True
  FZeroF -> True

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
getFsequencings :: Formula2 a f -> [Formula2 a f]
getFsequencings f1 = case unwrap f1 of
  (FSequencingF form1 form2) ->  getFsequencings form1 ++ getFsequencings form2
  _ -> [f1]

getFchoices :: Formula2 a f -> [Formula2 a f]
getFchoices f1 = case unwrap f1 of
  (FChoiceF form1 form2) -> getFchoices form1 ++ getFchoices form2
  _ -> [f1]

getFpars :: Formula2 a f -> [Formula2 a f]
getFpars f1 = case unwrap f1 of
  (FParF form1 form2) -> getFpars form1 ++ getFpars form2
  _ -> [f1]

getFsequencings' :: Formula2 a f -> [Formula2 a f]
getFsequencings' f1 = case unwrap f1 of
  (FSequencingF form1 form2) -> getFsequencings' form1 ++ [form2]
  _ -> [f1]

getFchoices' :: Formula2 a f -> [Formula2 a f]
getFchoices' f1 = case unwrap f1 of
  (FChoiceF form1 form2) -> getFchoices' form1 ++  [form2]
  _ -> [f1]

getFpars' :: Formula2 a f -> [Formula2 a f]
getFpars' f1 = case unwrap f1 of
  (FParF form1 form2) -> getFpars' form1 ++  [form2]
  _ -> [f1]

fsequencing :: [Formula] -> Formula
fsequencing = foldl (.*.) ( FOne)

fsequencing1 :: Monoid b => [Formula2 a b] -> Formula2 a b
fsequencing1 = foldl1 (\ f1@(b1 :< _)  f2@(b2 :< _) -> (b1 <> b2) :< (FSequencingF f1 f2))

fchoice :: [Formula] -> Formula
fchoice = foldl (.+.) ( FZero)

fchoice1 :: Monoid b => [Formula2 a b] -> Formula2 a b
fchoice1 = foldl1 (\ f1@(b1 :< _)  f2@(b2 :< _) -> (b1 <> b2) :< (FChoiceF f1 f2))

fpar :: [Formula] -> Formula
fpar = foldl (.|.) ( FZero)

fpar1 :: Monoid b => [Formula2 a b] -> Formula2 a b
fpar1 = foldl1 (\ f1@(b1 :< _)  f2@(b2 :< _) -> (b1 <> b2) :< (FParF f1 f2))

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


