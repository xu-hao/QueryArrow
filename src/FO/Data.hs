{-# LANGUAGE TypeFamilies, MultiParamTypeClasses, FunctionalDependencies, ExistentialQuantification, FlexibleInstances,
   RankNTypes, FlexibleContexts, GADTs, PatternSynonyms #-}

module FO.Data where

import Prelude hiding (lookup)
import Data.Map.Strict (Map, empty, insert, alter, lookup, fromList, delete)
import Data.List ((\\), intercalate, union, unwords)
import Control.Monad.Trans.State.Strict (evalState,get, put, State)
import Data.Set (Set)
import qualified Data.Set as Set
import Data.Maybe
import qualified Data.Map as Map
import Data.Convertible
import Control.Monad.Except
import qualified Data.Text as T

-- variable types
type Type = String

data PredKind = ObjectPred | PropertyPred deriving (Eq, Ord, Show)

-- predicate types
data PredType = PredType PredKind [ParamType] deriving (Eq, Ord, Show)

data ParamType = Key Type
               | Property Type deriving (Eq, Ord, Show)

-- predicate
data PredName = PredName {namespace :: Maybe String, name :: String} deriving (Eq, Ord, Show)

data Pred = Pred {  predName :: PredName, predType :: PredType} deriving (Eq, Ord, Show)

-- variables
newtype Var = Var {unVar :: String} deriving (Eq, Ord)

-- expression
data Expr = VarExpr Var | IntExpr Int | StringExpr T.Text | PatternExpr T.Text deriving (Eq, Ord)

-- atoms
data Atom = Atom { atomPred :: Pred, atomArgs :: [Expr] } deriving (Eq, Ord)

-- sign of literal
data Sign = Pos | Neg deriving (Eq, Ord)

-- literals
data Lit = Lit { sign :: Sign,  atom :: Atom } deriving (Eq, Ord)

data PureFormula = Return [Var]
             | Atomic Atom
             | Disjunction PureFormula PureFormula
             | Conjunction PureFormula PureFormula
             | Not PureFormula
             | Exists { boundvar :: Var, formula :: PureFormula }
             | CTrue
             | CFalse deriving (Eq, Ord)
data Formula = FTransaction
             | FReturn [Var]
             | FAtomic Atom
             | FInsert Lit
             | FClassical PureFormula
             | FChoice Formula Formula
             | FSequencing Formula Formula
             | FOne
             | FZero deriving (Eq, Ord)

instance Convertible PureFormula Formula where
    safeConvert (Return a) = Right (FReturn a)
    safeConvert (Atomic a) = Right (FAtomic a)
    safeConvert (Disjunction a b) = Right (FChoice  (convert a) (convert b))
    safeConvert (Conjunction a b) = Right (FSequencing (convert a)  (convert b))
    safeConvert f@(Not _) = Right (FClassical f)
    safeConvert f@(Exists _ _) = Right (FClassical f)
    safeConvert CTrue = Right FOne
    safeConvert CFalse = Right FZero

instance Convertible Formula PureFormula where
    safeConvert (FReturn a) = Right (Return a)
    safeConvert (FAtomic a) = Right (Atomic a)
    safeConvert (FClassical a) = Right a
    safeConvert (FChoice a b) = Disjunction <$> (safeConvert a) <*> (safeConvert b)
    safeConvert (FSequencing a b) = Conjunction <$> (safeConvert a) <*> (safeConvert b)
    safeConvert f@(FInsert _) = Left (ConvertError "Formula" "PureFormula" (show f) "cannot convert")
    safeConvert f@(FTransaction) = Left (ConvertError "Formula" "PureFormula" (show f) "cannot convert")
    safeConvert FOne = Right CTrue
    safeConvert FZero = Right CFalse

intersects :: Eq a => [[a]] -> [a]
intersects [] = error "can't intersect zero lists"
intersects (hd : tl) = foldl (\ as bs -> [ x | x <- as, x `elem` bs ]) hd tl

unions :: Eq a => [[a]] -> [a]
unions = foldl union []

unique :: [a] -> a
unique as = if length as /= 1
        then error "More than one primary parameters and nonzero secondary parameters"
        else head as

isConst :: Expr -> Bool
isConst (VarExpr _) = False
isConst _ = True
-- free variables


class FreeVars a where
    freeVars :: a -> [Var]

instance FreeVars Expr where
    freeVars (VarExpr var) = [var]
    freeVars _ = []

instance FreeVars Atom where
    freeVars (Atom _ theargs) = foldl (\fvs expr ->
        fvs `union` freeVars expr) [] theargs



instance FreeVars Lit where
    freeVars (Lit _ theatom) = freeVars theatom

instance FreeVars PureFormula where
    freeVars (Return _) =
        []
    freeVars (Atomic atom1) = freeVars atom1
    freeVars (Conjunction form1 form2) =
        freeVars form1 `union` freeVars form2
    freeVars (Disjunction form1 form2) =
        freeVars form1 `union` freeVars form2
    freeVars (Not formula1) =
        freeVars formula1
    freeVars (Exists var formula1) =
        freeVars formula1 \\ [var]
    freeVars _ = []

instance FreeVars Formula where
    freeVars (FReturn _) =
        []
    freeVars (FAtomic atom1) =
        freeVars atom1
    freeVars (FInsert lits) =
        freeVars lits
    freeVars (FClassical formula1) =
        freeVars formula1
    freeVars (FTransaction ) =
        []
    freeVars (FSequencing form1 form2) =
        freeVars form1 `union` freeVars form2
    freeVars (FChoice form1 form2) =
        freeVars form1 `union` freeVars form2
    freeVars _ = []

instance FreeVars a => FreeVars [a] where
    freeVars = unions. map freeVars

class Unify a where
    unify :: a -> a -> Maybe Substitution

instance Unify Expr where
    unify (VarExpr var) e = Just (Map.singleton var e)
    unify e (VarExpr var) = Just (Map.singleton var e)
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

keyComponents :: Pred -> [a] -> [a]
keyComponents (Pred _ (PredType _ paramtypes)) = map snd . filter (\(type1, _) -> case type1 of
    Key _ -> True
    _ -> False) . zip paramtypes

propComponents :: Pred -> [a] -> [a]
propComponents (Pred _ (PredType _ paramtypes)) = map snd . filter (\(type1, _) -> case type1 of
    Property _ -> True
    _ -> False) . zip paramtypes

isObjectPred :: Pred -> Bool
isObjectPred (Pred _ (PredType predKind _)) = case predKind of
            ObjectPred -> True
            PropertyPred -> False

constructPredMap :: [Pred] -> PredMap
constructPredMap = foldl (\map1 pred1@(Pred name _) -> insert name pred1 map1) empty

sortByKey :: [Lit] -> Map [Expr] [Lit]
sortByKey = foldl insertByKey empty where
    insertByKey map1 lit@(Lit _ (Atom pred1 args)) =
        let keyargs = keyComponents pred1 args in
            alter (\l -> case l of
                Nothing -> Just [lit]
                Just lits -> Just (lits ++ [lit])) keyargs map1

isObjectPredAtom :: Atom -> Bool
isObjectPredAtom (Atom (Pred _ (PredType ObjectPred _)) _) = True
isObjectPredAtom _ = False

isObjectPredLit :: Lit -> Bool
isObjectPredLit (Lit _ a) = isObjectPredAtom a

sortAtomByPred :: [Atom] -> Map Pred [Atom]
sortAtomByPred = foldl insertAtomByPred empty where
    insertAtomByPred map1 a@(Atom pred1 _) =
        alter (\asmaybe -> case asmaybe of
            Nothing -> Just [a]
            Just as -> Just (as ++ [a])) pred1 map1

pattern QPredName ns n = PredName (Just ns) n
pattern UQPredName n = PredName Nothing n

predNameToString :: PredName -> String
predNameToString (PredName mns n)= case mns of
                            Just ns -> ns ++ "." ++ n
                            Nothing -> n

predNameToString2 :: PredName -> String
predNameToString2 (PredName _ n) = n

predNameMatches :: PredName -> PredName -> Bool
predNameMatches (PredName Nothing n1) (PredName Nothing n2) = n1 == n2
predNameMatches (PredName (Just _) n1) (PredName Nothing n2) = n1 == n2
predNameMatches (PredName (Just ns1) n1) (PredName (Just ns2) n2) = ns1 == ns2 && n1 == n2
predNameMatches _ _ = False

setNamespace :: String -> PredName -> PredName
setNamespace ns (UQPredName n) = QPredName ns n
setNamespace ns1 n@(QPredName ns2 _) | ns1 == ns2 = n
                                     | otherwise = error ("cannot set namespace to a qualified predicate name" ++ ns1 ++ ns2)

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

instance Show Atom where
    show (Atom (Pred name _) args) = (predNameToString name) ++ "(" ++ intercalate "," (map show args) ++ ")"

instance Show Expr where
    show (VarExpr var) = show var
    show (IntExpr i) = show i
    show (StringExpr s) = show s
    show (PatternExpr p) = show p

instance Show Var where
    show (Var s) = s

instance Show Lit where
    show (Lit thesign theatom) = case thesign of
        Pos -> show theatom
        Neg -> "¬" ++ show theatom

instance Show PureFormula where
    show (Return vars) = "(return " ++ unwords (map show vars) ++ ")"
    show (Atomic a) = show a
    show form@(Conjunction _ _) = "(" ++ intercalate " ⊗ " (map show (getConjuncts' form)) ++ ")"
    show form@(Disjunction _ _) = "(" ++ intercalate " ⊕ " (map show (getDisjuncts' form)) ++ ")"
    show (Exists var form) = "(∃ " ++ show var ++ "." ++ show form ++ ")"
    show (Not form) = "¬(" ++ show form ++ ")"
    show (CTrue) = "𝟏"
    show (CFalse) = "𝟎"

instance Show Formula where
    show (FReturn vars) = "(return " ++ unwords (map show vars) ++ ")"
    show (FAtomic a) = show a
    show (FInsert lits) = "(insert " ++ show lits ++ ")"
    show (FClassical form) = "(" ++ show form ++ ")"
    show (FTransaction ) = "transactional"
    show form@(FSequencing _ _) = "(" ++ intercalate " ⊗ " (map show (getFsequencings' form)) ++ ")"
    show form@(FChoice _ _) = "(" ++ intercalate " ⊕ " (map show (getFchoices' form)) ++ ")"
    show (FOne) = "𝟏"
    show (FZero) = "𝟎"

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
    new var = VarExpr <$> new (VarExpr var)

instance New String StringWrapper where
    new (StringWrapper var) = do
        (vidmap, vars) <- get
        let findVN vid =
                let vn = var ++ if vid == negate 1 then "" else show vid in
                    if vn `elem` vars
                        then findVN (vid + 1)
                        else (vn, vid)
        let (vn, vid) = findVN (fromMaybe (negate 1) (lookup var vidmap))
        put (insert var (vid + 1) vidmap, vars)
        return vn

instance New Var Var where
    new (Var var) = Var <$> new (StringWrapper var)
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
    subst _ e = e

instance Subst Substitution where
    subst s = Map.map (subst s)

instance Subst Lit where
    subst s (Lit sign' a) = Lit sign' (subst s a)

instance Subst Atom where
    subst s (Atom pred' args) = Atom pred' (subst s args)

instance Subst PureFormula where
    subst s form@(Return _) = form
    subst s (Atomic a) = Atomic (subst s a)
    subst s (Disjunction a b) = Disjunction (subst s a) (subst s b)
    subst s (Conjunction a b) = Conjunction (subst s a) (subst s b)
    subst s (Not a) = Not (subst s a)
    subst s (Exists var a) = Exists var (subst (delete var s) a)
    subst _ form = form

instance Subst Formula where
    subst s form@(FReturn _) = form
    subst s (FAtomic a) = FAtomic (subst s a)
    subst s (FInsert lits) = FInsert (subst s lits)
    subst s (FClassical a) = FClassical (subst s a)
    subst s (FTransaction ) = FTransaction
    subst s (FSequencing form1 form2) = FSequencing (subst s form1) (subst s form2)
    subst s (FChoice form1 form2) = FChoice (subst s form1) (subst s form2)
    subst _ form = form

instance Subst a => Subst [a] where
    subst s = map (subst s)

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
    freeVars sa = unions (map freeVars (Set.toList sa))

pureF :: Formula -> Bool
pureF (FReturn _) = True
pureF (FAtomic _) = True
pureF (FClassical _) = False
pureF (FTransaction ) = False
pureF (FSequencing form1 form2) =  pureF form1 &&  pureF form2
pureF (FChoice form1 form2) = pureF form1 && pureF form2
pureF (FInsert _) = False
pureF FOne = True
pureF FZero = True

instantiate :: (Subst a, FreeVars a, Monad m) => m Expr -> a -> m a
instantiate es rule = do
    let fv = freeVars rule
    choose <- mapM (const es) fv
    let s = fromList (zip fv choose)
    return (subst s rule)

infixr 1 <-->
infixr 1 -->
infixl 2 |||
infixl 3 &
infixr 5 @@
(-->) :: PureFormula -> PureFormula -> PureFormula
rule --> goal = disj [Not rule, goal]

(<-->) :: PureFormula -> PureFormula -> PureFormula
rule <--> goal = (rule --> goal) & (goal --> rule)

(&) :: PureFormula -> PureFormula -> PureFormula
CTrue & b = b
CFalse & _ = CFalse
a & CTrue = a
_ & CFalse = CFalse
a & b = Conjunction a b

(|||) :: PureFormula -> PureFormula -> PureFormula
CTrue ||| _ = CTrue
CFalse ||| b = b
_ ||| CTrue = CTrue
a ||| CFalse = a
a ||| b = Disjunction a b

(.*.) :: Formula -> Formula -> Formula
FOne .*. b = b
FZero .*. _ = FZero
a .*. FOne = a
a .*. b = FSequencing a b

(.+.) :: Formula -> Formula -> Formula
FZero .+. b = b
a .+. FZero = a
a .+. b = FChoice a b

-- get the top level conjucts
getConjuncts :: PureFormula -> [PureFormula]
getConjuncts (Conjunction form1 form2) =  getConjuncts form1 ++ getConjuncts form2
getConjuncts a = [a]

getDisjuncts :: PureFormula -> [PureFormula]
getDisjuncts (Disjunction form1 form2) =  getDisjuncts form1 ++ getDisjuncts form2
getDisjuncts a = [a]

getFsequencings :: Formula -> [Formula]
getFsequencings (FSequencing form1 form2) =  getFsequencings form1 ++ getFsequencings form2
getFsequencings a = [a]

getFchoices :: Formula -> [Formula]
getFchoices (FChoice form1 form2) = getFchoices form1 ++ getFchoices form2
getFchoices a = [a]

getConjuncts' :: PureFormula -> [PureFormula]
getConjuncts' (Conjunction form1 form2) =  getConjuncts' form1 ++ [form2]
getConjuncts' a = [a]

getDisjuncts' :: PureFormula -> [PureFormula]
getDisjuncts' (Disjunction form1 form2) =  getDisjuncts' form1 ++ [form2]
getDisjuncts' a = [a]

getFsequencings' :: Formula -> [Formula]
getFsequencings' (FSequencing form1 form2) =  getFsequencings' form1 ++ [form2]
getFsequencings' a = [a]

getFchoices' :: Formula -> [Formula]
getFchoices' (FChoice form1 form2) = getFchoices' form1 ++  [form2]
getFchoices' a = [a]

conj :: [PureFormula] -> PureFormula
conj = foldl (&) CTrue

disj :: [PureFormula] -> PureFormula
disj = foldl (|||) CFalse

fsequencing :: [Formula] -> Formula
fsequencing = foldl (.*.) FOne

fchoice :: [Formula] -> Formula
fchoice = foldl (.+.) FZero

(@@) :: Pred -> [Expr] -> PureFormula
pred' @@ args = Atomic (Atom pred' args)

class SubstPred a where
    substPred :: Map Pred Pred -> a -> a

instance SubstPred PureFormula where
    substPred _ form@(Return _) = form
    substPred pmap (Atomic a) = Atomic (substPred pmap a)
    substPred pmap (Conjunction form1 form2) = Conjunction ( substPred pmap form1) ( substPred pmap form2)
    substPred pmap (Disjunction form1 form2) = Disjunction (substPred pmap form1) (substPred pmap form2)
    substPred pmap (Not a) = Not (substPred pmap a)
    substPred pmap (Exists v a) = Exists v (substPred pmap a)
    substPred _ form = form

instance SubstPred Formula where
    substPred _ form@(FReturn _) = form
    substPred pmap (FAtomic a) = FAtomic (substPred pmap a)
    substPred pmap (FClassical a) = FClassical (substPred pmap a)
    substPred pmap (FTransaction) = FTransaction
    substPred pmap (FSequencing form1 form2) = FSequencing (substPred pmap form1) (substPred pmap form2)
    substPred pmap (FChoice form1 form2) = FChoice (substPred pmap form1) (substPred pmap form2)
    substPred pmap (FInsert lits) = FInsert (substPred pmap lits)
    substPred _ form = form

instance SubstPred Atom where
  substPred pmap (Atom pred0 args) = Atom (case lookup pred0 pmap of
        Just p2 -> p2
        _ -> pred0) args

instance SubstPred Lit where
    substPred pmap (Lit sign0 atom0) = Lit sign0 (substPred pmap atom0)

instance SubstPred a => SubstPred [a] where
    substPred pmap = map (substPred pmap)


-- predicate map
type PredMap = Map PredName Pred

checkFormula' :: PureFormula -> Except String ()
checkFormula' (Return _) = return ()
checkFormula' (Atomic a) = checkAtom a
checkFormula' (Disjunction form1 form2) = do
    checkFormula' form1
    checkFormula' form2
checkFormula' (Conjunction form1 form2) = do
    checkFormula' form1
    checkFormula' form2
checkFormula' (Not a) = checkFormula' a
checkFormula' (Exists _ a) = checkFormula' a
checkFormula' _ = return ()

checkFormula :: Formula -> Except String ()
checkFormula (FReturn _) = return ()
checkFormula (FAtomic a) = checkAtom a
checkFormula (FClassical a) = checkFormula' a
checkFormula (FTransaction) = return ()
checkFormula (FSequencing form1 form2) = do
    checkFormula form1
    checkFormula form2
checkFormula (FChoice form1 form2) = do
    checkFormula form1
    checkFormula form2
checkFormula (FInsert lit) =
    checkLit lit
checkFormula _ = return ()

checkLit :: Lit -> Except String ()
checkLit (Lit _ a) = checkAtom a

checkAtom :: Atom -> Except String ()
checkAtom (Atom (Pred _ (PredType _ ptypes)) args) =
    if length ptypes /= length args
        then throwError "number of arguments doesn't match predicate"
        else return ()
