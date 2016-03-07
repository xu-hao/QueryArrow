{-# LANGUAGE TypeFamilies, MultiParamTypeClasses, FunctionalDependencies, ExistentialQuantification, FlexibleInstances,
   RankNTypes, FlexibleContexts, GADTs #-}

module FO.Data where

import Data.Monoid
import Prelude hiding (lookup)
import Data.Map.Strict (Map, (!), empty, member, insert, singleton, foldrWithKey, foldlWithKey, alter, lookup, fromList, toList, unionWith, unionsWith, intersectionWith, delete)
import Data.List ((\\), intercalate, union)
import Control.Monad.Trans.State.Strict (evalState,get, put, State)
import Control.Applicative ((<$>))
import Control.Monad (foldM)
import Data.Set (Set)
import qualified Data.Set as Set
import Data.Maybe
import qualified Data.Map as Map

-- variable types
type Type = String

data PredKind = ObjectPred | PropertyPred | EssentialPred deriving (Eq, Ord, Show)

-- predicate types
data PredType = PredType PredKind [ParamType] deriving (Eq, Ord, Show)

data ParamType = Key Type
               | Property Type deriving (Eq, Ord, Show)

-- predicate
data Pred = Pred { predName :: String, predType :: PredType} deriving (Eq, Ord, Show)

-- variables
newtype Var = Var {unVar :: String} deriving (Eq, Ord)

-- expression
data Expr = VarExpr Var | IntExpr Int | StringExpr String | PatternExpr String deriving (Eq, Ord)

-- atoms
data Atom = Atom { atomPred :: Pred, atomArgs :: [Expr] } deriving (Eq, Ord)

-- sign of literal
data Sign = Pos | Neg deriving (Eq, Ord)

-- literals
data Lit = Lit { sign :: Sign,  atom :: Atom } deriving (Eq, Ord)

data Formula = Atomic Atom
             | Disjunction [Formula]
             | Conjunction [Formula]
             | Not Formula
             | Exists { boundvar :: Var, formula :: Formula }
             | Forall { boundvar :: Var, formula :: Formula } deriving (Eq, Ord)

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

instance FreeVars Formula where
    freeVars (Atomic atom1) = freeVars atom1
    freeVars (Conjunction formulas) =
        unions (map freeVars formulas)
    freeVars (Disjunction formulas) =
        unions (map freeVars formulas)
    freeVars (Not formula1) =
        freeVars formula1
    freeVars (Exists var formula1) =
        freeVars formula1 \\ [var]
    freeVars (Forall var formula1) =
        freeVars formula1 \\ [var]

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

instance Unify Atom where
    unify (Atom p1 args1) (Atom p2 args2) | p1 == p2 = unify args1 args2
    unify _ _ = Nothing

pushNegations :: Sign -> Formula -> Formula
pushNegations Pos (Conjunction formulas) = conj (map (pushNegations Pos) formulas)
pushNegations Pos (Disjunction formulas) = disj (map (pushNegations Pos) formulas)
pushNegations Pos (Not formula) = pushNegations Neg formula
pushNegations Pos (Exists var formula) = Exists var (pushNegations Pos formula)
pushNegations Pos (Forall var formula) = Forall var (pushNegations Pos formula)
pushNegations Pos a@(Atomic _ ) = a
pushNegations Neg (Conjunction formulas) = disj (map (pushNegations Neg) formulas)
pushNegations Neg (Disjunction formulas) = conj (map (pushNegations Neg) formulas)
pushNegations Neg (Not formula) = pushNegations Pos formula
pushNegations Neg (Exists var formula) = Forall var (pushNegations Neg formula)
pushNegations Neg (Forall var formula) = Exists var (pushNegations Neg formula)
pushNegations Neg a@(Atomic _) = Not a

convertForall :: Sign -> Formula -> Formula
convertForall Pos (Conjunction formulas) = conj (map (convertForall Pos) formulas)
convertForall Pos (Disjunction formulas) = disj (map (convertForall Pos) formulas)
convertForall Pos (Not formula) = convertForall Neg formula
convertForall Pos (Exists var formula) = Exists var (convertForall Pos formula)
convertForall Pos (Forall var formula) = Not (Exists var (convertForall Neg formula))
convertForall Pos a@(Atomic _ ) = a
convertForall Neg (Conjunction formulas) = disj (map (convertForall Neg) formulas)
convertForall Neg (Disjunction formulas) = conj (map (convertForall Neg) formulas)
convertForall Neg (Not formula) = convertForall Pos formula
convertForall Neg (Exists var formula) = Not (Exists var (convertForall Pos formula))
convertForall Neg (Forall var formula) = Exists var (convertForall Neg formula)
convertForall Neg a@(Atomic _) = Not a

splitPosNegLits :: [Lit] -> ([Atom], [Atom])
splitPosNegLits = foldr (\ (Lit thesign theatom) (pos, neg) -> case thesign of
    Pos -> (theatom : pos, neg)
    Neg -> (pos, theatom : neg)) ([],[])

keyComponents :: PredType -> [a] -> [a]
keyComponents (PredType _ paramtypes) = map snd . filter (\(type1, arg) -> case type1 of
    Key _ -> True
    _ -> False) . zip paramtypes

propComponents :: PredType -> [a] -> [a]
propComponents (PredType _ paramtypes) = map snd . filter (\(type1, arg) -> case type1 of
    Property _ -> True
    _ -> False) . zip paramtypes


sortByKey :: [Lit] -> Map [Expr] [Lit]
sortByKey = foldl insertByKey empty where
    insertByKey map1 lit@(Lit _ (Atom (Pred _ predtype) args)) =
        let keyargs = keyComponents predtype args in
            alter (\l -> case l of
                Nothing -> Just [lit]
                Just lits -> Just (lits ++ [lit])) keyargs map1

isObjectPredAtom :: Atom -> Bool
isObjectPredAtom (Atom (Pred _ (PredType ObjectPred _)) _) = True
isObjectPredAtom _ = False

isObjectPredLit :: Lit -> Bool
isObjectPredLit (Lit _ a) = isObjectPredAtom a

sortAtomByPred :: [Atom] -> Map String [Atom]
sortAtomByPred = foldl insertAtomByPred empty where
    insertAtomByPred map1 atom@(Atom (Pred name _) _) =
        alter (\asmaybe -> case asmaybe of
            Nothing -> Just [atom]
            Just as -> Just (as ++ [atom])) name map1

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
    show (Atom (Pred name _) args) = name ++ "(" ++ intercalate "," (map show args) ++ ")"

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
        Neg -> "~" ++ show theatom

instance Show Formula where
    show (Conjunction formulas) = "(" ++ intercalate " " (map show formulas) ++ ")"
    show (Disjunction formulas) = "(" ++ intercalate "|" (map show formulas) ++ ")"
    show (Exists var formula) = "(exists " ++ show var ++ "." ++ show formula ++ ")"
    show (Forall var formula) = "(forall " ++ show var ++ "." ++ show formula ++ ")"
    show (Atomic a) = show a
    show (Not formula) = "~" ++ show formula


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

type Substitution = Map Var Expr

class Subst a where
    subst :: Substitution -> a -> a

instance Subst Expr where
    subst s ve@(VarExpr v) = fromMaybe ve (lookup v s)
    subst _ e = e

instance Subst Substitution where
    subst s = Map.map (subst s)

instance Subst Lit where
    subst s (Lit sign a) = Lit sign (subst s a)

instance Subst Atom where
    subst s (Atom pred args) = Atom pred (subst s args)

instance Subst Formula where
    subst s (Atomic a) = Atomic (subst s a)
    subst s (Disjunction as) = Disjunction (subst s as)
    subst s (Conjunction as) = Conjunction (subst s as)
    subst s (Not a) = Not (subst s a)
    subst s (Forall var a) = Forall var (subst (delete var s) a)
    subst s (Exists var a) = Exists var (subst (delete var s) a)

instance Subst a => Subst [a] where
    subst s = map (subst s)

instance (Subst a, Ord a) => Subst (Set a) where
    subst s = Set.map (subst s)

comp s1 s2 = subst s2 s1 `Map.union` s2

-- instance Monoid Substitution where
--    mempty = empty
--    s1 `mappend` s2 = comp

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

instance FreeVars a => FreeVars (Set a) where
    freeVars sa = unions (map freeVars (Set.toList sa))

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
infixl 4 ===
(-->) :: Formula -> Formula -> Formula
rule --> goal = disj [Not rule, goal]

(<-->) :: Formula -> Formula -> Formula
rule <--> goal = (rule --> goal) & (goal --> rule)

(&) :: Formula -> Formula -> Formula
a & b = conj (getConjuncts a ++ getConjuncts b)

(|||) :: Formula -> Formula -> Formula
a ||| b = disj (getDisjuncts a ++ getDisjuncts b)

-- get the top level conjucts
getConjuncts :: Formula -> [Formula]
getConjuncts (Conjunction conjuncts) = conjuncts
getConjuncts a = [a]

getDisjuncts :: Formula -> [Formula]
getDisjuncts (Disjunction disjuncts) = disjuncts
getDisjuncts a = [a]

conj :: [Formula] -> Formula
conj [a] = a
conj as = Conjunction (concatMap getConjuncts as)

disj :: [Formula] -> Formula
disj [a] = a
disj as = Disjunction (concatMap getDisjuncts as)

(@@) :: Pred -> [Expr] -> Formula
pred @@ args = Atomic (Atom pred args)

eqPred :: Pred
eqPred = Pred "==" (PredType ObjectPred [Key "Any", Key "Any"])

class Equate a where
    (===) :: a -> a -> Formula

instance Equate Expr where
    a === b = eqPred @@ [a, b]

instance Equate a => Equate [a] where
    as === bs = conj (zipWith (===) as bs)
class SubstPred a where
    substPred :: Map Pred Pred -> a -> a

instance SubstPred Formula where
    substPred pmap (Atomic a) = Atomic (substPred pmap a)
    substPred pmap (Conjunction as) = Conjunction (map (substPred pmap) as)
    substPred pmap (Disjunction as) = Disjunction (map (substPred pmap ) as)
    substPred pmap (Not a) = Not (substPred pmap a)
    substPred pmap (Exists v a) = Exists v (substPred pmap a)
    substPred pmap (Forall v a) = Forall v (substPred pmap a)

instance SubstPred Atom where
  substPred pmap (Atom pred args) = Atom (case lookup pred pmap of
          Just p2 -> p2
          _ -> pred) args

-- Wrong way to do it: all rules are implicit necessary i.e. we write r for []r
-- for example r = DATA_OBJ(x) -> <>DATA_NAME(x,y) says if DATA_OBJ(x) then it is possible that x has name y
-- therefore DATA_OBJ(x) insert DATA_NAME(x,y) is valid in the following sense:
-- DATA_OBJ(x) & (DATA_OBJ(x) -> <>DATA_NAME(x,y)) -> <>DATA_NAME(x,y)
-- this type of rules can be used as follows:
-- inserting P with condition C
-- usage: w ||- C & r -> <>P has to be valid
-- ~C | ~r | P
-- in particular DATA_OBJ(x) -> DATA_NAME(x,y) is not true
-- if we have DATA_SIZE(x,z) -> DATA_OBJ(x), then we can derive DATA_SIZE(x,z) -> <> DATA_NAME(x,y)
-- Right way to do it:
-- must have properties:
-- r = META_OBJ(x) <-> META_ATTR_NAME(x,n)
-- usage: for each predicate P(x) define the modified predicate P' as follows,
-- for key parameters s and property parameters s,
-- if P(s,t) is created, p0 = (P'(s,y) <-> y == t) & (~exists s. x == s -> (P'(x,y) <-> P(x,y)))
-- if P(s,t) is deleted, p0 = (~P'(s,y) <-> y == t) & (x /= s -> (P'(x,y) <-> P(x,y)))
-- the equality become trivially true or false during instantiation
-- then the combined predicate P'' is defined by
-- p = (exists x. C(x,s,t) -> (P''(s,t) <-> P'(s,t))) & (~exists x. C(x,s,t) -> (P''(s,t) <-> P(s,t))),
-- this particular for can be skolemized without introducing a function symbol
-- we have to show r & p0 & p-> r[P''/P]
-- r1 = DATA_NAME(x,y) -> DATA_OBJ(x)
-- DATA_OBJ(s) insert DATA_NAME(s,c)
-- We have p0 = (DATA_NAME'(s,y) <-> y == c) & (~exists s. x==s -> (DATA_NAME'(x,y) <-> DATA_NAME(x,y)))
-- which simplifies to p0 = DATA_NAME'(s,y) <-> y == c
-- p = (DATA_OBJ(s) -> (DATA_NAME''(s,t) <-> DATA_NAME'(s,t))) & (~DATA_OBJ(s) -> (DATA_NAME''(s,t) <-> DATA_NAME(s,t)))
-- show that
-- DATA_NAME(x,y) -> DATA_OBJ(x) (1)
-- DATA_NAME'(s,y) <-> y == c (2)
-- DATA_OBJ(s) -> (DATA_NAME''(s,t) <-> DATA_NAME'(s,t)) (3)
-- ~DATA_OBJ(s) -> (DATA_NAME''(s,t) <-> DATA_NAME(s,t)) (4)
-- DATA_NAME''(x,y) (5)
-- -> DATA_OBJ(x)
-- assume ~DATA_OBJ(d) (6)
-- MP (6) (4) DATA_NAME''(d,t) <-> DATA_NAME(d,t) (7)
-- MP (5) (7) DATA_NAME(d,t) (8)
-- MP (1) (8) DATA_OBJ(d) (9)
-- MP (6) (9) False

-- predicate map
type PredMap = Map String Pred

-- runLogic Solver (map convert instances)
class TheoremProver_ p where
    prove :: p -> [Formula] -> Formula -> IO (Maybe Bool)

data TheoremProver where
   TheoremProver :: forall p. TheoremProver_ p => p -> TheoremProver
