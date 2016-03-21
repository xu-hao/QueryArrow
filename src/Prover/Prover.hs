{-# LANGUAGE RankNTypes , GADTs , MultiParamTypeClasses #-}

module Prover.Prover where

import Prelude hiding (lookup)
import FO.Data
import Control.Monad.Trans.State
import Data.Map (lookup, insert, elems, empty, Map)
import Data.List ((\\), union)
import Control.Monad.Trans.Class
import Data.Maybe
import Config
import Prover.Parser
import qualified Data.ByteString.Lazy as B
import Data.ByteString.Lazy.UTF8 (toString)

-- runLogic Solver (map convert instances)
class TheoremProver_ p where
    prove :: p -> [PureFormula] -> PureFormula -> IO (Maybe Bool)

data TheoremProver where
   TheoremProver :: forall p. TheoremProver_ p => p -> TheoremProver

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
getRules :: PredMap -> VerificationInfo -> IO  [Input]
getRules predmap ps = do
   d0 <- toString <$> B.readFile (rule_file_path ps)
   let d = parseTPTP predmap d0
   return d
   {-     let veriinfo = verifier transinfo
       tp <- getVerifier veriinfo
       rules <- getRules predmap0 veriinfo
   -}
   {-    liftIO $ print "calling verifier"
       let (Just (verifier, rules)) = v
       let formulas = map (\(_,_,a) -> a) rules
       vres0 <- liftIO $    validate verifier qu
       case vres0 of
                   Just lit -> error ("Deleting a literal not implied by condition " ++ show lit)
                   Nothing -> do
                       vres1 <- liftIO $ validateInsert verifier formulas qu
                       case vres1 of
                           Just True -> do
                               let insp = insertPlan dbs insmap qu
                               let (vars, rs) = execQueryPlan dbs (vars, rs) insp
                               rs
                           _ -> error "cannot verify insert statement is correct"
   -}


-- The purpose of this function is to eliminate the situation where the negative literals in the insert clause
-- causes additional constraints to be added to the translated statement. These constraints are cause by the non-key
-- parameters. For example, if we have P(x,y) where x is a key parameter and y is a non key parameter, the mapping of
-- P to a target database contains two parts. One is a condition under which a data structure (such as a row or a subgraph)
-- matches the key argument (thereby fixing the data structure). The other is a data strcuture which stores the
-- information in y. When inserting P(a,b) for arbitrary a and b, P is translated to (schematically):
-- INSERT b WHERE a, whereas when deleting P(a, b), P is translated to: DELETE WHERE a AND b. This create an asymmetry.
-- In the deletion case, if b is added to the condition and subsequently merged to the condition of the whole query, then
-- it may constrain the value of a, thereby constraining other literals in the insertion clause (possibly indirectly)
-- constrained by a. For example, insert ~P(a,b) Q(a,c) /= insert ~P(a,b); insert Q(a,c)
-- To avoid this situation, we require that when a negative literal (~P(a, b)) appears in the insert clause,
-- its positive counterpart (P(a, b)) must be a logical consequence of condition of the statement, given a set of rules.
-- This ensures that this literal (~P(a, b)) doesn't constraint a more than the condition of the statement already does.
-- The set of rules given has to reflect the mapping f, i.e., the mapping must have the following property:
-- If condition & rule -> b in FO then f(condition) -> f(b) in the target database.
validate :: TheoremProver -> [Lit] -> PureFormula -> IO (Maybe Lit)
validate (TheoremProver a)  lits cond =
   if length lits == 1
       then return Nothing -- if there is only one literal we don't have to validate
       else validateNegLits cond neg where
           (_, neg) = splitPosNegLits lits
           validateNegLits _ [] = return Nothing
           validateNegLits cond (a1 : as) = do
               p <- prove a [cond] (Atomic a1)
               case p of
                   Just True -> validateNegLits cond as
                   _ -> return (Just (Lit Neg a1))

type ValEnv a = StateT (Map Pred Pred) NewEnv a


instance New Pred Pred where
   new pred@(Pred p pt) = do
     newp <- new (StringWrapper p)
     return (Pred newp pt)

newPred :: Pred -> ValEnv Pred
newPred pred@(Pred p pt) = do
   newpred <- lift $ new pred
   map1 <- get
   put (insert pred newpred map1)
   return newpred

getPredCurrent :: Pred -> ValEnv Pred
getPredCurrent pred = do
   map1 <- get
   return (fromMaybe pred (lookup pred map1))

generalize formula = let fv = freeVars formula in foldr Forall formula fv

-- here we implement a restricted version functional dependency
-- given a predicate P with parameters x, and parameters y such that
-- y functionally depend on x, written P(x,y) | x -> y
-- the create action: C insert P(s,t) where C is a condition modifies P -> P'
-- P' is uniquely determined by the following formula:
-- let vc = fv C \\ fv s \\ fv t
--     vs = fv s
--     (fv t \\ fv s should be [] since y functionally dependent on x)
-- forall vs. (exists vc. C & x == s) -> (P'(x, y) <-> y == t)) &
--           ~(exists vc. C & x == s) -> (P'(x, y) <-> P(x, y))
ruleP' :: PureFormula -> Pred -> [Expr] -> ValEnv PureFormula
ruleP' cond pred@(Pred p pt@(PredType _ paramtypes)) args = do
   lift $ registerVars (freeVars args)
   lift $ register [p]
   newvars <- lift $ new args
   let s = keyComponents pt args
       x = keyComponents pt newvars
       t = propComponents pt args
       y = propComponents pt newvars
       vc = (freeVars cond `union` freeVars s) \\ freeVars t
       vc1 = (freeVars cond `union` freeVars s)
   oldpred <- getPredCurrent pred
   newpred <- newPred pred
   let newcond = foldr Exists (cond & x === s) vc
   let newcond1 = foldr Exists (cond & x === s) vc1
   return (if not (null (freeVars t \\ freeVars s))
       then error "unbounded vars"
       else generalize ((newcond --> (newpred @@ newvars <--> y === t)) &
               (Not newcond1 --> (newpred @@ newvars <--> oldpred @@ newvars))))

-- given a predicate P with parameters x, and parameters y, such that P(x,y) | x -> y
-- the delete action: C insert ~P(s,t) where C is a condition modifies P -> P'
-- P' is uniquely determined by the following formula:
-- let vc = fv C \\ fv t \\ fv s
--     vs = fv s
-- forall vs. (exists vc. C & P(s, t) & x == s & y == t) -> ~P'(x, y) &
--           ~(exists vc. C & P(s, t) & x == s & y == t) -> (P'(x, y) <-> P(x, y))
ruleQ' :: PureFormula -> Pred -> [Expr] -> ValEnv PureFormula
ruleQ' cond pred@(Pred p pt@(PredType _ paramtypes)) args = do
   lift $ registerVars (freeVars args)
   lift $ register [p]
   newvars <- lift $ new args
   let s = keyComponents pt args
       x = keyComponents pt newvars
       t = propComponents pt args
       y = propComponents pt newvars
       vc = freeVars cond `union` freeVars t `union` freeVars s
   oldpred <- getPredCurrent pred
   newpred <- newPred pred
   let newcond = foldr Exists (cond & oldpred @@ args & newvars === args) vc
   return (generalize (((newcond --> Not (newpred @@ newvars)) &
               (Not newcond --> (newpred @@ newvars <--> oldpred @@ newvars)))))

-- the update action: C insert L1 ... Ln is similar to TL's \otimes
rulePQ' :: PureFormula -> [Lit] -> ValEnv [PureFormula]
rulePQ' cond lits = mapM insertOneLit lits where
   insertOneLit (Lit Pos (Atom pred args)) =
       ruleP' cond pred args
   insertOneLit (Lit Neg (Atom pred args)) =
       ruleQ' cond pred args

-- inserts and deletes preserves invariants
ruleInsert :: [PureFormula] -> PureFormula -> [Lit] -> ValEnv ([PureFormula], PureFormula)
ruleInsert rules cond atoms = do
   formulas <- rulePQ' cond atoms
   map1 <- get
   let newrules =  substPred map1 (conj rules)
   return (rules ++ formulas, newrules)

-- insert preserves condition
ruleInsertCond :: [PureFormula] -> PureFormula -> [Lit] -> ValEnv ([PureFormula], PureFormula)
ruleInsertCond rules cond lits = do
   formulas <- rulePQ' cond lits
   map1 <- get
   let newcond =  substPred map1 cond
   return (rules ++ formulas, cond --> newcond)

validateInsert :: TheoremProver -> [PureFormula] -> [Lit] -> PureFormula -> IO (Maybe Bool)
validateInsert (TheoremProver a) rules insertlits cond = (
   do
           let execplan = elems (sortByKey insertlits)
           let steps = take (length execplan - 1) execplan
           result <- validateRules insertlits
           case result of
               Nothing -> return Nothing
               Just False -> return (Just False)
               _ -> return (Just True)) where
{-                validateCond [] =
               validateCond (s : ss) = do
                   let (axioms, conjecture) = runNew (evalStateT (ruleInsertCond rules cond s) empty)
                   putStrLn ("validating that " ++ show s ++ " doesn't change the condition")
                   mapM (\a -> putStrLn ("axiom: " ++ show a)) axioms
                   putStrLn ("conjecture: " ++ show conjecture)
                   result <- prove a axioms conjecture
                   case result of
                       Nothing -> return Nothing
                       Just False -> return (Just False)
                       _ -> validateCond ss
-}
               validateRules lits = do
                   let (axioms, conjecture) = runNew (evalStateT (ruleInsert rules cond lits) empty)
                   putStrLn ("validating that " ++ show lits ++ " doesn't change the invariant")
                   mapM (\a -> putStrLn ("axiom: " ++ show a)) axioms
                   putStrLn ("conjecture: " ++ show conjecture)
                   prove a axioms conjecture
