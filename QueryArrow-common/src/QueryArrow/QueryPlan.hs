{-# LANGUAGE MultiParamTypeClasses, TypeSynonymInstances, FlexibleInstances, FlexibleContexts, TypeFamilies, ScopedTypeVariables, TypeApplications, DataKinds, DeriveFunctor, PatternSynonyms, RankNTypes #-}
module QueryArrow.QueryPlan where

import QueryArrow.DB.ResultStream
import QueryArrow.Syntax.Term
import QueryArrow.Syntax.Type
import QueryArrow.Semantics.TypeChecker
import Algebra.SemiBoundedLattice
import QueryArrow.DB.DB
import QueryArrow.Data.Heterogeneous.List

import Prelude  hiding (lookup, null)
import Data.List (elem, sortBy, groupBy, nub)
import qualified Data.List as List
import Control.Monad.Except
import Control.Applicative ((<|>))
import Data.Maybe
import Data.Monoid  ((<>))
import Data.Tree
import Data.Conduit
import Control.Concurrent.Async.Lifted
import System.Log.Logger
import Algebra.Lattice
import Data.Set (Set, fromList, toAscList)
import Data.Ord (comparing, Down(..))
import Control.Comonad.Cofree
import QueryArrow.Syntax.Serialize
import Data.Conduit
import qualified Data.Conduit.Combinators as C
import Debug.Trace

type MSet a = Complemented (Set a)

data QueryPlanF a qp = ExecF a
                | QPParF qp qp
                | QPChoiceF qp qp
                | QPSequencingF qp qp
                | QPZeroF
                | QPOneF
                | QPAggregateF Aggregator qp deriving Functor

type QueryPlan2 a b = Cofree (QueryPlanF a) b

type QueryPlan = QueryPlan2 FormulaT VarTypeMap

pattern Exec vtm a = vtm :< (ExecF a)
pattern QPPar vtm a b = vtm :< (QPParF a b)
pattern QPChoice vtm a b = vtm :< (QPChoiceF a b)
pattern QPSequencing vtm a b = vtm :< (QPSequencingF a b)
pattern QPZero vtm = vtm :< QPZeroF
pattern QPOne vtm = vtm :< QPOneF
pattern QPAggregate vtm a b = vtm :< (QPAggregateF a b)

data QueryPlanData  = QueryPlanData {
    linscopevs :: MSet Var,
    rinscopevs :: MSet Var,
    freevs :: Set Var,
    determinevs :: Set Var, -- determined vars
    paramvs :: Set Var, -- parameter vars
    returnvs :: Set Var, -- return vars
    combinedvs :: Set Var, -- combined return and available vars that are still in scope
    availablevs :: Set Var,
    vartypemap :: VarTypeMap
}

makeQueryPlanData :: QueryPlanData
makeQueryPlanData = QueryPlanData top top bottom bottom bottom bottom bottom bottom mempty

type QueryPlanC = QueryPlan2 FormulaT QueryPlanData

data IndexedFormula = IndexedFormula FormulaT Int

type QueryPlanD = QueryPlan2 IndexedFormula QueryPlanData

pattern ExecA qpd a = qpd :< (ExecF a)
pattern QPParA qpd a b = qpd :< (QPParF a b)
pattern QPChoiceA qpd a b = qpd :< (QPChoiceF a b)
pattern QPSequencingA qpd a b = qpd :< (QPSequencingF a b)
pattern QPZeroA qpd = qpd :< QPZeroF
pattern QPOneA qpd = qpd :< QPOneF
pattern QPAggregateA qpd a b = qpd :< (QPAggregateF a b)

showSet :: Set Var -> String
showSet s = "[" ++ List.intercalate "," (map serialize (toAscList s)) ++ "]"

showSet2 :: Complemented (Set Var) -> String
showSet2 (Include s) = "Include[" ++ List.intercalate "," (map serialize (toAscList s)) ++ "]"
showSet2 (Exclude s) = "Exclude[" ++ List.intercalate "," (map serialize (toAscList s)) ++ "]"

getQPSequencing2 :: QueryPlanD -> [QueryPlanD]
getQPSequencing2 (QPSequencingA _ qp1 qp2) = getQPSequencing2 qp1 ++ getQPSequencing2 qp2
getQPSequencing2 qp = [qp]

getQPChoice2 :: QueryPlanD -> [QueryPlanD]
getQPChoice2 (QPChoiceA _ qp1 qp2) = getQPChoice2 qp1 ++ getQPChoice2 qp2
getQPChoice2 qp = [qp]

getQPPar2 :: QueryPlanD -> [QueryPlanD]
getQPPar2 (QPParA _ qp1 qp2) = getQPPar2 qp1 ++ getQPPar2 qp2
getQPPar2 qp = [qp]

class ToTree a where
    toTree :: a -> Tree String

instance Show QueryPlanData where
    show qp = "[available=" ++ showSet (availablevs qp) ++ "|linscope=" ++ showSet2 (linscopevs qp) ++ "|param=" ++ showSet (paramvs qp) ++ "|free="
              ++ showSet (freevs qp) ++ "|determine=" ++ showSet (determinevs qp) ++ "|return="++ showSet (returnvs qp) ++ "|combined=" ++ showSet (combinedvs qp)
              ++ "|rinscope=" ++ showSet2 (rinscopevs qp) ++ "]"

instance ToTree QueryPlanD where
    toTree (ExecA qpd (IndexedFormula f dbs)) = Node ("exec " ++ serialize f ++ " at " ++ show dbs ++ show qpd ) []
    toTree qp@(QPSequencingA qpd qp1 qp2) = let qps = getQPSequencing2 qp in Node ("sequencing"++ show qpd ) (map toTree qps)
    toTree qp@(QPChoiceA qpd qp1 qp2) = let qps = getQPChoice2 qp in Node ("choice"++ show qpd ) (map toTree qps)
    toTree qp@(QPParA qpd qp1 qp2) = let qps = getQPPar2 qp in Node ("parallel"++ show qpd ) (map toTree qps)
    toTree (QPZeroA qpd) = Node ("zero"++ show qpd ) []
    toTree (QPOneA qpd) = Node ("one"++ show qpd ) []
    toTree (QPAggregateA qpd agg qp1) = Node ("aggregate " ++ show agg ++ " " ++ show qpd) [toTree qp1]

class ToTree' a where
    toTree' :: a -> Tree String

instance ToTree' QueryPlanD where
    toTree' (ExecA qpd (IndexedFormula f inx)) = Node ("exec " ++ serialize f ++ " at " ++ show inx ++ show qpd ) []
    toTree' qp@(QPSequencingA qpd qp1 qp2) = Node ("sequencing"++ show qpd ) [toTree' qp1, toTree' qp2]
    toTree' qp@(QPChoiceA qpd qp1 qp2) = Node ("choice"++ show qpd ) [toTree' qp1, toTree' qp2]
    toTree' qp@(QPParA qpd qp1 qp2) = Node ("parallel"++ show qpd ) [toTree' qp1, toTree' qp2]
    toTree' (QPZeroA qpd) = Node ("zero"++ show qpd ) []
    toTree' (QPOneA qpd) = Node ("one"++ show qpd ) []
    toTree' (QPAggregateA qpd agg qp1) = Node ("aggregate " ++ show agg ++ " " ++ show qpd) [toTree' qp1]

type QueryPlanT l = QueryPlan2 (HVariant' DBQueryTypeIso l, String) QueryPlanData
    
type QueryPlanS row = QueryPlan2 (AbstractDBStatement row, String) QueryPlanData


findDB :: forall  (l :: [*]) . (HMapConstraint (IDatabaseUniformDBFormula FormulaT) l) => Set Var -> FormulaT -> Set Var -> HList l -> Int
findDB sing form env dbs =
  let pred0 = case form of
                  _ :< (FAtomicF (Atom pred0 _)) -> pred0
                  _ :< (FInsertF (Lit _ (Atom pred0 _))) -> pred0
                  _ -> error ("findDB: cannot find database for " ++ show form)
  in
    toIntegerV
      (fromMaybe (error ("no database for predicate " ++ show pred0 ++ " available " ++ (
          let preds = filter (\(Pred (PredName _ pn1) _) ->
                          case pred0 of
                            PredName _ pn0 -> pn1 == pn0) (concat (hMapCUL @(IDatabaseUniformDBFormula FormulaT) getPreds dbs))
          in show preds))) (hFindCULV @(IDatabaseUniformDBFormula FormulaT) (\db -> pred0 `elem` (map predName (getPreds db)) && supported db sing form env) dbs))

formulaToQueryPlan :: FormulaT -> QueryPlan
formulaToQueryPlan   form@(vtm :< (FAtomicF (Atom pred0  _))) =
    Exec vtm form -- redundent vtm
formulaToQueryPlan  (vtm :< FOneF) = QPOne vtm
formulaToQueryPlan  (vtm :< FZeroF) = QPZero vtm
formulaToQueryPlan   (vtm :< (FChoiceF form1 form2)) = QPChoice vtm (formulaToQueryPlan  form1) (formulaToQueryPlan  form2)
formulaToQueryPlan   (vtm :< (FParF form1 form2)) = QPPar vtm (formulaToQueryPlan  form1) (formulaToQueryPlan  form2)
formulaToQueryPlan   (vtm :< (FSequencingF form1 form2)) = QPSequencing vtm (formulaToQueryPlan  form1) (formulaToQueryPlan  form2)
formulaToQueryPlan   (vtm :< (AggregateF agg form)) = QPAggregate vtm agg (formulaToQueryPlan  form)
formulaToQueryPlan   ins@(vtm :< (FInsertF (Lit _ (Atom pred1 _)))) =
        Exec vtm ins -- redundent vtm







combineQPSequencingData :: QueryPlanData  -> QueryPlanData  -> QueryPlanData
combineQPSequencingData qp1 qp2 =
    QueryPlanData {
        availablevs = availablevs qp1,
        freevs = freevs qp1 \/ freevs qp2,
        determinevs = determinevs qp1 \/ determinevs qp2,
        paramvs = paramvs qp1 \/ (paramvs qp2 \\\ returnvs qp1),
        returnvs = (returnvs qp1 \/ returnvs qp2) /\\ rinscopevs qp2,
        rinscopevs = rinscopevs qp2,
        linscopevs = linscopevs qp1,
        combinedvs = (combinedvs qp1 \/ combinedvs qp2) /\\ rinscopevs qp2,
        vartypemap = vartypemap qp1 <> vartypemap qp2
    }

combineQPChoiceData :: QueryPlanData -> QueryPlanData -> QueryPlanData
combineQPChoiceData qp1 qp2 =
    QueryPlanData {
        availablevs = availablevs qp1,
        freevs = freevs qp1 \/ freevs qp2,
        determinevs = determinevs qp1 /\ determinevs qp2,
        paramvs = paramvs qp1 \/ paramvs qp2,
        returnvs = returnvs qp1 \/ returnvs qp2,
        rinscopevs = rinscopevs qp1 \/ linscopevs qp2,
        linscopevs = linscopevs qp1 \/ linscopevs qp2,
        combinedvs = combinedvs qp2 \/ combinedvs qp2,
        vartypemap = vartypemap qp1 <> vartypemap qp2
    }

combineQPParData :: QueryPlanData -> QueryPlanData -> QueryPlanData
combineQPParData qp1 qp2 =
    QueryPlanData {
        availablevs = availablevs qp1,
        freevs = freevs qp1 \/ freevs qp2,
        determinevs = determinevs qp1 /\ determinevs qp2,
        paramvs = paramvs qp1 \/ paramvs qp2,
        returnvs = returnvs qp1 \/ returnvs qp2,
        rinscopevs = rinscopevs qp1 \/ linscopevs qp2,
        linscopevs = linscopevs qp1 \/ linscopevs qp2,
        combinedvs = combinedvs qp2 \/ combinedvs qp2,
        vartypemap = vartypemap qp1 <> vartypemap qp2
    }

calculateVars :: Set Var -> MSet Var -> QueryPlan -> QueryPlanC
calculateVars lvars rvars qp = calculateVars2 lvars (calculateVars1 rvars qp)

calculateVars1 :: MSet Var -> QueryPlan -> QueryPlanC
calculateVars1 rvars (Exec vtm form) =
    let fvs = freeVars form in
        ExecA makeQueryPlanData{freevs =  fvs, determinevs =  fvs, linscopevs = Include fvs \/ rvars, rinscopevs = rvars, vartypemap = vtm} form

calculateVars1  rvars (QPSequencing vtm qp1 qp2) =
    let qp2'@( qpd2 :< _) = calculateVars1 rvars qp2
        qp1'@( qpd1 :< _) = calculateVars1 (linscopevs qpd2) qp1 in
        QPSequencingA   makeQueryPlanData{
            freevs = freevs qpd1 \/ freevs qpd2,
            determinevs = determinevs qpd1 \/ determinevs qpd2,
            linscopevs = linscopevs qpd1,
            rinscopevs = rinscopevs qpd2, vartypemap = vtm
            } qp1' qp2'

calculateVars1 rvars  (QPChoice vtm qp1 qp2) =
    let qp1'@(qpd1 :< _) = calculateVars1 rvars qp1
        qp2'@(qpd2 :< _) = calculateVars1 rvars qp2 in
        QPChoiceA  makeQueryPlanData{
            freevs = freevs qpd1 \/ freevs qpd2,
            determinevs = determinevs qpd1 /\ determinevs qpd2,
            linscopevs = linscopevs qpd1 \/ linscopevs qpd2,
            rinscopevs = rinscopevs qpd1 \/ rinscopevs qpd2, vartypemap = vtm
            } qp1' qp2'
calculateVars1 rvars  (QPPar vtm qp1 qp2) =
    let qp1'@(qpd1 :< _) = calculateVars1 rvars qp1
        qp2'@(qpd2 :< _) = calculateVars1 rvars qp2 in
        QPParA makeQueryPlanData{
            freevs = freevs qpd1 \/ freevs qpd2,
            determinevs = determinevs qpd1 /\ determinevs qpd2,
            linscopevs = linscopevs qpd1 \/ linscopevs qpd2,
            rinscopevs = rinscopevs qpd1 \/ rinscopevs qpd2, vartypemap = vtm
            } qp1' qp2'
calculateVars1 rvars  (QPOne vtm) = QPOneA makeQueryPlanData{freevs = bottom, determinevs = bottom, linscopevs = rvars, rinscopevs = rvars, vartypemap = vtm}
calculateVars1 rvars  (QPZero vtm) = QPZeroA makeQueryPlanData{freevs = bottom, determinevs = bottom, linscopevs = rvars, rinscopevs = rvars, vartypemap = vtm}
calculateVars1 rvars  (QPAggregate vtm agg@Not qp1) =
    let qp1'@(qpd1 :< _) = calculateVars1 rvars qp1 in
        QPAggregateA makeQueryPlanData{freevs = freevs qpd1, determinevs = bottom, linscopevs = rvars, rinscopevs = rvars, vartypemap = vtm}  agg qp1'
calculateVars1 rvars  (QPAggregate vtm agg@Exists qp1) =
    let qp1'@(qpd1 :< _) = calculateVars1 rvars qp1 in
        QPAggregateA  makeQueryPlanData{freevs = freevs qpd1, determinevs = bottom, linscopevs = rvars, rinscopevs = rvars, vartypemap = vtm}  agg qp1'
calculateVars1 rvars  (QPAggregate vtm agg@(Summarize funcs groupby) qp1) =
    let qp1'@(qpd1 :< _) = calculateVars1 rvars qp1 in
        QPAggregateA  makeQueryPlanData{freevs = freevs qpd1, determinevs = fromList (map (\(Bind a _) -> a) funcs), linscopevs = linscopevs qpd1, rinscopevs = rvars \/ Include (fromList (map (\(Bind a _) -> a) funcs)), vartypemap = vtm}  agg qp1'
calculateVars1 rvars  (QPAggregate vtm agg@(Limit _) qp1) =
    let qp1'@(qpd1 :< _) = calculateVars1 rvars qp1 in
        QPAggregateA makeQueryPlanData{freevs = freevs qpd1, determinevs = determinevs qpd1, linscopevs = linscopevs qpd1, rinscopevs = rinscopevs qpd1, vartypemap = vtm}  agg qp1'
calculateVars1 rvars  (QPAggregate vtm agg@Distinct qp1) =
    let qp1'@(qpd1 :< _) = calculateVars1 rvars qp1 in
        QPAggregateA makeQueryPlanData{freevs = freevs qpd1, determinevs = determinevs qpd1, linscopevs = linscopevs qpd1, rinscopevs = rinscopevs qpd1, vartypemap = vtm}   agg qp1'
calculateVars1 rvars  (QPAggregate vtm agg@(OrderByAsc _) qp1) =
    let qp1'@(qpd1 :< _) = calculateVars1 rvars qp1 in
        QPAggregateA makeQueryPlanData{freevs = freevs qpd1, determinevs = determinevs qpd1, linscopevs = linscopevs qpd1, rinscopevs = rinscopevs qpd1, vartypemap = vtm}   agg qp1'
calculateVars1 rvars  (QPAggregate vtm agg@(OrderByDesc _) qp1) =
    let qp1'@(qpd1 :< _) = calculateVars1 rvars qp1 in
        QPAggregateA makeQueryPlanData{freevs = freevs qpd1, determinevs = determinevs qpd1, linscopevs = linscopevs qpd1, rinscopevs = rinscopevs qpd1, vartypemap = vtm}   agg qp1'
calculateVars1 rvars  (QPAggregate vtm agg@(FReturn vars) qp1) =
    let qp1'@(qpd1 :< _) = calculateVars1 rvars qp1 in
    QPAggregateA makeQueryPlanData{
        freevs = freevs qpd1,
        determinevs = fromList vars,
        linscopevs = linscopevs qpd1,
        rinscopevs = rvars, vartypemap = vtm
        } agg qp1'



calculateVars2 :: Set Var -> QueryPlanC -> QueryPlanC
calculateVars2  lvars (ExecA qpd form) =
            ExecA qpd{availablevs = lvars, paramvs = lvars /\ (freevs qpd), returnvs = (rinscopevs qpd //\ determinevs qpd \\\ lvars), combinedvs = rinscopevs qpd //\ (determinevs qpd \/ lvars) }  form
calculateVars2  lvars (QPChoiceA qpd qp1 qp2) =
            let qp1' = calculateVars2 lvars qp1
                qp2' = calculateVars2 lvars qp2 in
                QPChoiceA  qpd{availablevs = lvars, paramvs = lvars /\ (freevs qpd), returnvs = (rinscopevs qpd //\ determinevs qpd \\\ lvars), combinedvs = rinscopevs qpd //\ (determinevs qpd \/ lvars)}   qp1' qp2'
calculateVars2  lvars (QPParA qpd qp1 qp2) =
            let qp1' = calculateVars2 lvars qp1
                qp2' = calculateVars2 lvars qp2 in
                QPParA  qpd{availablevs = lvars, paramvs = lvars /\ (freevs qpd), returnvs = (rinscopevs qpd //\ determinevs qpd \\\ lvars), combinedvs = rinscopevs qpd //\ (determinevs qpd \/ lvars)}   qp1' qp2'
calculateVars2  lvars (QPSequencingA qpd qp1 qp2) =
            let qp1'@(qpd1 :< _) = calculateVars2 lvars qp1
                qp2' = calculateVars2 (combinedvs qpd1) qp2 in
                QPSequencingA  qpd{availablevs = lvars, paramvs = lvars /\ (freevs qpd), returnvs = (rinscopevs qpd //\ determinevs qpd \\\ lvars), combinedvs = rinscopevs qpd //\ (determinevs qpd \/ lvars)}   qp1' qp2'

calculateVars2 lvars  (QPOneA qpd) = QPOneA qpd{availablevs = lvars, paramvs = bottom, returnvs = bottom, combinedvs = rinscopevs qpd //\ lvars}
calculateVars2 lvars  (QPZeroA qpd) = QPZeroA qpd{availablevs = lvars, paramvs = bottom, returnvs = bottom,combinedvs = rinscopevs qpd //\ lvars}

calculateVars2 lvars  (QPAggregateA qpd agg@Not qp1) =
            let qp1' = calculateVars2 lvars qp1 in
                QPAggregateA qpd{availablevs = lvars, paramvs = lvars /\ (freevs qpd), returnvs = (rinscopevs qpd //\ determinevs qpd) \\\ lvars,combinedvs = rinscopevs qpd //\ (determinevs qpd \/ lvars)} agg qp1'
calculateVars2 lvars  (QPAggregateA qpd agg@Exists qp1) =
            let qp1' = calculateVars2 lvars qp1 in
                QPAggregateA qpd{availablevs = lvars, paramvs = lvars /\ (freevs qpd), returnvs = (rinscopevs qpd //\ determinevs qpd) \\\ lvars,combinedvs = rinscopevs qpd //\ (determinevs qpd \/ lvars)} agg qp1'
calculateVars2 lvars  (QPAggregateA qpd agg@(Summarize funcs groupby) qp1) =
            let qp1' = calculateVars2 lvars qp1 in
                QPAggregateA qpd{availablevs = lvars, paramvs = lvars /\ (freevs qpd), returnvs = (rinscopevs qpd //\ determinevs qpd) \\\ lvars,combinedvs = rinscopevs qpd //\ (determinevs qpd \/ lvars)} agg qp1'
calculateVars2 lvars  (QPAggregateA qpd agg@(Limit _) qp1) =
            let qp1' = calculateVars2 lvars qp1 in
                QPAggregateA qpd{availablevs = lvars, paramvs = lvars /\ (freevs qpd), returnvs = (rinscopevs qpd //\ determinevs qpd) \\\ lvars,combinedvs = rinscopevs qpd //\ (determinevs qpd \/ lvars)} agg qp1'
calculateVars2 lvars  (QPAggregateA qpd agg@Distinct qp1) =
            let qp1' = calculateVars2 lvars qp1 in
                QPAggregateA qpd{availablevs = lvars, paramvs = lvars /\ (freevs qpd), returnvs = (rinscopevs qpd //\ determinevs qpd) \\\ lvars,combinedvs = rinscopevs qpd //\ (determinevs qpd \/ lvars)} agg qp1'
calculateVars2 lvars  (QPAggregateA qpd agg@(OrderByAsc _) qp1) =
            let qp1' = calculateVars2 lvars qp1 in
                QPAggregateA qpd{availablevs = lvars, paramvs = lvars /\ (freevs qpd), returnvs = (rinscopevs qpd //\ determinevs qpd) \\\ lvars,combinedvs = rinscopevs qpd //\ (determinevs qpd \/ lvars)} agg qp1'
calculateVars2 lvars  (QPAggregateA qpd agg@(OrderByDesc _) qp1) =
            let qp1' = calculateVars2 lvars qp1 in
                QPAggregateA qpd{availablevs = lvars, paramvs = lvars /\ (freevs qpd), returnvs = (rinscopevs qpd //\ determinevs qpd) \\\ lvars,combinedvs = rinscopevs qpd //\ (determinevs qpd \/ lvars)} agg qp1'
calculateVars2 lvars  (QPAggregateA qpd agg@(FReturn vars) qp1) =
            let qp1' = calculateVars2 lvars qp1 in
                QPAggregateA qpd{availablevs = lvars, paramvs = lvars /\ (freevs qpd), returnvs = (rinscopevs qpd //\ determinevs qpd) \\\ lvars,combinedvs = rinscopevs qpd //\ (determinevs qpd \/ lvars)} agg qp1'


    

supportedDB :: IDatabaseUniformDBFormula FormulaT db => Set Var -> FormulaT -> Set Var -> db -> Bool
supportedDB sing form vars db = supported db sing form vars

findDBQueryPlan :: HMapConstraint (IDatabaseUniformDBFormula FormulaT) l => HList l -> QueryPlanC -> QueryPlanD
findDBQueryPlan dbsx qp@(ExecA qpd form) =
  let dbx = findDB (returnvs qpd) form (paramvs qpd) dbsx in
      (ExecA qpd (IndexedFormula form dbx))
findDBQueryPlan dbsx (QPSequencingA qpd ip1  ip2) =
    let qp1' = findDBQueryPlan dbsx ip1
        qp2' = findDBQueryPlan dbsx ip2 in
        -- trace ("findDBQueryPlan: \n" ++ drawTree (toTree ip1) ++ "\n------------>\n" ++ drawTree (toTree qp1') ++ "\n and \n" ++
          -- drawTree (toTree ip2) ++ "\n------------------>\n" ++ drawTree (toTree qp2')) $
        (QPSequencingA qpd qp1' qp2')
findDBQueryPlan dbsx (QPChoiceA qpd qp1 qp2) =
    let qp1' = findDBQueryPlan dbsx qp1
        qp2' = findDBQueryPlan dbsx qp2 in
        (QPChoiceA qpd qp1' qp2')
findDBQueryPlan dbsx (QPParA qpd qp1 qp2) =
    let qp1' = findDBQueryPlan dbsx qp1
        qp2' = findDBQueryPlan dbsx qp2 in
        (QPParA qpd qp1' qp2')
findDBQueryPlan _ (QPOneA qpd) = QPOneA qpd
findDBQueryPlan _ (QPZeroA qpd) = QPZeroA qpd
findDBQueryPlan dbsx (QPAggregateA qpd agg qp1) =
    let qp1' = findDBQueryPlan dbsx qp1 in
        (QPAggregateA qpd agg qp1')


optimizeQueryPlan :: HMapConstraint (IDatabaseUniformDBFormula FormulaT) l => HList l -> QueryPlanD -> QueryPlanD
optimizeQueryPlan _ qp@(ExecA qpd _) = qp

optimizeQueryPlan dbsx (QPSequencingA qpd ip1  ip2) =
    let qp1' = optimizeQueryPlan dbsx ip1
        qp2' = optimizeQueryPlan dbsx ip2 in
        -- trace ("optimizeQueryPlan: \n" ++ drawTree (toTree ip1) ++ "\n------------>\n" ++ drawTree (toTree qp1') ++ "\n and \n" ++
          -- drawTree (toTree ip2) ++ "\n------------------>\n" ++ drawTree (toTree qp2')) $
        case (qp1', qp2') of
            (ExecA _ (IndexedFormula form1 x1), ExecA _ (IndexedFormula form2 x2)) ->
                let dbs = x1 == x2
                    fse = fsequencing1 [form1, form2]
                    dbs' = fromMaybe (error "index out of range") (hApplyCUL @(IDatabaseUniformDBFormula FormulaT) @Bool (supportedDB (returnvs qpd) fse (paramvs qpd)) x1 dbsx) in
                    if dbs && dbs'
                            then ExecA qpd (IndexedFormula fse x1)
                            else QPSequencingA qpd qp1' qp2'
            (ExecA qpd1 _, QPSequencingA _ qp21@(qpd3 :< _) qp22) ->
                QPSequencingA qpd (optimizeQueryPlan dbsx (QPSequencingA (combineQPSequencingData qpd1 qpd3) qp1' qp21)) qp22
            (QPSequencingA _ qp11 qp12@(qpd3 :< _), ExecA qpd2 _) ->
                QPSequencingA qpd qp11 (optimizeQueryPlan dbsx (QPSequencingA (combineQPSequencingData qpd3 qpd2) qp12 qp2'))
            (QPSequencingA qpd1 qp11 qp12@(qpd3 :< _), QPSequencingA _ qp21@(qpd4 :< _) qp22) ->
                QPSequencingA qpd (QPSequencingA (combineQPSequencingData qpd1 qpd4) qp11 (optimizeQueryPlan dbsx (QPSequencingA (combineQPSequencingData qpd3 qpd4) qp12 qp21))) qp22
            _ -> QPSequencingA qpd qp1' qp2'
optimizeQueryPlan dbsx (QPChoiceA qpd qp1 qp2) =
    let qp1' = optimizeQueryPlan dbsx qp1
        qp2' = optimizeQueryPlan dbsx qp2 in
        case (qp1', qp2') of
            (ExecA _ (IndexedFormula formula1 x1), ExecA _ (IndexedFormula formula2 x2)) ->
                let dbs = x1 == x2
                    fch = fchoice1 [formula1, formula2]
                    dbs' = fromMaybe (error "index out of range") (hApplyCUL @(IDatabaseUniformDBFormula FormulaT) @Bool (supportedDB (returnvs qpd) (fch) (paramvs qpd)) x1 dbsx) in
                    if dbs && dbs' then
                      ExecA qpd (IndexedFormula fch x1)
                    else
                      QPChoiceA qpd qp1' qp2'
            (ExecA qpd1 _, QPChoiceA _ qp21@(qpd21 :< _) qp22) ->
                QPChoiceA qpd (optimizeQueryPlan dbsx (QPChoiceA (combineQPChoiceData qpd1 qpd21) qp1' qp21)) qp22
            (QPChoiceA _ qp11 qp12@(qpd3 :< _), ExecA qpd2 _) ->
                QPChoiceA qpd qp11 (optimizeQueryPlan dbsx (QPChoiceA (combineQPChoiceData qpd3 qpd2) qp12 qp2'))
            (QPChoiceA qpd1 qp11 qp12@(qpd3 :< _), QPChoiceA _ qp21@(qpd4 :< _) qp22) ->
                QPChoiceA qpd (QPChoiceA (combineQPChoiceData qpd1 qpd4) qp11 (optimizeQueryPlan dbsx (QPChoiceA (combineQPChoiceData qpd3 qpd4) qp12 qp21))) qp22
            _ -> QPChoiceA qpd qp1' qp2'
optimizeQueryPlan dbsx (QPParA qpd qp1 qp2) =
    let qp1' = optimizeQueryPlan dbsx qp1
        qp2' = optimizeQueryPlan dbsx qp2 in
        case (qp1', qp2') of
            (ExecA _ (IndexedFormula formula1 x1), ExecA _ (IndexedFormula formula2 x2)) ->
                let dbs = x1 == x2
                    fch = fpar1 [formula1, formula2]
                    dbs' = fromMaybe (error "index out of range") (hApplyCUL @(IDatabaseUniformDBFormula FormulaT) @Bool (supportedDB (returnvs qpd) (fch) (paramvs qpd)) x1 dbsx) in
                    if dbs && dbs' then
                      ExecA qpd (IndexedFormula fch x1)
                    else
                      QPParA qpd qp1' qp2'
            (ExecA qpd1 _, QPParA _ qp21@(qpd21 :< _) qp22) ->
                QPParA qpd (optimizeQueryPlan dbsx (QPParA (combineQPParData qpd1 qpd21) qp1' qp21)) qp22
            (QPParA _ qp11 qp12@(qpd3 :< _), ExecA qpd2 _) ->
                QPParA qpd qp11 (optimizeQueryPlan dbsx (QPParA (combineQPParData qpd3 qpd2) qp12 qp2'))
            (QPParA qpd1 qp11 qp12@(qpd3 :< _), QPParA _ qp21@(qpd4 :< _) qp22) ->
                QPParA qpd (QPParA (combineQPParData qpd1 qpd4) qp11 (optimizeQueryPlan dbsx (QPParA (combineQPParData qpd3 qpd4) qp12 qp21))) qp22
            _ -> QPParA qpd qp1' qp2'
optimizeQueryPlan _ qp@(QPOneA _) = qp
optimizeQueryPlan _ qp@(QPZeroA _) = qp
optimizeQueryPlan dbsx (QPAggregateA qpd agg qp1) =
    let qp1' = optimizeQueryPlan dbsx qp1 in
        case qp1' of
            ExecA _ (IndexedFormula formula1 x) ->
                        let vtm = vartypemap qpd
                            exi = vtm :< (AggregateF agg formula1)
                            dbs' = fromMaybe (error "index out of range") (hApplyCUL @(IDatabaseUniformDBFormula FormulaT) @Bool (supportedDB (returnvs qpd) exi (paramvs qpd)) x dbsx)  in
                            if dbs' then
                              ExecA qpd (IndexedFormula exi x)
                            else
                              QPAggregateA qpd agg qp1'
            _ -> QPAggregateA qpd agg qp1'


{- addTransaction' :: QueryPlan2 row -> QueryPlan2 row
addTransaction' = snd . addTransaction

addTransaction :: QueryPlan2 row -> (Bool, QueryPlan2 row )
addTransaction qp@(qpd, Exec2 form _) | pureF form = (False, qp)
                                    | otherwise = (True, (qpd, QPSequencing2 (QueryPlanData{
                                        availablevs = availablevs qpd,
                                        paramvs = bottom,
                                        returnvs = bottom,
                                        combinedvs = combinedvs qpd,
                                        linscopevs = linscopevs qpd,
                                        rinscopevs = rinscopevs qpd,
                                        determinevs = bottom,
                                        freevs = bottom,
                                        stmts = Nothing,
                                        tdb = Nothing
                                    }, QPTransaction2) qp))
addTransaction (qpd, QPSequencing2 qp1 qp2) =
    case addTransaction qp1 of
        (False, qp1') ->
            let (b, qp2') = addTransaction qp2 in
                (b, (qpd, QPSequencing2 qp1' qp2'))
        (True, qp1') ->
            (True, (qpd, QPSequencing2 qp1' qp2))
addTransaction (qpd, (QPChoice2 qp1 qp2)) =
    let (b1, qp1') = addTransaction qp1
        (b2, qp2') = addTransaction qp2 in
        (b1 && b2, (qpd, QPChoice2 qp1' qp2'))
addTransaction (qpd, (QPPar2 qp1 qp2)) =
    let (b1, qp1') = addTransaction qp1
        (b2, qp2') = addTransaction qp2 in
        (b1 && b2, (qpd, QPPar2 qp1' qp2'))
addTransaction qp@(_, QPTransaction2) = (True, qp)
addTransaction qp@(_, QPZero2) = (False, qp)
addTransaction qp@(_, QPOne2) = (False, qp)
addTransaction qp@(_, QPReturn2 _) = (False, qp)
-}

-- prepareTransaction :: [Int] -> QueryPlan2 -> ([Int], QueryPlan2)
-- prepareTransaction rdbxs qp@(_, (Exec2 _ x)) =
--     (x : rdbxs, qp)
-- prepareTransaction rdbxs (qpd, QPChoice2 qp1 qp2) =
--     let (dbxs1, qp1') = prepareTransaction rdbxs qp1
--         (dbxs2, qp2') = prepareTransaction rdbxs qp2 in
--         (dbxs1 `union` dbxs2, (qpd, QPChoice2 qp1' qp2'))
-- prepareTransaction rdbxs (qpd, QPPar2 qp1 qp2) =
--     let (dbxs1, qp1') = prepareTransaction rdbxs qp1
--         (dbxs2, qp2') = prepareTransaction rdbxs qp2 in
--         (dbxs1 `union` dbxs2, (qpd, QPPar2 qp1' qp2'))
-- prepareTransaction rdbxs (qpd, QPSequencing2 qp1 qp2) =
--     let (dbxs2, qp2') = prepareTransaction rdbxs qp2
--         (dbxs1, qp1') = prepareTransaction dbxs2 qp1 in
--         (dbxs1, (qpd, QPSequencing2 qp1' qp2'))
-- prepareTransaction rdbxs qp@(_, QPOne2) =  (rdbxs, qp)
-- prepareTransaction rdbxs qp@(_, QPZero2) =  (rdbxs, qp)
-- prepareTransaction rdbxs (qpd, QPAggregate2 agg qp1) =
--     let (dbxs, qp1') = prepareTransaction rdbxs qp1 in
--         (dbxs, (qpd, QPAggregate2 agg qp1'))


translateQueryPlan :: (HMapConstraint (IDatabaseUniformDBFormula FormulaT) l) => HList l -> QueryPlanD -> IO (QueryPlanT l)

translateQueryPlan dbs (ExecA qpd (IndexedFormula form x)) = do
        let vars = paramvs qpd
            vars2 = returnvs qpd
            trans :: (IDatabaseUniformDBFormula FormulaT db) => db ->  IO (DBQueryTypeIso db)
            trans db =  -- trace ("translateQueryPlan: " ++ show form) $ 
                DBQueryTypeIso <$> (translateQuery db vars2 form vars)
        qu <-  fromMaybe (error "index out of range") <$> (hApplyACULV @(IDatabaseUniformDBFormula FormulaT) @(DBQueryTypeIso) trans x dbs)
        return (ExecA qpd (qu, (serialize form)))
translateQueryPlan dbs  (QPChoiceA qpd qp1 qp2) = do
        qp1' <- translateQueryPlan dbs  qp1
        qp2' <- translateQueryPlan dbs  qp2
        return (QPChoiceA qpd qp1' qp2')
translateQueryPlan dbs  (QPParA qpd qp1 qp2) = do
        qp1' <- translateQueryPlan dbs  qp1
        qp2' <- translateQueryPlan dbs  qp2
        return (QPParA qpd qp1' qp2')
translateQueryPlan dbs  (QPSequencingA qpd qp1 qp2) = do
        qp1' <- translateQueryPlan dbs  qp1
        qp2' <- translateQueryPlan dbs  qp2
        return (QPSequencingA qpd qp1' qp2')
translateQueryPlan _ (QPOneA qpd) = return (QPOneA qpd)
translateQueryPlan _ (QPZeroA qpd) = return (QPZeroA qpd)
translateQueryPlan dbs  (QPAggregateA qpd agg qp1) = do
        qp1' <- translateQueryPlan dbs  qp1
        return (QPAggregateA qpd agg qp1')

hDbOpen :: (HMapConstraint IDatabase l) => HList l -> IO (HList' ConnectionType l)
hDbOpen dbs =
    let dbo :: (IDatabase db) => db -> IO (ConnectionType db)
        dbo db = dbOpen db in
        hMapACULL @IDatabase @ConnectionType dbo dbs

class (IDatabase conn, RowType (StatementType (ConnectionType conn)) ~ row) => IDatabaseUniformRow row conn
instance (IDatabase conn, RowType (StatementType (ConnectionType conn)) ~ row) => IDatabaseUniformRow row conn

class (IDatabase conn, DBFormulaType conn ~ form) => IDatabaseUniformDBFormula form conn
instance (IDatabase conn, DBFormulaType conn ~ form) => IDatabaseUniformDBFormula form conn

class (IDatabaseUniformRow row db, IDatabaseUniformDBFormula formula db) => IDatabaseUniformRowAndDBFormula row formula db
instance (IDatabaseUniformRow row db, IDatabaseUniformDBFormula formula db) => IDatabaseUniformRowAndDBFormula row formula db

prepareQueryPlan :: forall (row :: *) (l :: [*]). (IResultRow row, HMapConstraint (IDatabaseUniformRow row) l,
                HMapConstraint IDBConnection (HMap ConnectionType l)) => HList' ConnectionType l -> QueryPlanT l -> IO (QueryPlanS row )
prepareQueryPlan conns (ExecA qpd (qu, stmtshow)) = do
            let pq :: (IDatabaseUniformRow row conn) => ConnectionType conn -> DBQueryTypeIso conn -> IO (AbstractDBStatement row)
                pq conn (DBQueryTypeIso qu') = AbstractDBStatement <$> prepareQuery conn qu'
            stmt <- hApply2CULV' @(IDatabaseUniformRow row) @ConnectionType @DBQueryTypeIso pq conns qu
            return (ExecA qpd (stmt, stmtshow))
prepareQueryPlan dbs  (QPChoiceA qpd qp1 qp2) = do
    qp1' <- prepareQueryPlan dbs  qp1
    qp2' <- prepareQueryPlan dbs  qp2
    return (QPChoiceA qpd qp1' qp2')
prepareQueryPlan dbs  (QPParA qpd qp1 qp2) = do
    qp1' <- prepareQueryPlan dbs  qp1
    qp2' <- prepareQueryPlan dbs  qp2
    return (QPParA qpd qp1' qp2')
prepareQueryPlan dbs  (QPSequencingA qpd qp1 qp2) = do
    qp1' <- prepareQueryPlan dbs  qp1
    qp2' <- prepareQueryPlan dbs  qp2
    return (QPSequencingA qpd qp1' qp2')
prepareQueryPlan _ (QPOneA qpd) = return (QPOneA qpd)
prepareQueryPlan _ (QPZeroA qpd) = return (QPZeroA qpd)
prepareQueryPlan dbs  (QPAggregateA qpd agg qp1) = do
    qp1' <- prepareQueryPlan dbs  qp1
    return (QPAggregateA qpd agg qp1')

execQueryPlan :: (IResultRow row) =>  DBResultStream row -> QueryPlanS row -> DBResultStream row
execQueryPlan rs qp = rs .| execQueryPlan2 qp

execQueryPlan2 :: (IResultRow row) =>  QueryPlanS row -> DBResultStreamTrans row
execQueryPlan2 (ExecA qpd (stmt, stmtshow)) =
    case stmt of
        AbstractDBStatement stmt0 -> awaitForever (\row -> do
                                    liftIO $ infoM "QA" ("current row " ++ show row)
                                    liftIO $ infoM "QA" ("execute " ++ stmtshow)
                                    -- liftIO $ putStrLn ("execute " ++ stmtshow)
                                    C.map (const ()) .| dbStmtExec stmt0 (yield row) .| C.mapM (\row2 -> do
                                        liftIO $ infoM "QA" ("returns row")
                                        return (row <> row2)) .| C.map (proj (combinedvs qpd))) 
                                    

execQueryPlan2 (QPSequencingA _ qp1 qp2) =
    execQueryPlan2 qp1 .| execQueryPlan2 qp2

execQueryPlan2 (QPChoiceA _ qp1 qp2) = awaitForever (\row -> do
                yield row .| execQueryPlan2 qp1
                yield row .| execQueryPlan2 qp2)

execQueryPlan2 (QPParA _ qp1 qp2) = 
    execQueryPlan2 qp1 .| execQueryPlan2 qp2
{-    do
        mrow <- await
        case mrow of
            Just row -> do
                let rs1 = yield row .| execQueryPlan2 qp1
                    rs2 = yield row .| execQueryPlan2 qp2
                (rs1', rs2') <- lift $ concurrently (getAllResultsInStream rs1) (getAllResultsInStream rs2)
                C.yieldMany rs1' 
                C.yieldMany rs2'
            Nothing ->
                return () -}

execQueryPlan2 (QPOneA _) = awaitForever yield
execQueryPlan2 (QPZeroA _) = C.sinkNull
execQueryPlan2 (QPAggregateA qpd (FReturn vars) qp) =
    execQueryPlan2 qp .| C.map (proj (combinedvs qpd))
execQueryPlan2 (QPAggregateA qpd Not qp) =
    C.filterM (\row -> do
            let rs2 = yield row .| execQueryPlan2 qp
            isResultStreamEmpty rs2) .| C.map (proj (combinedvs qpd))
execQueryPlan2 (QPAggregateA qpd Exists qp) =
    C.filterM (\row -> do
        let rs2 = yield row .| execQueryPlan2 qp
        emp <- isResultStreamEmpty rs2
        return (not emp)) .| C.map (proj (combinedvs qpd))
execQueryPlan2 (QPAggregateA qpd (Summarize funcs groupby) qp) = awaitForever (\row -> do
                    let rs2 = yield row .| execQueryPlan2 qp
                    rows <- lift $ getAllResultsInStream rs2
                    let groups = groupBy (\a b -> all (\var -> get var a == get var b) groupby) rows
                    let rows2 = map (\rows -> mconcat (reverse (map (\(Bind v1 func1) ->
                                let m = case func1 of
                                            Max v2 ->
                                                if List.null rows
                                                    then error "max of empty list"
                                                    else maximum (map (get v2) rows)
                                            Min v2 ->
                                                if List.null rows
                                                    then error "min of empty list"
                                                    else minimum (map (get v2) rows)
                                            Sum v2 ->
                                                sum (map (get v2) rows)
                                            Average v2 ->
                                                if List.null rows
                                                    then error "min of empty list"
                                                    else average (map (get v2) rows)
                                            Count ->
                                                fromIntegral (length rows)
                                            CountDistinct v2 ->
                                                fromIntegral (length (List.nub (map (get v2) rows)))
                                            Random v2 ->
                                                if List.null rows
                                                    then error "random of empty list"
                                                    else head (map (get v2) rows) in
                                    sing v1 m) funcs))) groups where
                                        average :: Fractional a => [a] -> a
                                        average n = sum n / fromIntegral (length n)

                    C.yieldMany (map (<> row) rows2)) .| C.map (proj (combinedvs qpd))
execQueryPlan2 (QPAggregateA qpd (Limit n) qp) = awaitForever (\row -> do
                liftIO $ infoM "QueryPlan" ("limit " ++ show n ++ " input = " ++ show row)
                yield row .| execQueryPlan2 qp .| C.take n) .| C.map (proj (combinedvs qpd))
execQueryPlan2 (QPAggregateA qpd Distinct qp) = awaitForever (\row -> do
                liftIO $ infoM "QueryPlan" ("distinct input = " ++ show row)
                let rs2 = yield row .| execQueryPlan2 qp
                l <- lift $ getAllResultsInStream rs2
                C.yieldMany (nub l)) .| C.map (proj (combinedvs qpd))
execQueryPlan2 (QPAggregateA qpd (OrderByAsc v1) qp) = awaitForever (\row -> do
                let rs2 = yield row .| execQueryPlan2 qp
                rows <- lift $ getAllResultsInStream rs2
                let rows' = sortBy (\row1 row2 -> compare (get v1 row1) (get v1 row2)) rows
                C.yieldMany rows') .| C.map (proj (combinedvs qpd))
execQueryPlan2 (QPAggregateA qpd (OrderByDesc v1) qp) = awaitForever (\row -> do
                let rs2 = yield row .| execQueryPlan2 qp
                rows <- lift $ getAllResultsInStream rs2
                let rows' = sortBy (\row1 row2 -> comparing Down (get v1 row1) (get v1 row2)) rows
                C.yieldMany rows') .| C.map (proj (combinedvs qpd))

closeQueryPlan :: QueryPlanS row -> IO ()
closeQueryPlan (ExecA _ (stmt, _)) =
    case stmt of
        AbstractDBStatement stmt0 -> dbStmtClose stmt0

closeQueryPlan (QPSequencingA _  qp1 qp2) = do
  closeQueryPlan qp1
  closeQueryPlan qp2

closeQueryPlan (QPChoiceA _ qp1 qp2) = do
    closeQueryPlan qp1
    closeQueryPlan qp2

closeQueryPlan (QPParA _  qp1 qp2) = do
    closeQueryPlan qp1
    closeQueryPlan qp2

closeQueryPlan (QPOneA _)  = return ()
closeQueryPlan (QPZeroA _) = return ()
closeQueryPlan (QPAggregateA _ _ qp) =
    closeQueryPlan qp
