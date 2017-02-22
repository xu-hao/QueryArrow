{-# LANGUAGE MultiParamTypeClasses, TypeSynonymInstances, FlexibleInstances, FlexibleContexts, TypeFamilies, ScopedTypeVariables, TypeApplications, DataKinds #-}
module QueryArrow.QueryPlan where

import QueryArrow.DB.ResultStream
import QueryArrow.FO.Data
import Algebra.SemiBoundedLattice
import QueryArrow.DB.DB
import QueryArrow.Data.Heterogeneous.List

import Prelude  hiding (lookup, null)
import Data.List (elem, union, sortBy, groupBy, nub)
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
import Debug.Trace

type MSet a = Complemented (Set a)

data QueryPlan = Exec Formula
                | QPPar QueryPlan QueryPlan
                | QPChoice QueryPlan QueryPlan
                | QPSequencing QueryPlan QueryPlan
                | QPZero
                | QPOne
                | QPAggregate Aggregator QueryPlan deriving Show


data QueryPlanData  = QueryPlanData {
    linscopevs :: MSet Var,
    rinscopevs :: MSet Var,
    freevs :: Set Var,
    determinevs :: Set Var, -- determined vars
    paramvs :: Set Var, -- parameter vars
    returnvs :: Set Var, -- return vars
    combinedvs :: Set Var, -- combined return and available vars that are still in scope
    availablevs :: Set Var
}

dqdb :: QueryPlanData
dqdb = QueryPlanData top top bottom bottom bottom bottom bottom bottom

data QueryPlanNode2   = Exec2 Formula Int
                | QPChoice2 QueryPlan2 QueryPlan2
                | QPPar2 QueryPlan2 QueryPlan2
                | QPSequencing2 QueryPlan2 QueryPlan2
                | QPZero2
                | QPOne2
                | QPAggregate2 Aggregator QueryPlan2

type QueryPlan2  =  (QueryPlanData , QueryPlanNode2 )

showSet :: Set Var -> String
showSet s = "[" ++ List.intercalate "," (map serialize (toAscList s)) ++ "]"
showSet2 :: Complemented (Set Var) -> String
showSet2 (Include s) = "Include[" ++ List.intercalate "," (map serialize (toAscList s)) ++ "]"
showSet2 (Exclude s) = "Exclude[" ++ List.intercalate "," (map serialize (toAscList s)) ++ "]"

getQPSequencing2 :: QueryPlan2 -> [QueryPlan2]
getQPSequencing2 (_, QPSequencing2 qp1 qp2) = getQPSequencing2 qp1 ++ getQPSequencing2 qp2
getQPSequencing2 qp = [qp]

getQPChoice2 :: QueryPlan2 -> [QueryPlan2]
getQPChoice2 (_, QPChoice2 qp1 qp2) = getQPChoice2 qp1 ++ getQPChoice2 qp2
getQPChoice2 qp = [qp]

class ToTree a where
    toTree :: a -> Tree String
instance Show QueryPlanData where
    show qp = "[available=" ++ showSet (availablevs qp) ++ "|linscope=" ++ showSet2 (linscopevs qp) ++ "|param=" ++ showSet (paramvs qp) ++ "|free="
              ++ showSet (freevs qp) ++ "|determine=" ++ showSet (determinevs qp) ++ "|return="++ showSet (returnvs qp) ++ "|combined=" ++ showSet (combinedvs qp)
              ++ "|rinscope=" ++ showSet2 (rinscopevs qp) ++ "]"
instance ToTree QueryPlan2 where
    toTree (qpd, Exec2 f dbs) = Node ("exec " ++ serialize f ++ " at " ++ show dbs ++ show qpd ) []
    toTree qp@(qpd, QPSequencing2 qp1 qp2) = let qps = getQPSequencing2 qp in Node ("sequencing"++ show qpd ) (map toTree qps)
    toTree qp@(qpd, QPChoice2 qp1 qp2) = let qps = getQPChoice2 qp in Node ("choice"++ show qpd ) (map toTree qps)
    toTree (qpd, QPPar2 qp1 qp2) = Node ("parallel"++ show qpd ) [toTree qp1, toTree qp2]
    toTree (qpd, QPZero2) = Node ("zero"++ show qpd ) []
    toTree (qpd, QPOne2) = Node ("one"++ show qpd ) []
    toTree (qpd, QPAggregate2 agg qp1) = Node ("aggregate " ++ show agg ++ " " ++ show qpd) [toTree qp1]


data QueryPlan3 row = Exec3 (Set Var) (AbstractDBStatement row) String
                | QPChoice3 (QueryPlan3 row) (QueryPlan3 row)
                | QPPar3 (QueryPlan3 row) (QueryPlan3 row)
                | QPSequencing3 (QueryPlan3 row) (QueryPlan3 row)
                | QPZero3
                | QPOne3
                | QPAggregate3 (Set Var) Aggregator (QueryPlan3 row)

data QueryPlanT l = ExecT (Set Var) (HVariant' DBQueryTypeIso l) String
                | QPChoiceT (QueryPlanT l) (QueryPlanT l)
                | QPParT (QueryPlanT l) (QueryPlanT l)
                | QPSequencingT (QueryPlanT l) (QueryPlanT l)
                | QPZeroT
                | QPOneT
                | QPAggregateT (Set Var) Aggregator (QueryPlanT l)



findDB :: forall  (l :: [*]) . (HMapConstraint (IDatabaseUniformDBFormula Formula) l) => Set Var -> Formula -> Set Var -> HList l -> Int
findDB ret form env dbs =
  let pred0 = case form of
                  FAtomic (Atom pred0 _) -> pred0
                  FInsert (Lit _ (Atom pred0 _)) -> pred0
                  _ -> error ("findDB: cannot find database for " ++ show form)
  in
    toIntegerV
      (fromMaybe (error ("no database for predicate " ++ show pred0 ++ " available " ++ (
          let preds = filter (\(Pred (PredName _ pn1) _) ->
                          case pred0 of
                            PredName _ pn0 -> pn1 == pn0) (concat (hMapCUL @(IDatabaseUniformDBFormula Formula) getPreds dbs))
          in show preds))) (hFindCULV @(IDatabaseUniformDBFormula Formula) (\db -> pred0 `elem` (map predName (getPreds db)) && supported db ret form env) dbs))

formulaToQueryPlan :: (HMapConstraint (IDatabase) l) => HList l -> Formula -> QueryPlan
formulaToQueryPlan dbs  form@(FAtomic (Atom pred0  _)) =
    Exec form
formulaToQueryPlan _ FOne = QPOne
formulaToQueryPlan _ FZero = QPZero
formulaToQueryPlan dbs  (FChoice form1 form2) = QPChoice (formulaToQueryPlan dbs form1) (formulaToQueryPlan dbs form2)
formulaToQueryPlan dbs  (FPar form1 form2) = QPPar (formulaToQueryPlan dbs form1) (formulaToQueryPlan dbs form2)
formulaToQueryPlan dbs  (FSequencing form1 form2) = QPSequencing (formulaToQueryPlan dbs form1) (formulaToQueryPlan dbs form2)
formulaToQueryPlan dbs  (Aggregate agg form) = QPAggregate agg (formulaToQueryPlan dbs  form)
formulaToQueryPlan dbs  ins@(FInsert (Lit _ (Atom pred1 _))) =
        Exec ins




{- near semi-ring -}
simplifyQueryPlan :: QueryPlan -> QueryPlan

simplifyQueryPlan qp@(Exec _) = qp
simplifyQueryPlan (QPChoice qp1 qp2) =
    let qp1' = simplifyQueryPlan  qp1
        qp2' = simplifyQueryPlan  qp2 in
        case (qp1', qp2') of
            (QPZero, _) -> qp2'
            (_, QPZero) -> qp1'
            (_, _) -> QPChoice qp1' qp2'
simplifyQueryPlan (QPPar qp1 qp2) =
    let qp1' = simplifyQueryPlan  qp1
        qp2' = simplifyQueryPlan  qp2 in
        case (qp1', qp2') of
            (QPZero, _) -> qp2'
            (_, QPZero) -> qp1'
            (_, _) -> QPPar qp1' qp2'
simplifyQueryPlan  (QPSequencing qp1 qp2) =
    let qp1' = simplifyQueryPlan  qp1
        qp2' = simplifyQueryPlan  qp2 in
        case (qp1', qp2') of
            (QPOne, _) -> qp2'
            (QPZero, _) -> QPZero
            (_, QPOne) -> qp1'
            (_, _) -> QPSequencing qp1' qp2'
simplifyQueryPlan  (QPAggregate Not qp1) =
    let qp1' = simplifyQueryPlan  qp1 in
        case qp1' of
            QPOne -> QPZero
            QPZero -> QPOne
            _ -> QPAggregate Not qp1'
simplifyQueryPlan  (QPAggregate agg qp1) =
    let qp1' = simplifyQueryPlan  qp1 in
        QPAggregate agg qp1'
simplifyQueryPlan  qp = qp

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
        combinedvs = (combinedvs qp1 \/ combinedvs qp2) /\\ rinscopevs qp2
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
        combinedvs = combinedvs qp2 \/ combinedvs qp2
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
        combinedvs = combinedvs qp2 \/ combinedvs qp2
    }

supportedDB :: IDatabaseUniformDBFormula Formula db => Set Var -> Formula -> Set Var -> db -> Bool
supportedDB ret form vars db = supported db ret form vars

findDBQueryPlan :: HMapConstraint (IDatabaseUniformDBFormula Formula) l => HList l -> QueryPlan2 -> QueryPlan2
findDBQueryPlan dbsx qp@(qpd, Exec2 form _) =
  let dbx = findDB (returnvs qpd) form (paramvs qpd) dbsx in
      (qpd, Exec2 form dbx)
findDBQueryPlan dbsx (qpd, QPSequencing2 ip1  ip2) =
    let qp1' = findDBQueryPlan dbsx ip1
        qp2' = findDBQueryPlan dbsx ip2 in
        -- trace ("findDBQueryPlan: \n" ++ drawTree (toTree ip1) ++ "\n------------>\n" ++ drawTree (toTree qp1') ++ "\n and \n" ++
          -- drawTree (toTree ip2) ++ "\n------------------>\n" ++ drawTree (toTree qp2')) $
        (qpd, QPSequencing2 qp1' qp2')
findDBQueryPlan dbsx (qpd, QPChoice2 qp1 qp2) =
    let qp1' = findDBQueryPlan dbsx qp1
        qp2' = findDBQueryPlan dbsx qp2 in
        (qpd, QPChoice2 qp1' qp2')
findDBQueryPlan dbsx (qpd, QPPar2 qp1 qp2) =
    let qp1' = findDBQueryPlan dbsx qp1
        qp2' = findDBQueryPlan dbsx qp2 in
        (qpd, QPPar2 qp1' qp2')
findDBQueryPlan _ qp@(_, QPOne2) = qp
findDBQueryPlan _ qp@(_, QPZero2) = qp
findDBQueryPlan dbsx (qpd, QPAggregate2 agg qp1) =
    let qp1' = findDBQueryPlan dbsx qp1 in
        (qpd, QPAggregate2 agg qp1')


optimizeQueryPlan :: HMapConstraint (IDatabaseUniformDBFormula Formula) l => HList l -> QueryPlan2 -> QueryPlan2
optimizeQueryPlan _ qp@(qpd, Exec2 _ _) = qp

optimizeQueryPlan dbsx (qpd, QPSequencing2 ip1  ip2) =
    let qp1' = optimizeQueryPlan dbsx ip1
        qp2' = optimizeQueryPlan dbsx ip2 in
        -- trace ("optimizeQueryPlan: \n" ++ drawTree (toTree ip1) ++ "\n------------>\n" ++ drawTree (toTree qp1') ++ "\n and \n" ++
          -- drawTree (toTree ip2) ++ "\n------------------>\n" ++ drawTree (toTree qp2')) $
        case (qp1', qp2') of
            ((_, Exec2 form1 x1), (_, Exec2 form2 x2)) ->
                let dbs = x1 == x2
                    fse = fsequencing [form1, form2]
                    dbs' = fromMaybe (error "index out of range") (hApplyCUL @(IDatabaseUniformDBFormula Formula) @Bool (supportedDB (returnvs qpd) fse (paramvs qpd)) x1 dbsx) in
                    if dbs && dbs'
                            then (qpd, Exec2 fse x1)
                            else (qpd, QPSequencing2 qp1' qp2')
            ((qpd1, Exec2  _ _), (_, QPSequencing2 qp21@(qpd3, _) qp22)) ->
                (qpd ,QPSequencing2 (optimizeQueryPlan dbsx (combineQPSequencingData qpd1 qpd3, QPSequencing2 qp1' qp21)) qp22)
            ((_, QPSequencing2 qp11 qp12@(qpd3, _)), (qpd2, Exec2  _ _)) ->
                (qpd, QPSequencing2 qp11 (optimizeQueryPlan dbsx (combineQPSequencingData qpd3 qpd2, QPSequencing2 qp12 qp2')))
            ((qpd1, QPSequencing2 qp11 qp12@(qpd3, _)), (_, QPSequencing2 qp21@(qpd4, _) qp22)) ->
                (qpd, QPSequencing2 (combineQPSequencingData qpd1 qpd4, QPSequencing2 qp11 (optimizeQueryPlan dbsx (combineQPSequencingData qpd3 qpd4, QPSequencing2 qp12 qp21))) qp22)
            _ -> (qpd, QPSequencing2 qp1' qp2')
optimizeQueryPlan dbsx (qpd, QPChoice2 qp1 qp2) =
    let qp1' = optimizeQueryPlan dbsx qp1
        qp2' = optimizeQueryPlan dbsx qp2 in
        case (qp1', qp2') of
            ((_, Exec2 formula1 x1), (_, Exec2 formula2 x2)) ->
                let dbs = x1 == x2
                    fch = fchoice [formula1, formula2]
                    dbs' = fromMaybe (error "index out of range") (hApplyCUL @(IDatabaseUniformDBFormula Formula) @Bool (supportedDB (returnvs qpd) (fch) (paramvs qpd)) x1 dbsx) in
                    if dbs && dbs' then
                      (qpd, Exec2 fch x1)
                    else
                      (qpd, QPChoice2 qp1' qp2')
            ((qpd1, Exec2 _ _), (_, QPChoice2 qp21@(qpd21, _) qp22)) ->
                (qpd, QPChoice2 (optimizeQueryPlan dbsx (combineQPChoiceData qpd1 qpd21, QPChoice2 qp1' qp21)) qp22)
            ((_, QPChoice2 qp11 qp12@(qpd3, _)), (qpd2, Exec2 _ _)) ->
                (qpd, QPChoice2 qp11 (optimizeQueryPlan dbsx (combineQPChoiceData qpd3 qpd2, QPChoice2 qp12 qp2')))
            ((qpd1, QPChoice2 qp11 qp12@(qpd3, _)), (_, QPChoice2 qp21@(qpd4, _) qp22)) ->
                (qpd, QPChoice2 (combineQPChoiceData qpd1 qpd4, QPChoice2 qp11 (optimizeQueryPlan dbsx (combineQPChoiceData qpd3 qpd4, QPChoice2 qp12 qp21))) qp22)
            _ -> (qpd, QPChoice2 qp1' qp2')
optimizeQueryPlan dbsx (qpd, QPPar2 qp1 qp2) =
    let qp1' = optimizeQueryPlan dbsx qp1
        qp2' = optimizeQueryPlan dbsx qp2 in
        case (qp1', qp2') of
            ((_, Exec2 formula1 x1), (_, Exec2 formula2 x2)) ->
                let dbs = x1 == x2
                    fch = fchoice [formula1, formula2]
                    dbs' = fromMaybe (error "index out of range") (hApplyCUL @(IDatabaseUniformDBFormula Formula) @Bool (supportedDB (returnvs qpd) (fch) (paramvs qpd)) x1 dbsx) in
                    if dbs && dbs' then
                      (qpd, Exec2 fch x1)
                    else
                      (qpd, QPPar2 qp1' qp2')
            ((qpd1, Exec2 _ _), (_, QPPar2 qp21@(qpd21, _) qp22)) ->
                (qpd, QPPar2 (optimizeQueryPlan dbsx (combineQPParData qpd1 qpd21, QPPar2 qp1' qp21)) qp22)
            ((_, QPPar2 qp11 qp12@(qpd3, _)), (qpd2, Exec2 _ _)) ->
                (qpd, QPPar2 qp11 (optimizeQueryPlan dbsx (combineQPParData qpd3 qpd2, QPPar2 qp12 qp2')))
            ((qpd1, QPPar2 qp11 qp12@(qpd3, _)), (_, QPPar2 qp21@(qpd4, _) qp22)) ->
                (qpd, QPPar2 (combineQPParData qpd1 qpd4, QPPar2 qp11 (optimizeQueryPlan dbsx (combineQPParData qpd3 qpd4, QPPar2 qp12 qp21))) qp22)
            _ -> (qpd, QPPar2 qp1' qp2')
optimizeQueryPlan _ qp@(_, QPOne2) = qp
optimizeQueryPlan _ qp@(_, QPZero2) = qp
optimizeQueryPlan dbsx (qpd, QPAggregate2 agg qp1) =
    let qp1' = optimizeQueryPlan dbsx qp1 in
        case qp1' of
            (_, Exec2 formula1 x) ->
                        let exi = Aggregate agg formula1
                            dbs' = fromMaybe (error "index out of range") (hApplyCUL @(IDatabaseUniformDBFormula Formula) @Bool (supportedDB (returnvs qpd) exi (paramvs qpd)) x dbsx)  in
                            if dbs' then
                              (qpd, Exec2 exi  x)
                            else
                              (qpd, QPAggregate2 agg qp1')
            _ -> (qpd, QPAggregate2 agg qp1')

calculateVars :: Set Var -> MSet Var -> QueryPlan -> QueryPlan2
calculateVars lvars rvars qp = calculateVars2 lvars (calculateVars1 rvars qp)

calculateVars1 :: MSet Var -> QueryPlan -> QueryPlan2
calculateVars1  rvars (Exec form) =
    let fvs = freeVars form in
        ( dqdb{freevs =  fvs, determinevs =  fvs, linscopevs = Include fvs \/ rvars, rinscopevs = rvars}, (Exec2  form 0))

calculateVars1  rvars (QPSequencing qp1 qp2) =
    let qp2'@(qpd2, _) = calculateVars1 rvars qp2
        qp1'@(qpd1, _) = calculateVars1 (linscopevs qpd2) qp1 in
        ( dqdb{
            freevs = freevs qpd1 \/ freevs qpd2,
            determinevs = determinevs qpd1 \/ determinevs qpd2,
            linscopevs = linscopevs qpd1,
            rinscopevs = rinscopevs qpd2
            } , (QPSequencing2  qp1' qp2'))

calculateVars1 rvars  (QPChoice qp1 qp2) =
    let qp1'@(qpd1, _) = calculateVars1 rvars qp1
        qp2'@(qpd2, _) = calculateVars1 rvars qp2 in
        ( dqdb{
            freevs = freevs qpd1 \/ freevs qpd2,
            determinevs = determinevs qpd1 /\ determinevs qpd2,
            linscopevs = linscopevs qpd1 \/ linscopevs qpd2,
            rinscopevs = rinscopevs qpd1 \/ rinscopevs qpd2
            } , (QPChoice2  qp1' qp2'))
calculateVars1 rvars  (QPPar qp1 qp2) =
    let qp1'@(qpd1, _) = calculateVars1 rvars qp1
        qp2'@(qpd2, _) = calculateVars1 rvars qp2 in
        ( dqdb{
            freevs = freevs qpd1 \/ freevs qpd2,
            determinevs = determinevs qpd1 /\ determinevs qpd2,
            linscopevs = linscopevs qpd1 \/ linscopevs qpd2,
            rinscopevs = rinscopevs qpd1 \/ rinscopevs qpd2
            } , (QPPar2  qp1' qp2'))
calculateVars1 rvars  (QPOne) = (dqdb{freevs = bottom, determinevs = bottom, linscopevs = rvars, rinscopevs = rvars} , QPOne2)
calculateVars1 rvars  (QPZero) = (dqdb{freevs = bottom, determinevs = bottom, linscopevs = rvars, rinscopevs = rvars} , QPZero2)
calculateVars1 rvars  (QPAggregate agg@Not qp1) =
    let qp1'@(qpd1, _) = calculateVars1 rvars qp1 in
        ( dqdb{freevs = freevs qpd1, determinevs = bottom, linscopevs = rvars, rinscopevs = rvars} , (QPAggregate2 agg qp1'))
calculateVars1 rvars  (QPAggregate agg@Exists qp1) =
    let qp1'@(qpd1, _) = calculateVars1 rvars qp1 in
        ( dqdb{freevs = freevs qpd1, determinevs = bottom, linscopevs = rvars, rinscopevs = rvars} , (QPAggregate2 agg qp1'))
calculateVars1 rvars  (QPAggregate agg@(Summarize funcs groupby) qp1) =
    let qp1'@(qpd1, _) = calculateVars1 rvars qp1 in
        ( dqdb{freevs = freevs qpd1, determinevs = fromList (fst (unzip funcs)), linscopevs = linscopevs qpd1, rinscopevs = rvars} , (QPAggregate2 agg qp1'))
calculateVars1 rvars  (QPAggregate agg@(Limit _) qp1) =
    let qp1'@(qpd1, _) = calculateVars1 rvars qp1 in
        ( dqdb{freevs = freevs qpd1, determinevs = determinevs qpd1, linscopevs = linscopevs qpd1, rinscopevs = rinscopevs qpd1} , (QPAggregate2 agg qp1'))
calculateVars1 rvars  (QPAggregate agg@(Distinct) qp1) =
    let qp1'@(qpd1, _) = calculateVars1 rvars qp1 in
        ( dqdb{freevs = freevs qpd1, determinevs = determinevs qpd1, linscopevs = linscopevs qpd1, rinscopevs = rinscopevs qpd1} , (QPAggregate2 agg qp1'))
calculateVars1 rvars  (QPAggregate agg@(OrderByAsc _) qp1) =
    let qp1'@(qpd1, _) = calculateVars1 rvars qp1 in
        ( dqdb{freevs = freevs qpd1, determinevs = determinevs qpd1, linscopevs = linscopevs qpd1, rinscopevs = rinscopevs qpd1} , (QPAggregate2 agg qp1'))
calculateVars1 rvars  (QPAggregate agg@(OrderByDesc _) qp1) =
    let qp1'@(qpd1, _) = calculateVars1 rvars qp1 in
        ( dqdb{freevs = freevs qpd1, determinevs = determinevs qpd1, linscopevs = linscopevs qpd1, rinscopevs = rinscopevs qpd1} , (QPAggregate2 agg qp1'))

calculateVars1 rvars  (QPAggregate agg@(FReturn vars) qp1) =
  let qp1'@(qpd1, _) = calculateVars1 rvars qp1 in
    (dqdb{
        freevs = freevs qpd1,
        determinevs = fromList vars,
        linscopevs = linscopevs qpd1,
        rinscopevs = rvars
        } , QPAggregate2 agg qp1')



calculateVars2 :: Set Var -> QueryPlan2 -> QueryPlan2
calculateVars2  lvars (qpd, Exec2 form dbxs) =
            ( qpd{availablevs = lvars, paramvs = lvars /\ (freevs qpd), returnvs = (rinscopevs qpd //\ determinevs qpd \\\ lvars), combinedvs = rinscopevs qpd //\ (determinevs qpd \/ lvars) }, (Exec2  form dbxs))
calculateVars2  lvars (qpd, QPChoice2 qp1 qp2) =
            let qp1' = calculateVars2 lvars qp1
                qp2' = calculateVars2 lvars qp2 in
                ( qpd{availablevs = lvars, paramvs = lvars /\ (freevs qpd), returnvs = (rinscopevs qpd //\ determinevs qpd \\\ lvars), combinedvs = rinscopevs qpd //\ (determinevs qpd \/ lvars)} , (QPChoice2  qp1' qp2'))
calculateVars2  lvars (qpd, QPPar2 qp1 qp2) =
            let qp1' = calculateVars2 lvars qp1
                qp2' = calculateVars2 lvars qp2 in
                ( qpd{availablevs = lvars, paramvs = lvars /\ (freevs qpd), returnvs = (rinscopevs qpd //\ determinevs qpd \\\ lvars), combinedvs = rinscopevs qpd //\ (determinevs qpd \/ lvars)} , (QPPar2  qp1' qp2'))
calculateVars2  lvars (qpd, QPSequencing2 qp1 qp2) =
            let qp1'@(qpd1, _) = calculateVars2 lvars qp1
                qp2' = calculateVars2 (lvars \/ freevs qpd1) qp2 in
                ( qpd{availablevs = lvars, paramvs = lvars /\ (freevs qpd), returnvs = (rinscopevs qpd //\ determinevs qpd \\\ lvars), combinedvs = rinscopevs qpd //\ (determinevs qpd \/ lvars)} , (QPSequencing2  qp1' qp2'))

calculateVars2 lvars  (qpd, QPOne2) = ( qpd{availablevs = lvars, paramvs = bottom, returnvs = bottom, combinedvs = rinscopevs qpd //\ lvars} , QPOne2)
calculateVars2 lvars  (qpd, QPZero2) = ( qpd{availablevs = lvars, paramvs = bottom, returnvs = bottom,combinedvs = rinscopevs qpd //\ lvars} , QPZero2)
calculateVars2 lvars  (qpd, QPAggregate2 agg qp1) =
            let qp1' = calculateVars2 lvars qp1 in
                ( qpd{availablevs = lvars, paramvs = lvars /\ (freevs qpd), returnvs = (rinscopevs qpd //\ determinevs qpd) \\\ lvars,combinedvs = rinscopevs qpd //\ (determinevs qpd \/ lvars)} , (QPAggregate2 agg qp1'))



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

prepareTransaction :: [Int] -> QueryPlan2 -> ([Int], QueryPlan2)
prepareTransaction rdbxs qp@(_, (Exec2 _ x)) =
    (x : rdbxs, qp)
prepareTransaction rdbxs (qpd, QPChoice2 qp1 qp2) =
    let (dbxs1, qp1') = prepareTransaction rdbxs qp1
        (dbxs2, qp2') = prepareTransaction rdbxs qp2 in
        (dbxs1 `union` dbxs2, (qpd, QPChoice2 qp1' qp2'))
prepareTransaction rdbxs (qpd, QPPar2 qp1 qp2) =
    let (dbxs1, qp1') = prepareTransaction rdbxs qp1
        (dbxs2, qp2') = prepareTransaction rdbxs qp2 in
        (dbxs1 `union` dbxs2, (qpd, QPPar2 qp1' qp2'))
prepareTransaction rdbxs (qpd, QPSequencing2 qp1 qp2) =
    let (dbxs2, qp2') = prepareTransaction rdbxs qp2
        (dbxs1, qp1') = prepareTransaction dbxs2 qp1 in
        (dbxs1, (qpd, QPSequencing2 qp1' qp2'))
prepareTransaction rdbxs qp@(_, QPOne2) =  (rdbxs, qp)
prepareTransaction rdbxs qp@(_, QPZero2) =  (rdbxs, qp)
prepareTransaction rdbxs (qpd, QPAggregate2 agg qp1) =
    let (dbxs, qp1') = prepareTransaction rdbxs qp1 in
        (dbxs, (qpd, QPAggregate2 agg qp1'))


translateQueryPlan :: (HMapConstraint (IDatabaseUniformDBFormula Formula) l) => HList l -> QueryPlan2 -> IO (QueryPlanT l)

translateQueryPlan dbs (qpd, (Exec2  form x)) = do
        let vars = paramvs qpd
            vars2 = returnvs qpd
            trans :: (IDatabaseUniformDBFormula Formula db) => db ->  IO (DBQueryTypeIso db)
            trans db =  DBQueryTypeIso <$> (translateQuery db vars2 form vars)
        qu <-  fromMaybe (error "index out of range") <$> (hApplyACULV @(IDatabaseUniformDBFormula Formula) @(DBQueryTypeIso) trans x dbs)
        return (ExecT (combinedvs qpd) qu (serialize form))
translateQueryPlan dbs  (_, QPChoice2 qp1 qp2) = do
        qp1' <- translateQueryPlan dbs  qp1
        qp2' <- translateQueryPlan dbs  qp2
        return (QPChoiceT qp1' qp2')
translateQueryPlan dbs  (_, QPPar2 qp1 qp2) = do
        qp1' <- translateQueryPlan dbs  qp1
        qp2' <- translateQueryPlan dbs  qp2
        return (QPParT qp1' qp2')
translateQueryPlan dbs  (_, QPSequencing2 qp1 qp2) = do
        qp1' <- translateQueryPlan dbs  qp1
        qp2' <- translateQueryPlan dbs  qp2
        return (QPSequencingT qp1' qp2')
translateQueryPlan _ (_, QPOne2) = return QPOneT
translateQueryPlan _ (_, QPZero2) = return QPZeroT
translateQueryPlan dbs  (qpd, QPAggregate2 agg qp1) = do
        qp1' <- translateQueryPlan dbs  qp1
        return (QPAggregateT (combinedvs qpd) agg qp1')

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
                HMapConstraint IDBConnection (HMap ConnectionType l)) => HList' ConnectionType l -> QueryPlanT l -> IO (QueryPlan3 row )
prepareQueryPlan conns (ExecT combinedvs qu stmtshow) = do
            let pq :: (IDatabaseUniformRow row conn) => ConnectionType conn -> DBQueryTypeIso conn -> IO (AbstractDBStatement row)
                pq conn (DBQueryTypeIso qu') = AbstractDBStatement <$> prepareQuery conn qu'
            stmt <- hApply2CULV' @(IDatabaseUniformRow row) @ConnectionType @DBQueryTypeIso pq conns qu
            return (Exec3 combinedvs stmt stmtshow)
prepareQueryPlan dbs  (QPChoiceT qp1 qp2) = do
    qp1' <- prepareQueryPlan dbs  qp1
    qp2' <- prepareQueryPlan dbs  qp2
    return (QPChoice3 qp1' qp2')
prepareQueryPlan dbs  (QPParT qp1 qp2) = do
    qp1' <- prepareQueryPlan dbs  qp1
    qp2' <- prepareQueryPlan dbs  qp2
    return (QPPar3 qp1' qp2')
prepareQueryPlan dbs  (QPSequencingT qp1 qp2) = do
    qp1' <- prepareQueryPlan dbs  qp1
    qp2' <- prepareQueryPlan dbs  qp2
    return (QPSequencing3 qp1' qp2')
prepareQueryPlan _ QPOneT = return QPOne3
prepareQueryPlan _ QPZeroT = return QPOne3
prepareQueryPlan dbs  (QPAggregateT combinedvs agg qp1) = do
    qp1' <- prepareQueryPlan dbs  qp1
    return (QPAggregate3 combinedvs agg qp1')

addCleanupRS :: Monad m => (Bool -> m ()) -> ResultStream m row -> ResultStream m row
addCleanupRS a (ResultStream rs) = ResultStream (addCleanup a rs)

execQueryPlan :: (IResultRow row) =>  DBResultStream row -> QueryPlan3 row -> DBResultStream row
execQueryPlan  rs (Exec3 combinedvs stmt stmtshow) =
    case stmt of
        AbstractDBStatement stmt0 -> {- bracketPStream (return ()) (\_ -> dbStmtClose stmt0) (\_ -> -} do
                        row <- rs
                        liftIO $ infoM "QA" ("current row " ++ show row)
                        liftIO $ infoM "QA" ("execute " ++ stmtshow)
                        -- liftIO $ putStrLn ("execute " ++ stmtshow)
                        row2 <- dbStmtExec stmt0 (pure row)
                        liftIO $ infoM "QA" ("returns row")
                        return (transform combinedvs (row <> row2)){- ) -}

execQueryPlan  r (QPSequencing3  qp1 qp2) =
        let r1 = execQueryPlan  r qp1
            rs2 = execQueryPlan  r1 qp2 in
            rs2

execQueryPlan  rs (QPChoice3 qp1 qp2) = do
    row <- rs
    let rs1 = execQueryPlan  (pure row) qp1
        rs2 = execQueryPlan  (pure row) qp2 in
        rs1 <|> rs2

execQueryPlan rs (QPPar3 qp1 qp2) = do
    row <- rs
    let rs1 = execQueryPlan  (pure row) qp1
        rs2 = execQueryPlan  (pure row) qp2
    (rs1', rs2') <- lift $ concurrently (getAllResultsInStream rs1) (getAllResultsInStream rs2)
    listResultStream rs1' <|> listResultStream rs2'

execQueryPlan r QPOne3 = r
execQueryPlan rs (QPAggregate3 combinedvs (FReturn vars) qp) =
      transformResultStream combinedvs (execQueryPlan rs qp)
execQueryPlan rs QPZero3 = closeResultStream rs
execQueryPlan rs (QPAggregate3 combinedvs Not qp) =
    transformResultStream combinedvs (filterResultStream rs (\row -> do
        let rs2 = execQueryPlan (pure row) qp
        isResultStreamEmpty rs2))
execQueryPlan rs (QPAggregate3 combinedvs Exists qp) =
    transformResultStream combinedvs (filterResultStream rs (\row -> do
        let rs2 = execQueryPlan (pure row) qp
        emp <- isResultStreamEmpty rs2
        return (not emp)))
execQueryPlan rs (QPAggregate3 combinedvs (Summarize funcs groupby) qp) =
    transformResultStream combinedvs (do
        row <- rs
        let rs2 = execQueryPlan (pure row) qp
        rows <- lift $ getAllResultsInStream rs2
        let groups = groupBy (\a b -> all (\var -> ext var a == ext var b) groupby) rows
        let rows2 = map (\rows -> mconcat (reverse (map (\(v1, func1) ->
                      let m = case func1 of
                                Max v2 ->
                                    if List.null rows
                                        then error "max of empty list"
                                        else maximum (map (ext v2) rows)
                                Min v2 ->
                                    if List.null rows
                                        then error "min of empty list"
                                        else minimum (map (ext v2) rows)
                                Sum v2 ->
                                    sum (map (ext v2) rows)
                                Average v2 ->
                                    if List.null rows
                                        then error "min of empty list"
                                        else average (map (ext v2) rows)
                                Count ->
                                    fromIntegral (length rows)
                                CountDistinct v2 ->
                                    fromIntegral (length (List.nub (map (ext v2) rows))) in
                          ret v1 m) funcs))) groups where
                              average :: Fractional a => [a] -> a
                              average n = sum n / fromIntegral (length n)

        listResultStream (map (<> row) rows2))
execQueryPlan rs (QPAggregate3 combinedvs (Limit n) qp) =
    transformResultStream combinedvs (do
        row <- rs
        liftIO $ infoM "QueryPlan" ("limit " ++ show n ++ " input = " ++ show row)
        let rs2 = execQueryPlan (pure row) qp
        takeResultStream n rs2)
execQueryPlan rs (QPAggregate3 combinedvs Distinct qp) =
    transformResultStream combinedvs (do
        row <- rs
        liftIO $ infoM "QueryPlan" ("distinct input = " ++ show row)
        let rs2 = execQueryPlan (pure row) qp
        l <- lift $ getAllResultsInStream rs2
        listResultStream (nub l))
execQueryPlan rs (QPAggregate3 combinedvs (OrderByAsc v1) qp) =
    transformResultStream combinedvs (do
        row <- rs
        let rs2 = execQueryPlan (pure row) qp
        rows <- lift $ getAllResultsInStream rs2
        let rows' = sortBy (\row1 row2 -> compare (ext v1 row1) (ext v1 row2)) rows
        listResultStream rows')
execQueryPlan rs (QPAggregate3 combinedvs (OrderByDesc v1) qp) =
    transformResultStream combinedvs (do
        row <- rs
        let rs2 = execQueryPlan (pure row) qp
        rows <- lift $ getAllResultsInStream rs2
        let rows' = sortBy (\row1 row2 -> comparing Down (ext v1 row1) (ext v1 row2)) rows
        listResultStream rows')

closeQueryPlan :: QueryPlan3 row -> IO ()
closeQueryPlan (Exec3 combinedvs stmt stmtshow) =
    case stmt of
        AbstractDBStatement stmt0 -> dbStmtClose stmt0

closeQueryPlan (QPSequencing3  qp1 qp2) = do
  closeQueryPlan qp1
  closeQueryPlan qp2

closeQueryPlan (QPChoice3 qp1 qp2) = do
    closeQueryPlan qp1
    closeQueryPlan qp2

closeQueryPlan (QPPar3 qp1 qp2) = do
    closeQueryPlan qp1
    closeQueryPlan qp2

closeQueryPlan QPOne3 = return ()
closeQueryPlan QPZero3 = return ()
closeQueryPlan (QPAggregate3 _ _ qp) =
    closeQueryPlan qp
