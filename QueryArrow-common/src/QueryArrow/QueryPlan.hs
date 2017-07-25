{-# LANGUAGE MultiParamTypeClasses, TypeSynonymInstances, FlexibleInstances, FlexibleContexts, TypeFamilies, ScopedTypeVariables, TypeApplications, DataKinds, DeriveFunctor, PatternSynonyms, ConstraintKinds #-}
module QueryArrow.QueryPlan where

import QueryArrow.Semantics.ResultStream
import QueryArrow.Syntax.Data
import QueryArrow.Syntax.Types
import Algebra.SemiBoundedLattice
import QueryArrow.DB.DB
import QueryArrow.Semantics.ResultSet
import QueryArrow.Semantics.ResultHeader
import QueryArrow.Semantics.ResultSet.AbstractResultSet
import QueryArrow.Semantics.ResultSet.ResultStreamResultSet
import QueryArrow.Semantics.ResultSet.ResultSetResultStreamResultSet
import QueryArrow.Data.Heterogeneous.List

import Prelude  hiding (lookup, null, take)
import Data.List (elem, sortBy, groupBy, nub)
import qualified Data.List as List
import Control.Monad.Except
import Data.Maybe
import Data.Tree
import Data.Conduit
import Control.Concurrent.Async.Lifted
import System.Log.Logger
import Algebra.Lattice
import Data.Set (Set, fromList, toAscList)
import Data.Ord (comparing, Down(..))
import Data.Proxy
import QueryArrow.Data.Monoid.Action
import Debug.Trace
import Data.Foldable
import Data.Conduit.List (sourceList, sinkNull, isolate)
import Data.Monoid ((<>))

type MSet a = Complemented (Set a)

data QueryPlan0 a qp = Exec0 a
                | QPPar0 qp qp
                | QPChoice0 qp qp
                | QPSequencing0 qp qp
                | QPZero0
                | QPOne0
                | QPAggregate0 Aggregator qp deriving Functor

type QueryPlan1 a f = f (QueryPlan0 a)

type QueryPlan = QueryPlan1 FormulaT (Annotated VarTypeMap)

pattern Exec vtm a = Annotated vtm (Exec0 a)
pattern QPPar vtm a b = Annotated vtm (QPPar0 a b)
pattern QPChoice vtm a b = Annotated vtm (QPChoice0 a b)
pattern QPSequencing vtm a b = Annotated vtm (QPSequencing0 a b)
pattern QPZero vtm = Annotated vtm QPZero0
pattern QPOne vtm = Annotated vtm QPOne0
pattern QPAggregate vtm a b = Annotated vtm (QPAggregate0 a b)

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

dqdb :: QueryPlanData
dqdb = QueryPlanData top top bottom bottom bottom bottom bottom bottom mempty

type QueryPlan2 = QueryPlan1 (FormulaT, Int) (Annotated QueryPlanData)

pattern Exec2 qpd a = Annotated qpd (Exec0 a)
pattern QPPar2 qpd a b = Annotated qpd (QPPar0 a b)
pattern QPChoice2 qpd a b = Annotated qpd (QPChoice0 a b)
pattern QPSequencing2 qpd a b = Annotated qpd (QPSequencing0 a b)
pattern QPZero2 qpd = Annotated qpd QPZero0
pattern QPOne2 qpd =Annotated qpd  QPOne0
pattern QPAggregate2 qpd a b = Annotated qpd (QPAggregate0 a b)

showSet :: Set Var -> String
showSet s = "[" ++ List.intercalate "," (map serialize (toAscList s)) ++ "]"
showSet2 :: Complemented (Set Var) -> String
showSet2 (Include s) = "Include[" ++ List.intercalate "," (map serialize (toAscList s)) ++ "]"
showSet2 (Exclude s) = "Exclude[" ++ List.intercalate "," (map serialize (toAscList s)) ++ "]"

getQPSequencing2 :: QueryPlan2 -> [QueryPlan2]
getQPSequencing2 (QPSequencing2 _ qp1 qp2) = getQPSequencing2 qp1 ++ getQPSequencing2 qp2
getQPSequencing2 qp = [qp]

getQPChoice2 :: QueryPlan2 -> [QueryPlan2]
getQPChoice2 (QPChoice2 _ qp1 qp2) = getQPChoice2 qp1 ++ getQPChoice2 qp2
getQPChoice2 qp = [qp]

getQPPar2 :: QueryPlan2 -> [QueryPlan2]
getQPPar2 (QPPar2 _ qp1 qp2) = getQPPar2 qp1 ++ getQPPar2 qp2
getQPPar2 qp = [qp]

class ToTree a where
    toTree :: a -> Tree String
instance Show QueryPlanData where
    show qp = "[available=" ++ showSet (availablevs qp) ++ "|linscope=" ++ showSet2 (linscopevs qp) ++ "|param=" ++ showSet (paramvs qp) ++ "|free="
              ++ showSet (freevs qp) ++ "|determine=" ++ showSet (determinevs qp) ++ "|return="++ showSet (returnvs qp) ++ "|combined=" ++ showSet (combinedvs qp)
              ++ "|rinscope=" ++ showSet2 (rinscopevs qp) ++ "]"
instance ToTree QueryPlan2 where
    toTree (Exec2 qpd (f, dbs)) = Node ("exec " ++ serialize f ++ " at " ++ show dbs ++ show qpd ) []
    toTree qp@(QPSequencing2 qpd qp1 qp2) = let qps = getQPSequencing2 qp in Node ("sequencing"++ show qpd ) (map toTree qps)
    toTree qp@(QPChoice2 qpd qp1 qp2) = let qps = getQPChoice2 qp in Node ("choice"++ show qpd ) (map toTree qps)
    toTree qp@(QPPar2 qpd qp1 qp2) = let qps = getQPPar2 qp in Node ("parallel"++ show qpd ) (map toTree qps)
    toTree (QPZero2 qpd) = Node ("zero"++ show qpd ) []
    toTree (QPOne2 qpd) = Node ("one"++ show qpd ) []
    toTree (QPAggregate2 qpd agg qp1) = Node ("aggregate " ++ show agg ++ " " ++ show qpd) [toTree qp1]

class ToTree' a where
    toTree' :: a -> Tree String
instance ToTree' QueryPlan2 where
    toTree' (Exec2 qpd (f, dbs)) = Node ("exec " ++ serialize f ++ " at " ++ show dbs ++ show qpd ) []
    toTree' qp@(QPSequencing2 qpd qp1 qp2) = Node ("sequencing"++ show qpd ) [toTree' qp1, toTree' qp2]
    toTree' qp@(QPChoice2 qpd qp1 qp2) = Node ("choice"++ show qpd ) [toTree' qp1, toTree' qp2]
    toTree' qp@(QPPar2 qpd qp1 qp2) = Node ("parallel"++ show qpd ) [toTree' qp1, toTree' qp2]
    toTree' (QPZero2 qpd) = Node ("zero"++ show qpd ) []
    toTree' (QPOne2 qpd) = Node ("one"++ show qpd ) []
    toTree' (QPAggregate2 qpd agg qp1) = Node ("aggregate " ++ show agg ++ " " ++ show qpd) [toTree' qp1]

type QueryPlan3 trans row = QueryPlan1 (AbstractDBStatement trans row row, String) (Annotated QueryPlanData)

type QueryPlanT l = QueryPlan1 (HVariant' DBQueryTypeIso l, String) (Annotated QueryPlanData)

findDB :: forall  (l :: [*]) . (HMapConstraint (IDatabaseUniformDBFormula FormulaT) l) => Set Var -> FormulaT -> Set Var -> HList l -> Int
findDB ret form env dbs =
  let pred0 = case form of
                  Annotated _ (FAtomic0 (Atom pred0 _)) -> pred0
                  Annotated _ (FInsert0 (Lit _ (Atom pred0 _))) -> pred0
                  _ -> error ("findDB: cannot find database for " ++ show form)
  in
    toIntegerV
      (fromMaybe (error ("no database for predicate " ++ show pred0 ++ " available " ++ (
          let preds = filter (\(Pred (PredName _ pn1) _) ->
                          case pred0 of
                            PredName _ pn0 -> pn1 == pn0) (concat (hMapCUL @(IDatabaseUniformDBFormula FormulaT) getPreds dbs))
          in show preds))) (hFindCULV @(IDatabaseUniformDBFormula FormulaT) (\db -> pred0 `elem` (map predName (getPreds db)) && supported db ret form env) dbs))

formulaToQueryPlan :: (HMapConstraint (IDatabase) l) => HList l -> FormulaT -> QueryPlan
formulaToQueryPlan dbs  form@(Annotated vtm (FAtomic0 (Atom pred0  _))) =
    Exec vtm form -- redundent vtm
formulaToQueryPlan _ (Annotated vtm FOne0) = QPOne vtm
formulaToQueryPlan _ (Annotated vtm FZero0) = QPZero vtm
formulaToQueryPlan dbs  (Annotated vtm (FChoice0 form1 form2)) = QPChoice vtm (formulaToQueryPlan dbs form1) (formulaToQueryPlan dbs form2)
formulaToQueryPlan dbs  (Annotated vtm (FPar0 form1 form2)) = QPPar vtm (formulaToQueryPlan dbs form1) (formulaToQueryPlan dbs form2)
formulaToQueryPlan dbs  (Annotated vtm (FSequencing0 form1 form2)) = QPSequencing vtm (formulaToQueryPlan dbs form1) (formulaToQueryPlan dbs form2)
formulaToQueryPlan dbs  (Annotated vtm (Aggregate0 agg form)) = QPAggregate vtm agg (formulaToQueryPlan dbs  form)
formulaToQueryPlan dbs  ins@(Annotated vtm (FInsert0 (Lit _ (Atom pred1 _)))) =
        Exec vtm ins -- redundent vtm




{- near semi-ring -}
simplifyQueryPlan :: QueryPlan -> QueryPlan

simplifyQueryPlan qp@(Exec _ _) = qp
simplifyQueryPlan (QPChoice vtm qp1 qp2) =
    let qp1' = simplifyQueryPlan  qp1
        qp2' = simplifyQueryPlan  qp2 in
        case (qp1', qp2') of
            (QPZero _, _) -> qp2'
            (_, QPZero _) -> qp1'
            (_, _) -> QPChoice vtm qp1' qp2'
simplifyQueryPlan (QPPar vtm qp1 qp2) =
    let qp1' = simplifyQueryPlan  qp1
        qp2' = simplifyQueryPlan  qp2 in
        case (qp1', qp2') of
            (QPZero _, _) -> qp2'
            (_, QPZero _) -> qp1'
            (_, _) -> QPPar vtm qp1' qp2'
simplifyQueryPlan  (QPSequencing vtm qp1 qp2) =
    let qp1' = simplifyQueryPlan  qp1
        qp2' = simplifyQueryPlan  qp2 in
        case (qp1', qp2') of
            (QPOne _, _) -> qp2'
            (QPZero _, _) -> QPZero vtm
            (_, QPOne _) -> qp1'
            (_, _) -> QPSequencing vtm qp1' qp2'
simplifyQueryPlan  (QPAggregate vtm Not qp1) =
    let qp1' = simplifyQueryPlan  qp1 in
        case qp1' of
            QPOne _ -> QPZero vtm
            QPZero _ -> QPOne vtm
            _ -> QPAggregate vtm Not qp1'
simplifyQueryPlan  (QPAggregate vtm agg qp1) =
    let qp1' = simplifyQueryPlan  qp1 in
        QPAggregate vtm agg qp1'
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

supportedDB :: IDatabaseUniformDBFormula FormulaT db => Set Var -> FormulaT -> Set Var -> db -> Bool
supportedDB ret form vars db = supported db ret form vars

findDBQueryPlan :: HMapConstraint (IDatabaseUniformDBFormula FormulaT) l => HList l -> QueryPlan2 -> QueryPlan2
findDBQueryPlan dbsx qp@(Exec2 qpd (form, _)) =
  let dbx = findDB (returnvs qpd) form (paramvs qpd) dbsx in
      (Exec2 qpd (form, dbx))
findDBQueryPlan dbsx (QPSequencing2 qpd ip1  ip2) =
    let qp1' = findDBQueryPlan dbsx ip1
        qp2' = findDBQueryPlan dbsx ip2 in
        -- trace ("findDBQueryPlan: \n" ++ drawTree (toTree ip1) ++ "\n------------>\n" ++ drawTree (toTree qp1') ++ "\n and \n" ++
          -- drawTree (toTree ip2) ++ "\n------------------>\n" ++ drawTree (toTree qp2')) $
        (QPSequencing2 qpd qp1' qp2')
findDBQueryPlan dbsx (QPChoice2 qpd qp1 qp2) =
    let qp1' = findDBQueryPlan dbsx qp1
        qp2' = findDBQueryPlan dbsx qp2 in
        (QPChoice2 qpd qp1' qp2')
findDBQueryPlan dbsx (QPPar2 qpd qp1 qp2) =
    let qp1' = findDBQueryPlan dbsx qp1
        qp2' = findDBQueryPlan dbsx qp2 in
        (QPPar2 qpd qp1' qp2')
findDBQueryPlan _ qp@(QPOne2 _) = qp
findDBQueryPlan _ qp@(QPZero2 _) = qp
findDBQueryPlan dbsx (QPAggregate2 qpd agg qp1) =
    let qp1' = findDBQueryPlan dbsx qp1 in
        (QPAggregate2 qpd agg qp1')


optimizeQueryPlan :: HMapConstraint (IDatabaseUniformDBFormula FormulaT) l => HList l -> QueryPlan2 -> QueryPlan2
optimizeQueryPlan _ qp@(Exec2 qpd _) = qp

optimizeQueryPlan dbsx (QPSequencing2 qpd ip1  ip2) =
    let qp1' = optimizeQueryPlan dbsx ip1
        qp2' = optimizeQueryPlan dbsx ip2 in
        -- trace ("optimizeQueryPlan: \n" ++ drawTree (toTree ip1) ++ "\n------------>\n" ++ drawTree (toTree qp1') ++ "\n and \n" ++
          -- drawTree (toTree ip2) ++ "\n------------------>\n" ++ drawTree (toTree qp2')) $
        case (qp1', qp2') of
            (Exec2 _ (form1, x1), Exec2 _ (form2, x2)) ->
                let dbs = x1 == x2
                    fse = fsequencing1 [form1, form2]
                    dbs' = fromMaybe (error "index out of range") (hApplyCUL @(IDatabaseUniformDBFormula FormulaT) @Bool (supportedDB (returnvs qpd) fse (paramvs qpd)) x1 dbsx) in
                    if dbs && dbs'
                            then Exec2 qpd (fse, x1)
                            else QPSequencing2 qpd qp1' qp2'
            (Exec2 qpd1 _, QPSequencing2 _ qp21@(Annotated qpd3 _) qp22) ->
                QPSequencing2 qpd (optimizeQueryPlan dbsx (QPSequencing2 (combineQPSequencingData qpd1 qpd3) qp1' qp21)) qp22
            (QPSequencing2 _ qp11 qp12@(Annotated qpd3 _), Exec2 qpd2 _) ->
                QPSequencing2 qpd qp11 (optimizeQueryPlan dbsx (QPSequencing2 (combineQPSequencingData qpd3 qpd2) qp12 qp2'))
            (QPSequencing2 qpd1 qp11 qp12@(Annotated qpd3 _), QPSequencing2 _ qp21@(Annotated qpd4 _) qp22) ->
                QPSequencing2 qpd (QPSequencing2 (combineQPSequencingData qpd1 qpd4) qp11 (optimizeQueryPlan dbsx (QPSequencing2 (combineQPSequencingData qpd3 qpd4) qp12 qp21))) qp22
            _ -> QPSequencing2 qpd qp1' qp2'
optimizeQueryPlan dbsx (QPChoice2 qpd qp1 qp2) =
    let qp1' = optimizeQueryPlan dbsx qp1
        qp2' = optimizeQueryPlan dbsx qp2 in
        case (qp1', qp2') of
            (Exec2 _ (formula1, x1), Exec2 _ (formula2, x2)) ->
                let dbs = x1 == x2
                    fch = fchoice1 [formula1, formula2]
                    dbs' = fromMaybe (error "index out of range") (hApplyCUL @(IDatabaseUniformDBFormula FormulaT) @Bool (supportedDB (returnvs qpd) (fch) (paramvs qpd)) x1 dbsx) in
                    if dbs && dbs' then
                      Exec2 qpd (fch, x1)
                    else
                      QPChoice2 qpd qp1' qp2'
            (Exec2 qpd1 _, QPChoice2 _ qp21@(Annotated qpd21 _) qp22) ->
                QPChoice2 qpd (optimizeQueryPlan dbsx (QPChoice2 (combineQPChoiceData qpd1 qpd21) qp1' qp21)) qp22
            (QPChoice2 _ qp11 qp12@(Annotated qpd3 _), Exec2 qpd2 _) ->
                QPChoice2 qpd qp11 (optimizeQueryPlan dbsx (QPChoice2 (combineQPChoiceData qpd3 qpd2) qp12 qp2'))
            (QPChoice2 qpd1 qp11 qp12@(Annotated qpd3 _), QPChoice2 _ qp21@(Annotated qpd4 _) qp22) ->
                QPChoice2 qpd (QPChoice2 (combineQPChoiceData qpd1 qpd4) qp11 (optimizeQueryPlan dbsx (QPChoice2 (combineQPChoiceData qpd3 qpd4) qp12 qp21))) qp22
            _ -> QPChoice2 qpd qp1' qp2'
optimizeQueryPlan dbsx (QPPar2 qpd qp1 qp2) =
    let qp1' = optimizeQueryPlan dbsx qp1
        qp2' = optimizeQueryPlan dbsx qp2 in
        case (qp1', qp2') of
            (Exec2 _ (formula1, x1), Exec2 _ (formula2, x2)) ->
                let dbs = x1 == x2
                    fch = fpar1 [formula1, formula2]
                    dbs' = fromMaybe (error "index out of range") (hApplyCUL @(IDatabaseUniformDBFormula FormulaT) @Bool (supportedDB (returnvs qpd) (fch) (paramvs qpd)) x1 dbsx) in
                    if dbs && dbs' then
                      Exec2 qpd (fch, x1)
                    else
                      QPPar2 qpd qp1' qp2'
            (Exec2 qpd1 _, QPPar2 _ qp21@(Annotated qpd21 _) qp22) ->
                QPPar2 qpd (optimizeQueryPlan dbsx (QPPar2 (combineQPParData qpd1 qpd21) qp1' qp21)) qp22
            (QPPar2 _ qp11 qp12@(Annotated qpd3 _), Exec2 qpd2 _) ->
                QPPar2 qpd qp11 (optimizeQueryPlan dbsx (QPPar2 (combineQPParData qpd3 qpd2) qp12 qp2'))
            (QPPar2 qpd1 qp11 qp12@(Annotated qpd3 _), QPPar2 _ qp21@(Annotated qpd4 _) qp22) ->
                QPPar2 qpd (QPPar2 (combineQPParData qpd1 qpd4) qp11 (optimizeQueryPlan dbsx (QPPar2 (combineQPParData qpd3 qpd4) qp12 qp21))) qp22
            _ -> QPPar2 qpd qp1' qp2'
optimizeQueryPlan _ qp@(QPOne2 _) = qp
optimizeQueryPlan _ qp@(QPZero2 _) = qp
optimizeQueryPlan dbsx (QPAggregate2 qpd agg qp1) =
    let qp1' = optimizeQueryPlan dbsx qp1 in
        case qp1' of
            Exec2 _ (formula1, x) ->
                        let vtm = vartypemap qpd
                            exi = Annotated vtm (Aggregate0 agg formula1)
                            dbs' = fromMaybe (error "index out of range") (hApplyCUL @(IDatabaseUniformDBFormula FormulaT) @Bool (supportedDB (returnvs qpd) exi (paramvs qpd)) x dbsx)  in
                            if dbs' then
                              Exec2 qpd (exi,  x)
                            else
                              QPAggregate2 qpd agg qp1'
            _ -> QPAggregate2 qpd agg qp1'

calculateVars :: Set Var -> MSet Var -> QueryPlan -> QueryPlan2
calculateVars lvars rvars qp = calculateVars2 lvars (calculateVars1 rvars qp)

calculateVars1 :: MSet Var -> QueryPlan -> QueryPlan2
calculateVars1 rvars (Exec vtm form) =
    let fvs = freeVars form in
        Exec2 dqdb{freevs =  fvs, determinevs =  fvs, linscopevs = Include fvs \/ rvars, rinscopevs = rvars, vartypemap = vtm} (form, 0)

calculateVars1  rvars (QPSequencing vtm qp1 qp2) =
    let qp2'@(Annotated qpd2 _) = calculateVars1 rvars qp2
        qp1'@(Annotated qpd1 _) = calculateVars1 (linscopevs qpd2) qp1 in
        QPSequencing2   dqdb{
            freevs = freevs qpd1 \/ freevs qpd2,
            determinevs = determinevs qpd1 \/ determinevs qpd2,
            linscopevs = linscopevs qpd1,
            rinscopevs = rinscopevs qpd2, vartypemap = vtm
            } qp1' qp2'

calculateVars1 rvars  (QPChoice vtm qp1 qp2) =
    let qp1'@(Annotated qpd1 _) = calculateVars1 rvars qp1
        qp2'@(Annotated qpd2 _) = calculateVars1 rvars qp2 in
        QPChoice2  dqdb{
            freevs = freevs qpd1 \/ freevs qpd2,
            determinevs = determinevs qpd1 /\ determinevs qpd2,
            linscopevs = linscopevs qpd1 \/ linscopevs qpd2,
            rinscopevs = rinscopevs qpd1 \/ rinscopevs qpd2, vartypemap = vtm
            } qp1' qp2'
calculateVars1 rvars  (QPPar vtm qp1 qp2) =
    let qp1'@(Annotated qpd1 _) = calculateVars1 rvars qp1
        qp2'@(Annotated qpd2 _) = calculateVars1 rvars qp2 in
        QPPar2 dqdb{
            freevs = freevs qpd1 \/ freevs qpd2,
            determinevs = determinevs qpd1 /\ determinevs qpd2,
            linscopevs = linscopevs qpd1 \/ linscopevs qpd2,
            rinscopevs = rinscopevs qpd1 \/ rinscopevs qpd2, vartypemap = vtm
            } qp1' qp2'
calculateVars1 rvars  (QPOne vtm) = QPOne2 dqdb{freevs = bottom, determinevs = bottom, linscopevs = rvars, rinscopevs = rvars, vartypemap = vtm}
calculateVars1 rvars  (QPZero vtm) = QPZero2 dqdb{freevs = bottom, determinevs = bottom, linscopevs = rvars, rinscopevs = rvars, vartypemap = vtm}
calculateVars1 rvars  (QPAggregate vtm agg@Not qp1) =
    let qp1'@(Annotated qpd1 _) = calculateVars1 rvars qp1 in
        QPAggregate2 dqdb{freevs = freevs qpd1, determinevs = bottom, linscopevs = rvars, rinscopevs = rvars, vartypemap = vtm}  agg qp1'
calculateVars1 rvars  (QPAggregate vtm agg@Exists qp1) =
    let qp1'@(Annotated qpd1 _) = calculateVars1 rvars qp1 in
        QPAggregate2  dqdb{freevs = freevs qpd1, determinevs = bottom, linscopevs = rvars, rinscopevs = rvars, vartypemap = vtm}  agg qp1'
calculateVars1 rvars  (QPAggregate vtm agg@(Summarize funcs groupby) qp1) =
    let qp1'@(Annotated qpd1 _) = calculateVars1 rvars qp1 in
        QPAggregate2  dqdb{freevs = freevs qpd1, determinevs = fromList (map fst funcs), linscopevs = linscopevs qpd1, rinscopevs = rvars \/ Include (fromList (map fst funcs)), vartypemap = vtm}  agg qp1'
calculateVars1 rvars  (QPAggregate vtm agg@(Limit _) qp1) =
    let qp1'@(Annotated qpd1 _) = calculateVars1 rvars qp1 in
        QPAggregate2 dqdb{freevs = freevs qpd1, determinevs = determinevs qpd1, linscopevs = linscopevs qpd1, rinscopevs = rinscopevs qpd1, vartypemap = vtm}  agg qp1'
calculateVars1 rvars  (QPAggregate vtm agg@Distinct qp1) =
    let qp1'@(Annotated qpd1 _) = calculateVars1 rvars qp1 in
        QPAggregate2 dqdb{freevs = freevs qpd1, determinevs = determinevs qpd1, linscopevs = linscopevs qpd1, rinscopevs = rinscopevs qpd1, vartypemap = vtm}   agg qp1'
calculateVars1 rvars  (QPAggregate vtm agg@(OrderByAsc _) qp1) =
    let qp1'@(Annotated qpd1 _) = calculateVars1 rvars qp1 in
        QPAggregate2 dqdb{freevs = freevs qpd1, determinevs = determinevs qpd1, linscopevs = linscopevs qpd1, rinscopevs = rinscopevs qpd1, vartypemap = vtm}   agg qp1'
calculateVars1 rvars  (QPAggregate vtm agg@(OrderByDesc _) qp1) =
    let qp1'@(Annotated qpd1 _) = calculateVars1 rvars qp1 in
        QPAggregate2 dqdb{freevs = freevs qpd1, determinevs = determinevs qpd1, linscopevs = linscopevs qpd1, rinscopevs = rinscopevs qpd1, vartypemap = vtm}   agg qp1'
calculateVars1 rvars  (QPAggregate vtm agg@(FReturn vars) qp1) =
  let qp1'@(Annotated qpd1 _) = calculateVars1 rvars qp1 in
    QPAggregate2 dqdb{
        freevs = freevs qpd1,
        determinevs = fromList vars,
        linscopevs = linscopevs qpd1,
        rinscopevs = rvars, vartypemap = vtm
        } agg qp1'



calculateVars2 :: Set Var -> QueryPlan2 -> QueryPlan2
calculateVars2  lvars (Exec2 qpd (form, dbxs)) =
            Exec2 qpd{availablevs = lvars, paramvs = lvars /\ (freevs qpd), returnvs = (rinscopevs qpd //\ determinevs qpd \\\ lvars), combinedvs = rinscopevs qpd //\ (determinevs qpd \/ lvars) }  (form, dbxs)
calculateVars2  lvars (QPChoice2 qpd qp1 qp2) =
            let qp1' = calculateVars2 lvars qp1
                qp2' = calculateVars2 lvars qp2 in
                QPChoice2  qpd{availablevs = lvars, paramvs = lvars /\ (freevs qpd), returnvs = (rinscopevs qpd //\ determinevs qpd \\\ lvars), combinedvs = rinscopevs qpd //\ (determinevs qpd \/ lvars)}   qp1' qp2'
calculateVars2  lvars (QPPar2 qpd qp1 qp2) =
            let qp1' = calculateVars2 lvars qp1
                qp2' = calculateVars2 lvars qp2 in
                QPPar2  qpd{availablevs = lvars, paramvs = lvars /\ (freevs qpd), returnvs = (rinscopevs qpd //\ determinevs qpd \\\ lvars), combinedvs = rinscopevs qpd //\ (determinevs qpd \/ lvars)}   qp1' qp2'
calculateVars2  lvars (QPSequencing2 qpd qp1 qp2) =
            let qp1'@(Annotated qpd1 _) = calculateVars2 lvars qp1
                qp2' = calculateVars2 (combinedvs qpd1) qp2 in
                QPSequencing2  qpd{availablevs = lvars, paramvs = lvars /\ (freevs qpd), returnvs = (rinscopevs qpd //\ determinevs qpd \\\ lvars), combinedvs = rinscopevs qpd //\ (determinevs qpd \/ lvars)}   qp1' qp2'

calculateVars2 lvars  (QPOne2 qpd) = QPOne2 qpd{availablevs = lvars, paramvs = bottom, returnvs = bottom, combinedvs = rinscopevs qpd //\ lvars}
calculateVars2 lvars  (QPZero2 qpd) = QPZero2 qpd{availablevs = lvars, paramvs = bottom, returnvs = bottom,combinedvs = rinscopevs qpd //\ lvars}

calculateVars2 lvars  (QPAggregate2 qpd agg@Not qp1) =
            let qp1' = calculateVars2 lvars qp1 in
                QPAggregate2 qpd{availablevs = lvars, paramvs = lvars /\ (freevs qpd), returnvs = (rinscopevs qpd //\ determinevs qpd) \\\ lvars,combinedvs = rinscopevs qpd //\ (determinevs qpd \/ lvars)} agg qp1'
calculateVars2 lvars  (QPAggregate2 qpd agg@Exists qp1) =
            let qp1' = calculateVars2 lvars qp1 in
                QPAggregate2 qpd{availablevs = lvars, paramvs = lvars /\ (freevs qpd), returnvs = (rinscopevs qpd //\ determinevs qpd) \\\ lvars,combinedvs = rinscopevs qpd //\ (determinevs qpd \/ lvars)} agg qp1'
calculateVars2 lvars  (QPAggregate2 qpd agg@(Summarize funcs groupby) qp1) =
            let qp1' = calculateVars2 lvars qp1 in
                QPAggregate2 qpd{availablevs = lvars, paramvs = lvars /\ (freevs qpd), returnvs = (rinscopevs qpd //\ determinevs qpd) \\\ lvars,combinedvs = rinscopevs qpd //\ (determinevs qpd \/ lvars)} agg qp1'
calculateVars2 lvars  (QPAggregate2 qpd agg@(Limit _) qp1) =
            let qp1' = calculateVars2 lvars qp1 in
                QPAggregate2 qpd{availablevs = lvars, paramvs = lvars /\ (freevs qpd), returnvs = (rinscopevs qpd //\ determinevs qpd) \\\ lvars,combinedvs = rinscopevs qpd //\ (determinevs qpd \/ lvars)} agg qp1'
calculateVars2 lvars  (QPAggregate2 qpd agg@Distinct qp1) =
            let qp1' = calculateVars2 lvars qp1 in
                QPAggregate2 qpd{availablevs = lvars, paramvs = lvars /\ (freevs qpd), returnvs = (rinscopevs qpd //\ determinevs qpd) \\\ lvars,combinedvs = rinscopevs qpd //\ (determinevs qpd \/ lvars)} agg qp1'
calculateVars2 lvars  (QPAggregate2 qpd agg@(OrderByAsc _) qp1) =
            let qp1' = calculateVars2 lvars qp1 in
                QPAggregate2 qpd{availablevs = lvars, paramvs = lvars /\ (freevs qpd), returnvs = (rinscopevs qpd //\ determinevs qpd) \\\ lvars,combinedvs = rinscopevs qpd //\ (determinevs qpd \/ lvars)} agg qp1'
calculateVars2 lvars  (QPAggregate2 qpd agg@(OrderByDesc _) qp1) =
            let qp1' = calculateVars2 lvars qp1 in
                QPAggregate2 qpd{availablevs = lvars, paramvs = lvars /\ (freevs qpd), returnvs = (rinscopevs qpd //\ determinevs qpd) \\\ lvars,combinedvs = rinscopevs qpd //\ (determinevs qpd \/ lvars)} agg qp1'
calculateVars2 lvars  (QPAggregate2 qpd agg@(FReturn vars) qp1) =
            let qp1' = calculateVars2 lvars qp1 in
                QPAggregate2 qpd{availablevs = lvars, paramvs = lvars /\ (freevs qpd), returnvs = (rinscopevs qpd //\ determinevs qpd) \\\ lvars,combinedvs = rinscopevs qpd //\ (determinevs qpd \/ lvars)} agg qp1'



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


translateQueryPlan :: (HMapConstraint (IDatabaseUniformDBFormula FormulaT) l) => HList l -> QueryPlan2 -> IO (QueryPlanT l)

translateQueryPlan dbs (Exec2 qpd (form, x)) = do
        let vars = paramvs qpd
            vars2 = returnvs qpd
            trans :: (IDatabaseUniformDBFormula FormulaT db) => db ->  IO (DBQueryTypeIso db)
            trans db =  -- trace ("translateQueryPlan: " ++ show form) $
                DBQueryTypeIso <$> (translateQuery db vars2 form vars)
        qu <-  fromMaybe (error "index out of range") <$> (hApplyACULV @(IDatabaseUniformDBFormula FormulaT) @(DBQueryTypeIso) trans x dbs)
        return (Exec2 qpd (qu, (serialize form)))
translateQueryPlan dbs  (QPChoice2 qpd qp1 qp2) = do
        qp1' <- translateQueryPlan dbs  qp1
        qp2' <- translateQueryPlan dbs  qp2
        return (QPChoice2 qpd qp1' qp2')
translateQueryPlan dbs  (QPPar2 qpd qp1 qp2) = do
        qp1' <- translateQueryPlan dbs  qp1
        qp2' <- translateQueryPlan dbs  qp2
        return (QPPar2 qpd qp1' qp2')
translateQueryPlan dbs  (QPSequencing2 qpd qp1 qp2) = do
        qp1' <- translateQueryPlan dbs  qp1
        qp2' <- translateQueryPlan dbs  qp2
        return (QPSequencing2 qpd qp1' qp2')
translateQueryPlan _ (QPOne2 qpd) = return (QPOne2 qpd)
translateQueryPlan _ (QPZero2 qpd) = return (QPZero2 qpd)
translateQueryPlan dbs  (QPAggregate2 qpd agg qp1) = do
        qp1' <- translateQueryPlan dbs  qp1
        return (QPAggregate2 qpd agg qp1')

hDbOpen :: (HMapConstraint IDatabase l) => HList l -> IO (HList' ConnectionType l)
hDbOpen dbs =
    let dbo :: (IDatabase db) => db -> IO (ConnectionType db)
        dbo db = dbOpen db in
        hMapACULL @IDatabase @ConnectionType dbo dbs


type CDatabaseUniformRow trans row conn = (IDatabase conn, ResultSetRowType (ResultSetType (StatementType (ConnectionType conn))) ~ row, InputRowType (StatementType (ConnectionType conn)) ~ row, ResultSetTransType (ResultSetType (StatementType (ConnectionType conn))) ~ trans)
class CDatabaseUniformRow trans row conn => IDatabaseUniformRow trans row conn
instance CDatabaseUniformRow trans row conn => IDatabaseUniformRow trans row conn

type CDatabaseUniformDBFormula form conn = (IDatabase conn, DBFormulaType conn ~ form)
class CDatabaseUniformDBFormula form conn => IDatabaseUniformDBFormula form conn
instance CDatabaseUniformDBFormula form conn => IDatabaseUniformDBFormula form conn

type CDatabaseUniformRowAndDBFormula trans row formula db = (IDatabaseUniformRow trans row db, IDatabaseUniformDBFormula formula db)
class CDatabaseUniformRowAndDBFormula trans row formula db => IDatabaseUniformRowAndDBFormula trans row formula db
instance CDatabaseUniformRowAndDBFormula trans row formula db => IDatabaseUniformRowAndDBFormula trans row formula db

prepareQueryPlan :: forall trans (row :: *) (l :: [*]). (IResultRow row, HMapConstraint (IDatabaseUniformRow trans row) l,
                HMapConstraint IDBConnection (HMap ConnectionType l)) => HList' ConnectionType l -> QueryPlanT l -> IO (QueryPlan3 trans row )
prepareQueryPlan conns (Exec2 qpd (qu, stmtshow)) = do
            let pq :: (IDatabaseUniformRow trans row conn) => ConnectionType conn -> DBQueryTypeIso conn -> IO (AbstractDBStatement trans row row)
                pq conn (DBQueryTypeIso qu') = AbstractDBStatement <$> prepareQuery conn qu'
            stmt <- hApply2CULV' @(IDatabaseUniformRow trans row) @ConnectionType @DBQueryTypeIso pq conns qu
            return (Exec2 qpd (stmt, stmtshow))
prepareQueryPlan dbs  (QPChoice2 qpd qp1 qp2) = do
    qp1' <- prepareQueryPlan dbs  qp1
    qp2' <- prepareQueryPlan dbs  qp2
    return (QPChoice2 qpd qp1' qp2')
prepareQueryPlan dbs  (QPPar2 qpd qp1 qp2) = do
    qp1' <- prepareQueryPlan dbs  qp1
    qp2' <- prepareQueryPlan dbs  qp2
    return (QPPar2 qpd qp1' qp2')
prepareQueryPlan dbs  (QPSequencing2 qpd qp1 qp2) = do
    qp1' <- prepareQueryPlan dbs  qp1
    qp2' <- prepareQueryPlan dbs  qp2
    return (QPSequencing2 qpd qp1' qp2')
prepareQueryPlan _ (QPOne2 qpd) = return (QPOne2 qpd)
prepareQueryPlan _ (QPZero2 qpd) = return (QPZero2 qpd)
prepareQueryPlan dbs  (QPAggregate2 qpd agg qp1) = do
    qp1' <- prepareQueryPlan dbs  qp1
    return (QPAggregate2 qpd agg qp1')

addCleanupRS :: Monad m => (Bool -> m ()) -> ResultStream m row -> ResultStream m row
addCleanupRS a (ResultStream rs) = ResultStream (addCleanup a rs)

oneRowResultSet :: (Monoid trans) => HeaderType row -> row -> ResultStreamResultSet trans row
oneRowResultSet hdr row = ResultStreamResultSet mempty hdr (ResultStream (yield row))

execQueryPlan :: forall a trans row . (ResultSet a, ResultSetTransType a ~ trans, ResultSetRowType a ~ row) =>
                  a -> QueryPlan3 trans row -> IO (AbstractResultSet trans row)
execQueryPlan rset (Exec2 qpd (stmt, stmtshow)) =
    case stmt of
        AbstractDBStatement stmt0 -> do
          liftIO $ infoM "QA" ("execute " ++ stmtshow)
          rset2 <- dbStmtExec stmt0 rset
          let hdr2 = getHeader rset2
          let trans2 = filterRow (Proxy :: Proxy row) (combinedvs qpd) hdr2 :: trans
          return (AbstractResultSet (act trans2 rset2))

execQueryPlan r (QPSequencing2 _ qp1 qp2) = do
        r1 <- execQueryPlan r qp1
        execQueryPlan r1 qp2

execQueryPlan  rset (QPChoice2 qpd qp1 qp2)  =
  let hdr = getHeader rset
      hdrout = toHeader (toAscList (combinedvs qpd)) in
      return (AbstractResultSet (ResultSetResultStreamResultSet mempty hdrout (ResultStream (runResultStream (toResultStream rset) =$= awaitForever (\row -> do
        let rset2 = oneRowResultSet hdr row
        rset3 <- liftIO $ execQueryPlan rset2 qp1
        rset4 <- liftIO $ execQueryPlan rset2 qp2
        let hdr3 = getHeader rset3
        let hdr4 = getHeader rset4
        let trans3 = alignHeaders (Proxy :: Proxy row) hdrout hdr3 :: trans
        let trans4 = alignHeaders (Proxy :: Proxy row) hdrout hdr4 :: trans
        yield (AbstractResultSet (act trans3 rset3))
        yield (AbstractResultSet (act trans4 rset4)))))))

execQueryPlan  rset (QPPar2 qpd qp1 qp2)  =
  let hdr = getHeader rset
      hdrout = toHeader (toAscList (combinedvs qpd)) in
      return (AbstractResultSet (ResultSetResultStreamResultSet mempty hdrout (ResultStream (runResultStream (toResultStream rset) =$= awaitForever (\row -> do
        let rset2 = oneRowResultSet hdr row
        (rset3, rset4) <- liftIO $ concurrently (execQueryPlan rset2 qp1) (execQueryPlan rset2 qp2)
        let hdr3 = getHeader rset3
        let hdr4 = getHeader rset4
        let trans3 = alignHeaders (Proxy :: Proxy row) hdrout hdr3 :: trans
        let trans4 = alignHeaders (Proxy :: Proxy row) hdrout hdr4 :: trans
        yield (AbstractResultSet (act trans3 rset3))
        yield (AbstractResultSet (act trans4 rset4)))))))

execQueryPlan  r (QPOne2 _)  = return (AbstractResultSet r)
execQueryPlan  rset (QPZero2 qpd)  =
  let hdrout = toHeader (toAscList (combinedvs qpd)) in
      return (AbstractResultSet (ResultStreamResultSet mempty hdrout (ResultStream (runResultStream (toResultStream rset) =$= sinkNull))))
execQueryPlan  rs (QPAggregate2 qpd (FReturn vars) qp)  =
    execQueryPlan  rs qp  -- if combinedvs is calculated correctly, this should not need further transformation
execQueryPlan  rset (QPAggregate2 qpd Not qp)  =
  let hdr = getHeader rset
      hdrout = toHeader (toAscList (combinedvs qpd)) in
      return (AbstractResultSet (ResultStreamResultSet mempty hdrout (ResultStream (runResultStream (toResultStream rset) =$= awaitForever (\row -> do
          let rset2 = oneRowResultSet hdr row
          rset3 <- liftIO $ execQueryPlan rset2 qp
          let rs3 = toResultStream rset3
          b <- lift $ isResultStreamEmpty rs3
          when b $ yield row)))))
execQueryPlan  rset (QPAggregate2 qpd Exists qp)  =
  let hdr = getHeader rset
      hdrout = toHeader (toAscList (combinedvs qpd)) in
      return (AbstractResultSet (ResultStreamResultSet mempty hdrout (ResultStream (runResultStream (toResultStream rset) =$= awaitForever (\row -> do
          let rset2 = oneRowResultSet hdr row
          rset3 <- liftIO $ execQueryPlan rset2 qp
          let rs3 = toResultStream rset3
          b <- lift $ isResultStreamEmpty rs3
          when (not b) $ yield row)))))
execQueryPlan  rset (QPAggregate2 qpd (Summarize funcs groupby) qp) =
  let hdr = getHeader rset
      hdrout = toHeader (toAscList (combinedvs qpd)) in
      return (AbstractResultSet (ResultStreamResultSet mempty hdrout (ResultStream (runResultStream (toResultStream rset) =$= awaitForever (\row -> do
          let rset2 = oneRowResultSet hdr row
          rset3 <- liftIO $ execQueryPlan rset2 qp
          let rs3 = toResultStream rset3
          rows <- lift $ getAllResultsInStream rs3
          let groups = groupBy (\a b -> all (\var -> ext var hdr a == ext var hdr b) groupby) (rows :: [row])
          let updatess = map (\rows ->  map (\(v1, func1) ->
                        let m = case func1 of
                                  Max v2 ->
                                      if List.null rows
                                          then error "max of empty list"
                                          else maximum (map (fromMaybe (error "max empty value") . ext v2 hdr) rows)
                                  Min v2 ->
                                      if List.null rows
                                          then error "min of empty list"
                                          else minimum (map (fromMaybe (error "min empty value") . ext v2 hdr) rows)
                                  Sum v2 ->
                                      sum (map (fromMaybe (error "sum empty value") . ext v2 hdr) rows)
                                  Average v2 ->
                                      if List.null rows
                                          then error "average of empty list"
                                          else average (map (fromMaybe (error "average empty value") . ext v2 hdr) rows)
                                  Count ->
                                      fromIntegral (length rows)
                                  CountDistinct v2 ->
                                      fromIntegral (length (List.nub (concatMap (\row -> case ext v2 hdr row of
                                                                                      Nothing -> []
                                                                                      Just a -> [a]) rows)))
                                  Random v2 ->
                                      if List.null rows
                                          then error "random of empty list"
                                          else head (map (fromMaybe (error "average empty value") . ext v2 hdr) rows) in
                            (v1, m)) funcs) groups where
                                average :: Fractional b => [b] -> b
                                average n = let (s,c) = average2 n in s/fromIntegral c
                                average2 n = foldl' (\(s0, c0) a -> (s0 + a, c0 + 1)) (0,0) n

          sourceList  (map (\updates -> updateRow updates hdr row) updatess))))))
execQueryPlan rset (QPAggregate2 qpd (Limit n) qp)  =
  let hdr = getHeader rset
      hdrout = toHeader (toAscList (combinedvs qpd)) in
      return (AbstractResultSet (ResultStreamResultSet mempty hdrout (ResultStream (runResultStream (toResultStream rset) =$= awaitForever (\row -> do
          let rset2 = oneRowResultSet hdr row
          rset3 <- liftIO $ execQueryPlan rset2 qp
          let rs3 = toResultStream rset3
          runResultStream rs3 =$= do
            isolate n
            sinkNull)))))
execQueryPlan  rset (QPAggregate2 qpd Distinct qp)  =
  let hdr = getHeader rset
      hdrout = toHeader (toAscList (combinedvs qpd)) in
      return (AbstractResultSet (ResultStreamResultSet mempty hdrout (ResultStream (runResultStream (toResultStream rset) =$= awaitForever (\row -> do
          let rset2 = oneRowResultSet hdr row
          rset3 <- liftIO $ execQueryPlan rset2 qp
          let rs3 = toResultStream rset3
          l <- lift $ getAllResultsInStream rs3
          sourceList (nub l))))))
execQueryPlan  rset (QPAggregate2 qpd (OrderByAsc v1) qp)  =
  let hdr = getHeader rset
      hdrout = toHeader (toAscList (combinedvs qpd)) in
      return (AbstractResultSet (ResultStreamResultSet mempty hdrout (ResultStream (runResultStream (toResultStream rset) =$= awaitForever (\row -> do
          let rset2 = oneRowResultSet hdr row
          rset3 <- liftIO $ execQueryPlan rset2 qp
          let rs3 = toResultStream rset3
          rows <- lift $ getAllResultsInStream rs3
          let rows' = sortBy (\row1 row2 -> compare (ext v1 hdr row1) (ext v1 hdr row2)) rows
          sourceList rows')))))
execQueryPlan  rset (QPAggregate2 qpd (OrderByDesc v1) qp)  =
  let hdr = getHeader rset
      hdrout = toHeader (toAscList (combinedvs qpd)) in
      return (AbstractResultSet (ResultStreamResultSet mempty hdrout (ResultStream (runResultStream (toResultStream rset) =$= awaitForever (\row -> do
          let rset2 = oneRowResultSet hdr row
          rset3 <- liftIO $ execQueryPlan rset2 qp
          let rs3 = toResultStream rset3
          rows <- lift $ getAllResultsInStream rs3
          let rows' = sortBy (\row1 row2 -> comparing Down (ext v1 hdr row1) (ext v1 hdr row2)) rows
          sourceList rows')))))

closeQueryPlan :: QueryPlan3 trans row -> IO ()
closeQueryPlan (Exec2 _ (stmt, _)) =
    case stmt of
        AbstractDBStatement stmt0 -> dbStmtClose stmt0

closeQueryPlan (QPSequencing2 _  qp1 qp2) = do
  closeQueryPlan qp1
  closeQueryPlan qp2

closeQueryPlan (QPChoice2 _ qp1 qp2) = do
    closeQueryPlan qp1
    closeQueryPlan qp2

closeQueryPlan (QPPar2 _  qp1 qp2) = do
    closeQueryPlan qp1
    closeQueryPlan qp2

closeQueryPlan (QPOne2 _)  = return ()
closeQueryPlan (QPZero2 _) = return ()
closeQueryPlan (QPAggregate2 _ _ qp) =
    closeQueryPlan qp
