{-# LANGUAGE ExistentialQuantification, MultiParamTypeClasses, TypeSynonymInstances, FlexibleInstances, FlexibleContexts, TypeFamilies, ScopedTypeVariables #-}
module QueryPlan where

import DB.ResultStream
import FO.Data
import FO.Domain
import ListUtils
import Algebra.SemiBoundedLattice
import DB.DB

import Prelude  hiding (lookup, null)
import Data.Map.Strict (Map, empty, insert, lookup, intersectionWith, delete, singleton)
import Data.List (intercalate, find, elem, union, intersect, sortBy)
import qualified Data.List as List
import Control.Monad.Except
import Control.Applicative ((<|>))
import Data.Convertible.Base
import Data.Maybe
import Data.Monoid  ((<>))
import Data.Tree
import Data.Conduit
import Control.Monad.Trans.Control
import Control.Concurrent.Async.Lifted
import qualified Data.Text as T
import System.Log.Logger
import Algebra.Lattice
import Data.Set (Set, fromList, toAscList, null, isSubsetOf, member)
import Data.Ord (comparing, Down(..))
import Data.Dynamic
import Data.Typeable
import Control.Monad.Trans.Resource

type MSet a = Complemented (Set a)

data QueryPlan = Exec Formula Int
                | QPReturn [Var]
                | QPTransaction
                | QPPar QueryPlan QueryPlan
                | QPChoice QueryPlan QueryPlan
                | QPSequencing QueryPlan QueryPlan
                | QPZero
                | QPOne
                | QPAggregate Aggregator QueryPlan deriving Show


data QueryPlanData row  = QueryPlanData {
    linscopevs :: MSet Var,
    rinscopevs :: MSet Var,
    freevs :: Set Var,
    determinevs :: Set Var, -- determined vars
    paramvs :: Set Var, -- parameter vars
    returnvs :: Set Var, -- return vars
    combinedvs :: Set Var, -- combined return and available vars that are still in scope
    availablevs :: Set Var,
    query :: Maybe Dynamic,
    stmts :: Maybe (AbstractDBStatement row, String), -- stmt, show
    tdb :: Maybe [Int]
}

dqdb :: QueryPlanData row
dqdb = QueryPlanData top top bottom bottom bottom bottom bottom bottom Nothing Nothing Nothing

data QueryPlanNode2 row  = Exec2 Formula Int
                | QPReturn2 [Var]
                | QPTransaction2
                | QPChoice2 (QueryPlan2 row ) (QueryPlan2 row )
                | QPPar2 (QueryPlan2 row ) (QueryPlan2 row )
                | QPSequencing2 (QueryPlan2 row ) (QueryPlan2 row )
                | QPZero2
                | QPOne2
                | QPAggregate2 Aggregator (QueryPlan2 row )

type QueryPlan2 row  =  (QueryPlanData row , QueryPlanNode2 row )

class ToTree a where
    toTree :: a -> Tree String
instance Show (QueryPlanData row ) where
    show qp = "[" ++ show (availablevs qp) ++ "|" ++ show (linscopevs qp) ++ "|" ++ show (paramvs qp) ++ "|" ++ show (freevs qp) ++ "|" ++ show (determinevs qp) ++ "|"++ show (returnvs qp) ++ "|" ++ show (combinedvs qp) ++ "|" ++ show (rinscopevs qp) ++ "]"
instance ToTree (QueryPlan2 row ) where
    toTree (qpd, Exec2 f  dbs) = Node ("exec " ++ show f ++ " at " ++ show dbs ++ show qpd ) []
    toTree (qpd, QPReturn2 vars) = Node ("return "++ unwords (map show vars) ) []
    toTree (qpd, QPSequencing2 qp1 qp2) = Node ("sequencing"++ show qpd ) [toTree qp1, toTree qp2]
    toTree (qpd, QPChoice2 qp1 qp2) = Node ("choice"++ show qpd ) [toTree qp1, toTree qp2]
    toTree (qpd, QPPar2 qp1 qp2) = Node ("parallel"++ show qpd ) [toTree qp1, toTree qp2]
    toTree (qpd, QPTransaction2 ) = Node ("transaction" ++ show qpd) []
    toTree (qpd, QPZero2) = Node ("zero"++ show qpd ) []
    toTree (qpd, QPOne2) = Node ("one"++ show qpd ) []
    toTree (qpd, QPAggregate2 agg qp1) = Node ("aggregate " ++ show agg ++ " " ++ show qpd) [toTree qp1]

findDB pred0 dbs =
    let dbxs = filter (\x -> pred0 `elem` getPreds (dbs !! x)) [0..(length dbs-1)] in
        if List.null dbxs
            then error ("no database for predicate " ++ show pred0)
            else head dbxs

formulaToQueryPlan :: [AbstractDatabase row] -> Formula -> QueryPlan
formulaToQueryPlan dbs  form@(FAtomic (Atom pred0  _)) =
    Exec form  (findDB pred0 dbs)
formulaToQueryPlan _  (FReturn vars) =
    QPReturn vars
formulaToQueryPlan _ FOne = QPOne
formulaToQueryPlan _ FZero = QPZero
formulaToQueryPlan dbs  (FTransaction ) = QPTransaction
formulaToQueryPlan dbs  (FChoice form1 form2) = QPChoice (formulaToQueryPlan dbs form1) (formulaToQueryPlan dbs form2)
formulaToQueryPlan dbs  (FPar form1 form2) = QPPar (formulaToQueryPlan dbs form1) (formulaToQueryPlan dbs form2)
formulaToQueryPlan dbs  (FSequencing form1 form2) = QPSequencing (formulaToQueryPlan dbs form1) (formulaToQueryPlan dbs form2)
formulaToQueryPlan dbs  (Aggregate agg form) = QPAggregate agg (formulaToQueryPlan dbs  form)
formulaToQueryPlan dbs  ins@(FInsert (Lit _ (Atom pred1 _))) =
    let xs2 = findDB pred1 dbs in
        Exec ins xs2




{- near semi-ring -}
simplifyQueryPlan :: QueryPlan -> QueryPlan

simplifyQueryPlan qp@(Exec _ _) = qp
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
simplifyQueryPlan  (QPTransaction ) =
    QPTransaction
simplifyQueryPlan qp@(QPReturn _) = qp
simplifyQueryPlan  qp = qp

combineQPSequencingData :: QueryPlanData row -> QueryPlanData row -> QueryPlanData row
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
        query = Nothing,
        stmts = Nothing,
        tdb = Nothing
    }

combineQPChoiceData :: QueryPlanData row -> QueryPlanData row -> QueryPlanData row
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
        query = Nothing,
        stmts = Nothing,
        tdb = Nothing
    }

combineQPParData :: QueryPlanData row -> QueryPlanData row -> QueryPlanData row
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
        query = Nothing,
        stmts = Nothing,
        tdb = Nothing
    }

optimizeQueryPlan :: [AbstractDatabase row] -> QueryPlan2 row -> QueryPlan2 row
optimizeQueryPlan _ qp@(_, Exec2 _ _) = qp

optimizeQueryPlan dbsx (qpd, QPSequencing2 ip1  ip2) =
    let qp1' = optimizeQueryPlan dbsx ip1
        qp2' = optimizeQueryPlan dbsx ip2 in
        case (qp1', qp2') of
            ((_, Exec2 form1 x1), (_, Exec2 form2 x2)) ->
                let dbs = x1 == x2
                    fse = fsequencing [form1, form2]
                    dbs' = case dbsx !! x1 of (AbstractDatabase db) -> supported db (fse) (returnvs qpd) in
                    if dbs && dbs'
                            then (qpd, QPSequencing2 qp1' qp2')
                            else (qpd, Exec2 fse x1)
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
                    dbs' = case dbsx !! x1 of (AbstractDatabase db) -> supported db (fch) (returnvs qpd) in
                    if dbs && dbs' then
                        (qpd, QPChoice2 qp1' qp2')
                    else
                        (qpd, Exec2 fch x1)
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
                    dbs' = case dbsx !! x1 of (AbstractDatabase db) -> supported db (fch) (returnvs qpd) in
                    if dbs && dbs' then
                        (qpd, QPPar2 qp1' qp2')
                    else
                        (qpd, Exec2 fch x1)
            ((qpd1, Exec2 _ _), (_, QPPar2 qp21@(qpd21, _) qp22)) ->
                (qpd, QPPar2 (optimizeQueryPlan dbsx (combineQPParData qpd1 qpd21, QPPar2 qp1' qp21)) qp22)
            ((_, QPPar2 qp11 qp12@(qpd3, _)), (qpd2, Exec2 _ _)) ->
                (qpd, QPPar2 qp11 (optimizeQueryPlan dbsx (combineQPParData qpd3 qpd2, QPPar2 qp12 qp2')))
            ((qpd1, QPPar2 qp11 qp12@(qpd3, _)), (_, QPPar2 qp21@(qpd4, _) qp22)) ->
                (qpd, QPPar2 (combineQPParData qpd1 qpd4, QPPar2 qp11 (optimizeQueryPlan dbsx (combineQPParData qpd3 qpd4, QPPar2 qp12 qp21))) qp22)
            _ -> (qpd, QPPar2 qp1' qp2')
optimizeQueryPlan _ qp@(_, QPTransaction2) =
    qp
optimizeQueryPlan _ qp@(_, QPOne2) = qp
optimizeQueryPlan _ qp@(_, QPZero2) = qp
optimizeQueryPlan dbsx (qpd, QPAggregate2 agg qp1) =
    let qp1' = optimizeQueryPlan dbsx qp1 in
        case qp1' of
            (_, Exec2 formula1 x) ->
                        let exi = Aggregate agg formula1
                            dbs' = case dbsx !! x of (AbstractDatabase db) -> supported db exi (returnvs qpd)  in
                            if dbs' then
                                (qpd, QPAggregate2 agg qp1')
                            else
                                (qpd, Exec2 exi  x)
            _ -> (qpd, QPAggregate2 agg qp1')
optimizeQueryPlan _ qp@(_, QPReturn2 _) = qp


domainSizeFormula :: Set Var -> AbstractDatabase row -> Formula -> (Set Var)
domainSizeFormula vars (AbstractDatabase db_) form =
        let map1' = determinedVars (determinateVars db_ ) vars form in
            (vars \/ map1')

checkQueryPlan :: [AbstractDatabase row] -> QueryPlan2 row -> Except String ()
checkQueryPlan dbs (qpd, Exec2 form x) = do
    let par0 = paramvs qpd
        det0 = determinevs qpd
        map2 = domainSizeFormula (par0) (dbs !! x) form
    if det0 `isSubsetOf` map2
        then return ()
        else throwError ("checkQueryPlan: unbounded vars: " ++ show (det0 \\\ map2) ++ ", the formula is " ++ show form)

checkQueryPlan dbs (_, QPChoice2 qp1 qp2) = do
    checkQueryPlan dbs qp1
    checkQueryPlan dbs qp2

checkQueryPlan dbs (_, QPPar2 qp1 qp2) = do
    checkQueryPlan dbs qp1
    checkQueryPlan dbs qp2

checkQueryPlan dbs (_, QPSequencing2 qp1 qp2) = do
    checkQueryPlan dbs qp1
    checkQueryPlan dbs qp2

checkQueryPlan _ (_, QPOne2) = do
    return ()

checkQueryPlan _ (_, QPZero2) = do
    return ()

checkQueryPlan dbs   (_, QPAggregate2 Not qp) = do
    checkQueryPlan dbs qp

checkQueryPlan dbs  (_, QPAggregate2 Exists qp) = do
    checkQueryPlan dbs qp

checkQueryPlan dbs   (_, QPAggregate2 (Summarize funcs) qp@(qpd, _)) = do
    checkQueryPlan dbs qp
    let det = determinevs qpd
    mapM_ (\(_, func) ->
        case func of
            Max var2 ->
                if var2 `member` det
                    then return ()
                    else throwError ("checkQueryPlan: unbounded vars: " ++ show var2)
            Min var2 ->
                if var2 `member` det
                    then return ()
                    else throwError ("checkQueryPlan: unbounded vars: " ++ show var2)
            Count ->
                return ()) funcs

checkQueryPlan dbs   (_, QPAggregate2 (Limit _) qp) = do
    checkQueryPlan dbs qp

checkQueryPlan dbs   (_, QPAggregate2 (OrderByAsc _) qp) = do
    checkQueryPlan dbs qp

checkQueryPlan dbs   (_, QPAggregate2 (OrderByDesc _) qp) = do
    checkQueryPlan dbs qp

checkQueryPlan _ (qpd, QPReturn2 vars) = do
    let par0 = availablevs qpd
    if null (fromList vars \\\ par0)
        then return ()
        else throwError ("checkQueryPlan': unbounded vars: " ++ show (fromList vars \\\ par0) ++ " " ++ show (FReturn vars))

checkQueryPlan dbs (_, QPTransaction2) =
    return ()


calculateVars :: Set Var -> MSet Var -> QueryPlan -> QueryPlan2 row
calculateVars lvars rvars qp = calculateVars2 lvars (calculateVars1 rvars qp)

calculateVars1 :: MSet Var -> QueryPlan -> QueryPlan2 row
calculateVars1  rvars (Exec form dbxs) =
    let fvs = freeVars form in
        ( dqdb{freevs =  fvs, determinevs =  fvs, linscopevs = Include fvs \/ rvars, rinscopevs = rvars}, (Exec2  form dbxs))

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
calculateVars1 rvars  (QPAggregate agg@(Summarize funcs) qp1) =
    let qp1'@(qpd1, _) = calculateVars1 rvars qp1 in
        ( dqdb{freevs = freevs qpd1, determinevs = fromList (fst (unzip funcs)), linscopevs = linscopevs qpd1, rinscopevs = rvars} , (QPAggregate2 agg qp1'))
calculateVars1 rvars  (QPAggregate agg@(Limit _) qp1) =
    let qp1'@(qpd1, _) = calculateVars1 rvars qp1 in
        ( dqdb{freevs = freevs qpd1, determinevs = determinevs qpd1, linscopevs = linscopevs qpd1, rinscopevs = rinscopevs qpd1} , (QPAggregate2 agg qp1'))
calculateVars1 rvars  (QPAggregate agg@(OrderByAsc _) qp1) =
    let qp1'@(qpd1, _) = calculateVars1 rvars qp1 in
        ( dqdb{freevs = freevs qpd1, determinevs = determinevs qpd1, linscopevs = linscopevs qpd1, rinscopevs = rinscopevs qpd1} , (QPAggregate2 agg qp1'))
calculateVars1 rvars  (QPAggregate agg@(OrderByDesc _) qp1) =
    let qp1'@(qpd1, _) = calculateVars1 rvars qp1 in
        ( dqdb{freevs = freevs qpd1, determinevs = determinevs qpd1, linscopevs = linscopevs qpd1, rinscopevs = rinscopevs qpd1} , (QPAggregate2 agg qp1'))

calculateVars1 rvars  (QPReturn vars) =
    (dqdb{
        freevs = fromList vars,
        determinevs = bottom,
        linscopevs = rvars /\ Include (fromList vars),
        rinscopevs = rvars /\ Include (fromList vars)
        } , QPReturn2 vars)
calculateVars1 rvars  (QPTransaction) =
        (dqdb{freevs = bottom, determinevs = bottom, linscopevs = rvars, rinscopevs = rvars} , QPTransaction2)



calculateVars2 :: Set Var -> QueryPlan2 row  -> QueryPlan2 row
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
calculateVars2 lvars  (qpd, QPReturn2 vars) = (dqdb{availablevs = lvars, paramvs = lvars /\ fromList vars, returnvs = rinscopevs qpd //\ (lvars /\ fromList vars), combinedvs = rinscopevs qpd //\ (lvars /\ fromList vars)} , QPReturn2 vars)
calculateVars2 lvars  (qpd, QPTransaction2) =
    (qpd{availablevs = lvars, paramvs = bottom, returnvs = bottom,combinedvs = rinscopevs qpd //\ lvars} , QPTransaction2)



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

prepareTransaction :: (IResultRow row) => [Int] -> QueryPlan2 row  -> ([Int], QueryPlan2 row )
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
prepareTransaction rdbxs qp@(qpd, QPTransaction2) =
    (rdbxs, (qpd{tdb = Just rdbxs }, QPTransaction2))
prepareTransaction rdbxs qp@(_, QPReturn2 _) =  (rdbxs, qp)

translateQueryPlan :: (IResultRow row) => [AbstractDatabase row] -> QueryPlan2 row  -> (QueryPlan2 row )

translateQueryPlan dbs (qpd, e@(Exec2  form x)) =
    if x >= length dbs || x < 0 then
        error "index out of range"
    else case dbs !! x of
        AbstractDatabase db ->
            let vars = paramvs qpd
                vars2 = returnvs qpd
                qu = Query form
                qu' = toDyn (translateQuery db vars2 qu vars) in
            (qpd {query = Just qu'}, e)
translateQueryPlan dbs  (qpd, QPChoice2 qp1 qp2) =
    let qp1' = translateQueryPlan dbs  qp1
        qp2' = translateQueryPlan dbs  qp2 in
        (qpd, QPChoice2 qp1' qp2')
translateQueryPlan dbs  (qpd, QPPar2 qp1 qp2) =
    let qp1' = translateQueryPlan dbs  qp1
        qp2' = translateQueryPlan dbs  qp2 in
        (qpd, QPPar2 qp1' qp2')
translateQueryPlan dbs  (qpd, QPSequencing2 qp1 qp2) =
    let qp1' = translateQueryPlan dbs  qp1
        qp2' = translateQueryPlan dbs  qp2 in
        (qpd, QPSequencing2 qp1' qp2')
translateQueryPlan _ qp@(_, QPOne2) = qp
translateQueryPlan _ qp@(_, QPZero2) = qp
translateQueryPlan dbs  (qpd, QPAggregate2 agg qp1) =
    let qp1' = translateQueryPlan dbs  qp1 in
        (qpd, QPAggregate2 agg qp1')
translateQueryPlan _ qp@(_, QPReturn2 _) = qp
translateQueryPlan dbs qp@(_, QPTransaction2) = qp

prepareQueryPlan :: (IResultRow row) => [AbstractDBConnection row] -> QueryPlan2 row  -> IO (QueryPlan2 row )
prepareQueryPlan dbs (qpd, e@(Exec2  form x)) =
    if x >= length dbs || x < 0 then
        error "index out of range"
    else case dbs !! x of
        AbstractDBConnection db -> do
            let vars = paramvs qpd
                vars2 = returnvs qpd
                qu = fromDyn  (fromMaybe (error "query not translated") (query qpd)) (error "prepareQueryPlan: error converting dynamic")
                stmtshow = show qu
            stmt <- prepareQuery db vars2 qu vars
            return (qpd {stmts = Just (AbstractDBStatement stmt, stmtshow)}, e)
prepareQueryPlan dbs  (qpd, QPChoice2 qp1 qp2) = do
    qp1' <- prepareQueryPlan dbs  qp1
    qp2' <- prepareQueryPlan dbs  qp2
    return (qpd, QPChoice2 qp1' qp2')
prepareQueryPlan dbs  (qpd, QPPar2 qp1 qp2) = do
    qp1' <- prepareQueryPlan dbs  qp1
    qp2' <- prepareQueryPlan dbs  qp2
    return (qpd, QPPar2 qp1' qp2')
prepareQueryPlan dbs  (qpd, QPSequencing2 qp1 qp2) = do
    qp1' <- prepareQueryPlan dbs  qp1
    qp2' <- prepareQueryPlan dbs  qp2
    return (qpd, QPSequencing2 qp1' qp2')
prepareQueryPlan _ qp@(_, QPOne2) = return qp
prepareQueryPlan _ qp@(_, QPZero2) = return qp
prepareQueryPlan dbs  (qpd, QPAggregate2 agg qp1) = do
    qp1' <- prepareQueryPlan dbs  qp1
    return (qpd, QPAggregate2 agg qp1')
prepareQueryPlan _ qp@(_, QPReturn2 _) = return qp
prepareQueryPlan dbs qp@(_, QPTransaction2) =
    return qp

addCleanupRS :: Monad m => (Bool -> m ()) -> ResultStream m row -> ResultStream m row
addCleanupRS a (ResultStream rs) = ResultStream (addCleanup a rs)

execQueryPlan :: (IResultRow row, Ord (ElemType row), Num (ElemType row)) => [AbstractDBConnection row] -> ([Var], ResultStream (ResourceT IO) row) -> QueryPlan2 row  ->   ([Var], ResultStream (ResourceT IO) row     )
execQueryPlan conns (vars, rs) (qpd, Exec2 _ _) = do
    let (stmt, stmtshow) = fromJust (stmts qpd)
    case stmt of
        AbstractDBStatement stmt0 -> do
            (toAscList (combinedvs qpd), bracketPStream (return ()) (\_ -> dbStmtClose stmt0) (\_ -> do
                        row <- rs
                        liftIO $ infoM "QA" ("current row " ++ show row)
                        liftIO $ infoM "QA" ("execute " ++ stmtshow)
                        row2 <- dbStmtExec stmt0 vars (pure row)
                        liftIO $ infoM "QA" ("returns row")
                        return (transform vars (toAscList (combinedvs qpd)) (row <> row2))))

execQueryPlan conns r (qpd, QPSequencing2 qp1 qp2) =
    (toAscList (combinedvs qpd), do
        let r1 = execQueryPlan conns r qp1
            (vars2, rs2) = execQueryPlan conns r1 qp2 in
            transformResultStream vars2 (toAscList (combinedvs qpd)) rs2)

execQueryPlan conns (vars, rs) (qpd, QPChoice2 qp1 qp2) =
    (toAscList (combinedvs qpd), do
        row <- rs
        let (vars1, rs1) = execQueryPlan conns (vars, pure row) qp1
            (vars2, rs2) = execQueryPlan conns (vars, pure row) qp2 in
            transformResultStream vars1 (toAscList (combinedvs qpd)) rs1 <|> transformResultStream vars2 (toAscList (combinedvs qpd)) rs2)

execQueryPlan conns (vars, rs) (qpd, QPPar2 qp1 qp2) =
    (toAscList (combinedvs qpd), do
        row <- rs
        let (vars1, rs1) = execQueryPlan conns (vars, pure row) qp1
            (vars2, rs2) = execQueryPlan conns (vars, pure row) qp2
        (rs1', rs2') <- lift $ concurrently (getAllResultsInStream rs1) (getAllResultsInStream rs2)
        transformResultStream vars1 (toAscList (combinedvs qpd)) (listResultStream rs1') <|> transformResultStream vars2 (toAscList (combinedvs qpd)) (listResultStream rs2'))

execQueryPlan conns (vars, rs) (qpd, QPTransaction2 ) =
    (toAscList (combinedvs qpd), do
        let (begin, prepare, commit, rollback) = case tdb qpd of
                Nothing -> error "execQueryPlan: tranaction without db"
                Just dbs ->
                    (mapM_ (\db -> dbBegin (conns !! db)) dbs,
                    mapM (\db -> dbPrepare (conns !! db)) dbs,
                    mapM (\db -> dbCommit (conns !! db)) dbs,
                    mapM_ (\db -> dbRollback (conns !! db)) dbs)
        row <- rs
        let commitCleanup = do
                b' <- prepare
                if and b'
                    then do
                        b'' <- commit
                        if and b''
                            then return ()
                            else do
                                rollback
                                liftIO $ errorM "QA" ("execQueryPlan: commit failed " ++ show b'')
                    else do
                        rollback
                        liftIO $ errorM "QA" ("execQueryPlan: prepare failed rollback " ++ show b')
        let rollbackCleanup = do
                rollback
                liftIO $ errorM "QA" ("execQueryPlan: stream terminated rollback ")
        liftIO begin
        ResultStream (do
            yieldOr row (liftIO rollbackCleanup)
            liftIO commitCleanup))

execQueryPlan conns r (_, QPOne2) = r
execQueryPlan conns (vars', rs) (_, QPReturn2 vars) = (vars, transformResultStream vars' vars rs)
execQueryPlan conns (vars, rs) (_, QPZero2) = (vars, closeResultStream rs)
execQueryPlan conns (vars, rs) (_, QPAggregate2 Not qp) = -- assume no unbounded vars under not
            (vars, filterResultStream rs (\row -> do
                let (_, rs2) = execQueryPlan conns (vars, (pure row)) qp
                isResultStreamEmpty rs2))
execQueryPlan conns (vars, rs) (_, QPAggregate2 Exists qp) = -- assume no unbounded vars under exists
            (vars, filterResultStream rs (\row -> do
                let (_, rs2) = execQueryPlan conns (vars, (pure row)) qp
                emp <- isResultStreamEmpty rs2
                return (not emp)))
execQueryPlan conns (vars, rs) (_, QPAggregate2 (Summarize funcs) qp) = -- assume no unbounded vars under not
            (vars, mapResultStream rs (\row -> do
                let (vars2, rs2) = execQueryPlan conns (vars, (pure row)) qp
                rows <- getAllResultsInStream rs2
                let rows2 = map (\(v1, func1) ->
                              let m = case func1 of
                                        Max v2 ->
                                            if length rows == 0
                                                then error "max of empty list"
                                                else foldl1 max (map (ext v2) rows)
                                        Min v2 ->
                                            if length rows == 0
                                                then error "min of empty list"
                                                else foldl1 min (map (ext v2) rows)
                                        Count ->
                                            fromInteger (toInteger (length rows)) in
                                  ret v1 m) funcs

                return (mconcat (reverse rows2) <> row)))
execQueryPlan conns (vars, rs) (_, QPAggregate2 (Limit n) qp) = -- assume no unbounded vars under exists
            (vars, do
                row <- rs
                let (_, rs2) = execQueryPlan conns (vars, (pure row)) qp
                takeResultStream n rs)
execQueryPlan conns (vars, rs) (_, QPAggregate2 (OrderByAsc v1) qp) = -- assume no unbounded vars under not
            (vars, do
                row <- rs
                let (_, rs2) = execQueryPlan conns (vars, (pure row)) qp
                rows <- lift $ getAllResultsInStream rs2
                let rows' = sortBy (\row1 row2 -> compare (ext v1 row1) (ext v1 row2)) rows
                listResultStream rows')
execQueryPlan conns (vars, rs) (_, QPAggregate2 (OrderByDesc v1) qp) = -- assume no unbounded vars under exists
            (vars, do
                row <- rs
                let (_, rs2) = execQueryPlan conns (vars, (pure row)) qp
                rows <- lift $ getAllResultsInStream rs2
                let rows' = sortBy (\row1 row2 -> comparing Down (ext v1 row1) (ext v1 row2)) rows
                listResultStream rows')
