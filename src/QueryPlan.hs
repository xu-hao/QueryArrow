{-# LANGUAGE ExistentialQuantification, MultiParamTypeClasses, FunctionalDependencies, TypeSynonymInstances, FlexibleInstances, FlexibleContexts, TypeFamilies, ScopedTypeVariables #-}
module QueryPlan where

import ResultStream
import FO.Data
import FO.Domain
import ListUtils
import Algebra.SemiBoundedLattice

import Prelude  hiding (lookup, null)
import Data.Map.Strict (Map, empty, insert, lookup, intersectionWith, delete)
import Data.List (intercalate, find, elem, union, intersect)
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
import Data.Set (Set, fromList, toAscList, singleton, null, isSubsetOf)

type MSet a = Complemented (Set a)

-- result value
data ResultValue = StringValue T.Text | IntValue Int | Null deriving (Eq , Show)

instance Convertible ResultValue Expr where
    safeConvert (StringValue s) = Right (StringExpr s)
    safeConvert (IntValue i) = Right (IntExpr i)
    safeConvert v = Left (ConvertError (show v) "ResultValue" "Expr" "")

instance Convertible Expr ResultValue where
    safeConvert (StringExpr s) = Right (StringValue s)
    safeConvert (IntExpr i) = Right (IntValue i)
    safeConvert v = Left (ConvertError (show v) "Expr" "ResultValue" "")

-- result row
type MapResultRow = Map Var ResultValue

instance ResultRow MapResultRow where
    transform _ vars2 map1 = foldr (\var map2 -> case lookup var map1 of
                                                        Nothing -> insert var Null map2
                                                        Just rv -> insert var rv map2) empty vars2

class SubstituteResultValue a where
    substResultValue :: MapResultRow -> a -> a

instance SubstituteResultValue Expr where
    substResultValue varmap expr@(VarExpr var) = case lookup var varmap of
        Nothing -> expr
        Just res -> convert res
    substResultValue _ expr = expr

instance SubstituteResultValue Atom where
    substResultValue varmap (Atom thesign theargs) = Atom thesign newargs where
        newargs = map (substResultValue varmap) theargs
instance SubstituteResultValue Lit where
    substResultValue varmap (Lit thesign theatom) = Lit thesign newatom where
        newatom = substResultValue varmap theatom


-- query
newtype Query = Query Formula

checkQuery :: Query -> Except String ()
checkQuery (Query form) = checkFormula form

instance Show Query where
    show (Query  disjs) = show disjs


class DBStatementExec m row stmt where
    dbStmtExec :: stmt -> [Var] -> ResultStream (m) (row) -> ResultStream (m) (row)

class DBStatementClose m stmt where
    dbStmtClose :: stmt -> m ()

-- database
class (Monad m, DBStatementClose m stmt, DBStatementExec m row stmt) => Database_ db m row stmt | db -> m row stmt where
    dbBegin :: db -> m ()
    dbPrepare :: db -> m Bool
    dbCommit :: db -> m Bool
    dbRollback :: db -> m ()
    getName :: db -> String
    getPreds :: db -> [Pred]
    -- determinateVars function is a function from a given list of determined vars to vars determined by this atom
    determinateVars :: db -> Set Var -> Atom -> m (Set Var)
    prepareQuery :: db -> Set Var -> Query -> Set Var -> m stmt
    supported :: db -> Formula -> Set Var -> Bool
    translateQuery :: db -> Set Var -> Query -> Set Var -> (String, [Var])

doQuery :: Database_ db m row stmt => db -> Set Var -> Query -> [Var] -> ResultStream (m) (row) -> ResultStream (m) (row)
doQuery db vars2 qu vars rs = do
        stmt <- lift $ prepareQuery db vars2 qu (fromList vars)
        dbStmtExec stmt vars rs


-- https://wiki.haskell.org/Existential_type#Dynamic_dispatch_mechanism_of_OOP
data Database m row = forall db stmt. (Database_ db m row stmt) => Database { unDatabase :: db }

data QueryPlan = Exec Formula [Int]
                | QPReturn [Var]
                | QPTransaction
                | QPPar QueryPlan QueryPlan
                | QPChoice QueryPlan QueryPlan
                | QPSequencing QueryPlan QueryPlan
                | QPZero
                | QPOne
                | QPNot QueryPlan
                | QPExists Var QueryPlan deriving Show


data AbstractDBStatement m row = forall stmt. (DBStatementClose m stmt, DBStatementExec m row stmt) => AbstractDBStatement {unAbstractDBStatement :: stmt}

data QueryPlanData m row  = QueryPlanData {
    linscopevs :: MSet Var,
    rinscopevs :: MSet Var,
    freevs :: Set Var,
    determinevs :: Set Var, -- determined vars
    paramvs :: Set Var, -- parameter vars
    returnvs :: Set Var, -- return vars
    combinedvs :: Set Var, -- combined return and available vars that are still in scope
    availablevs :: Set Var,
    stmts :: Maybe [(AbstractDBStatement m row, String)], -- stmt, show
    tdb :: Maybe [Database m row]
}

dqdb :: QueryPlanData m row
dqdb = QueryPlanData top top bottom bottom bottom bottom bottom bottom Nothing Nothing

data QueryPlanNode2 m row  = Exec2 Formula [Int]
                | QPReturn2 [Var]
                | QPTransaction2
                | QPChoice2 (QueryPlan2 m row ) (QueryPlan2 m row )
                | QPPar2 (QueryPlan2 m row ) (QueryPlan2 m row )
                | QPSequencing2 (QueryPlan2 m row ) (QueryPlan2 m row )
                | QPZero2
                | QPOne2
                | QPNot2 (QueryPlan2 m row )
                | QPExists2 Var (QueryPlan2 m row )

type QueryPlan2 m row  =  (QueryPlanData m row , QueryPlanNode2 m row )

class ToTree a where
    toTree :: a -> Tree String
instance Show (QueryPlanData m row ) where
    show qp = "[" ++ show (availablevs qp) ++ "|" ++ show (linscopevs qp) ++ "|" ++ show (paramvs qp) ++ "|" ++ show (freevs qp) ++ "|" ++ show (determinevs qp) ++ "|"++ show (returnvs qp) ++ "|" ++ show (combinedvs qp) ++ "|" ++ show (rinscopevs qp) ++ "]"
instance ToTree (QueryPlan2 m row ) where
    toTree (qpd, Exec2 f  dbs) = Node ("exec " ++ show f ++ " at " ++ show dbs ++ show qpd ) []
    toTree (qpd, QPReturn2 vars) = Node ("return "++ unwords (map show vars) ) []
    toTree (qpd, QPSequencing2 qp1 qp2) = Node ("sequencing"++ show qpd ) [toTree qp1, toTree qp2]
    toTree (qpd, QPChoice2 qp1 qp2) = Node ("choice"++ show qpd ) [toTree qp1, toTree qp2]
    toTree (qpd, QPPar2 qp1 qp2) = Node ("parallel"++ show qpd ) [toTree qp1, toTree qp2]
    toTree (qpd, QPTransaction2 ) = Node ("transaction" ++ show qpd) []
    toTree (qpd, QPZero2) = Node ("zero"++ show qpd ) []
    toTree (qpd, QPOne2) = Node ("one"++ show qpd ) []
    toTree (qpd, QPNot2 qp1) = Node ("not" ++ show qpd) [toTree qp1]
    toTree (qpd, QPExists2 v qp1) = Node ("exists " ++ show v ++ " where" ++ show qpd) [toTree qp1]

findDB pred0 dbs = filter (\x -> case dbs !! x of
                            Database db -> pred0 `elem` getPreds db) [0..(length dbs-1)]

formulaToQueryPlan :: (Monad m)=> [Database m row] -> Formula -> QueryPlan
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
formulaToQueryPlan dbs  (Not form) = QPNot (formulaToQueryPlan dbs  form)
formulaToQueryPlan dbs  (Exists v form) = QPExists v (formulaToQueryPlan dbs  form)
formulaToQueryPlan dbs  ins@(FInsert (Lit _ (Atom pred1 _))) =
    let xs2 = findDB pred1 dbs
        xs3 = (filter (\x ->
            case dbs !! x of
                 Database db ->
                    supported db ins bottom) xs2) in
        Exec ins xs3




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
simplifyQueryPlan  (QPExists v qp1) =
    let qp1' = simplifyQueryPlan  qp1 in
        QPExists v qp1'
simplifyQueryPlan  (QPNot qp1) =
    let qp1' = simplifyQueryPlan  qp1 in
        case qp1' of
            QPOne -> QPZero
            QPZero -> QPOne
            _ -> QPNot qp1'
simplifyQueryPlan  (QPTransaction ) =
    QPTransaction
simplifyQueryPlan qp@(QPReturn _) = qp
simplifyQueryPlan  qp = qp

combineQPSequencingData :: QueryPlanData  m row -> QueryPlanData m row -> QueryPlanData m row
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
        stmts = Nothing,
        tdb = Nothing
    }

combineQPChoiceData :: QueryPlanData m row -> QueryPlanData m row -> QueryPlanData m row
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
        stmts = Nothing,
        tdb = Nothing
    }

combineQPParData :: QueryPlanData m row -> QueryPlanData m row -> QueryPlanData m row
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
        stmts = Nothing,
        tdb = Nothing
    }

optimizeQueryPlan :: (Monad m ) => [Database m row] -> QueryPlan2 m row -> QueryPlan2 m row
optimizeQueryPlan _ qp@(_, Exec2 _ _) = qp

optimizeQueryPlan dbsx (qpd, QPSequencing2 ip1  ip2) =
    let qp1' = optimizeQueryPlan dbsx ip1
        qp2' = optimizeQueryPlan dbsx ip2 in
        case (qp1', qp2') of
            ((_, Exec2 form1 dbs1), (_, Exec2 form2 dbs2)) ->
                let dbs = dbs1 `intersect` dbs2
                    fse = fsequencing [form1, form2]
                    dbs' = filter (\x -> case dbsx !! x of (Database db) -> supported db (fse) (returnvs qpd)) dbs in
                    if List.null dbs'
                            then (qpd, QPSequencing2 qp1' qp2')
                            else (qpd, Exec2 fse dbs')
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
            ((_, Exec2 formula1 dbs1), (_, Exec2 formula2 dbs2)) ->
                let dbs = dbs1 `intersect` dbs2
                    fch = fchoice [formula1, formula2]
                    dbs' = filter (\x -> case dbsx !! x of (Database db) -> supported db (fch) (returnvs qpd)) dbs in
                    if List.null dbs' then
                        (qpd, QPChoice2 qp1' qp2')
                    else
                        (qpd, Exec2 fch dbs')
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
            ((_, Exec2 formula1 dbs1), (_, Exec2 formula2 dbs2)) ->
                let dbs = dbs1 `intersect` dbs2
                    fch = fchoice [formula1, formula2]
                    dbs' = filter (\x -> case dbsx !! x of (Database db) -> supported db (fch) (returnvs qpd)) dbs in
                    if List.null dbs' then
                        (qpd, QPPar2 qp1' qp2')
                    else
                        (qpd, Exec2 fch dbs')
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
optimizeQueryPlan dbsx (qpd, QPExists2 v qp1) =
    let qp1' = optimizeQueryPlan dbsx qp1 in
        case qp1' of
            (_, Exec2 formula1 dbs) ->
                        let exi = (Exists v formula1)
                            dbs' = filter (\x -> case dbsx !! x of (Database db) -> supported db (exi) (returnvs qpd)) dbs  in
                            if List.null dbs' then
                                (qpd, QPExists2 v qp1')
                            else
                                (qpd, Exec2 exi  dbs')
            _ -> (qpd, QPExists2 v qp1')
optimizeQueryPlan dbsx (qpd, QPNot2 qp1) =
    let qp1' = optimizeQueryPlan dbsx qp1 in
        case qp1' of
            (_, Exec2 formula1 dbs) ->
                        let notform = Not formula1
                            dbs' = filter (\x -> case dbsx !! x of (Database db) -> supported db (notform) (returnvs qpd)) dbs  in
                            if List.null dbs' then
                                (qpd, QPNot2 qp1')
                            else
                                (qpd, Exec2 notform  dbs')
            _ -> (qpd, QPNot2 qp1')
optimizeQueryPlan _ qp@(_, QPReturn2 _) = qp


domainSizeFormula :: (Monad m) => Set Var -> Database m row -> Formula -> m (Set Var)
domainSizeFormula vars (Database db_) form = do
        map1' <- determinedVars (determinateVars db_ ) vars form
        return (vars \/ map1')

checkQueryPlan :: (Monad m ) => [Database m row] -> QueryPlan2 m row -> ExceptT (String, Formula) m ()
checkQueryPlan _ (_, Exec2 form []) = throwError ("no database", form)
checkQueryPlan dbs (qpd, Exec2 form (x : _)) = do
    let par0 = paramvs qpd
    let det0 = determinevs qpd
    map2 <- lift (domainSizeFormula (par0) (dbs !! x) form)
    if det0 `isSubsetOf` map2
        then return ()
        else throwError ("checkQueryPlan: unbounded vars: " ++ show (det0 \\\ map2) ++ ", the formula is " ++ show form, form)

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

checkQueryPlan dbs   (_, QPNot2 qp) = do
    checkQueryPlan dbs qp

checkQueryPlan dbs  (_, QPExists2 _ qp) = do
    checkQueryPlan dbs qp

checkQueryPlan _ (qpd, QPReturn2 vars) = do
    let par0 = availablevs qpd
    if null (fromList vars \\\ par0)
        then return ()
        else throwError ("checkQueryPlan': unbounded vars: " ++ show (fromList vars \\\ par0),  FReturn vars)

checkQueryPlan dbs (_, QPTransaction2) =
    return ()


calculateVars :: Set Var -> MSet Var -> QueryPlan -> QueryPlan2 m row
calculateVars lvars rvars qp = calculateVars2 lvars (calculateVars1 rvars qp)

calculateVars1 :: MSet Var -> QueryPlan -> QueryPlan2 m row
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
calculateVars1 rvars  (QPExists v qp1) =
    let qp1'@(qpd1, _) = calculateVars1 rvars qp1 in
        ( dqdb{freevs = freevs qpd1 \\\ singleton v, determinevs = determinevs qpd1 \\\ singleton v, linscopevs = linscopevs qpd1 \\\ Include (singleton v), rinscopevs = rinscopevs qpd1} , (QPExists2 v qp1'))

calculateVars1 rvars  (QPNot qp1) =
    let qp1'@(qpd1, _) = calculateVars1 rvars qp1 in
        ( dqdb{freevs = freevs qpd1, determinevs = bottom, linscopevs = rvars, rinscopevs = rvars} , (QPNot2  qp1'))
calculateVars1 rvars  (QPReturn vars) =
    (dqdb{
        freevs = fromList vars,
        determinevs = bottom,
        linscopevs = rvars /\ Include (fromList vars),
        rinscopevs = rvars /\ Include (fromList vars)
        } , QPReturn2 vars)
calculateVars1 rvars  (QPTransaction) =
        (dqdb{freevs = bottom, determinevs = bottom, linscopevs = rvars, rinscopevs = rvars} , QPTransaction2)



calculateVars2 :: Set Var -> QueryPlan2 m row  -> QueryPlan2 m row
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
calculateVars2 lvars  (qpd, QPExists2 v qp1) =
            let qp1' = calculateVars2 (lvars \\\ singleton v) qp1 in
                ( qpd{availablevs = lvars, paramvs = lvars /\ (freevs qpd), returnvs = (rinscopevs qpd //\ determinevs qpd) \\\ lvars,combinedvs = rinscopevs qpd //\ (determinevs qpd \/ lvars)} , (QPExists2 v qp1'))

calculateVars2 lvars  (qpd, QPNot2 qp1) =
            let qp1' = calculateVars2 lvars qp1 in
                ( qpd{availablevs = lvars, paramvs = lvars /\ (freevs qpd), returnvs = (rinscopevs qpd //\ determinevs qpd) \\\ lvars,combinedvs = rinscopevs qpd //\ (determinevs qpd \/ lvars)} , (QPNot2  qp1'))
calculateVars2 lvars  (qpd, QPReturn2 vars) = (dqdb{availablevs = lvars, paramvs = lvars /\ fromList vars, returnvs = rinscopevs qpd //\ (lvars /\ fromList vars), combinedvs = rinscopevs qpd //\ (lvars /\ fromList vars)} , QPReturn2 vars)
calculateVars2 lvars  (qpd, QPTransaction2) =
    (qpd{availablevs = lvars, paramvs = bottom, returnvs = bottom,combinedvs = rinscopevs qpd //\ lvars} , QPTransaction2)



addTransaction' :: QueryPlan2 m row -> QueryPlan2 m row
addTransaction' = snd . addTransaction

addTransaction :: QueryPlan2 m row -> (Bool, QueryPlan2 m row )
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


prepareTransaction :: (Monad m, ResultRow row) => [Database m row] -> [Int] -> QueryPlan2 m row  -> m ([Int], QueryPlan2 m row )
prepareTransaction  _ _ (_, (Exec2  form [])) = error ("prepareQueryPlan: Exec2: no database" ++ show form)
prepareTransaction _ rdbxs qp@(_, (Exec2 _ (x : _))) =
    return (x : rdbxs, qp)
prepareTransaction dbs rdbxs (qpd, QPChoice2 qp1 qp2) = do
    (dbxs1, qp1') <- prepareTransaction dbs rdbxs qp1
    (dbxs2, qp2') <- prepareTransaction dbs rdbxs qp2
    return (dbxs1 `union` dbxs2, (qpd, QPChoice2 qp1' qp2'))
prepareTransaction dbs rdbxs (qpd, QPPar2 qp1 qp2) = do
    (dbxs1, qp1') <- prepareTransaction dbs rdbxs qp1
    (dbxs2, qp2') <- prepareTransaction dbs rdbxs qp2
    return (dbxs1 `union` dbxs2, (qpd, QPPar2 qp1' qp2'))
prepareTransaction dbs rdbxs (qpd, QPSequencing2 qp1 qp2) = do
    (dbxs2, qp2') <- prepareTransaction dbs rdbxs qp2
    (dbxs1, qp1') <- prepareTransaction dbs dbxs2 qp1
    return (dbxs1, (qpd, QPSequencing2 qp1' qp2'))
prepareTransaction dbs rdbxs qp@(_, QPOne2) = return (rdbxs, qp)
prepareTransaction dbs _ qp@(_, QPZero2) = return ([], qp)
prepareTransaction dbs rdbxs (qpd, QPNot2 qp1) = do
    (dbxs, qp1') <- prepareTransaction dbs rdbxs qp1
    return (dbxs, (qpd, QPNot2 qp1'))
prepareTransaction dbs rdbxs (qpd, QPExists2 v qp1) = do
    (dbxs, qp1') <- prepareTransaction dbs rdbxs qp1
    return (dbxs, (qpd, QPExists2 v qp1'))
prepareTransaction dbs rdbxs qp@(qpd, QPTransaction2) =
    return (rdbxs, (qpd{tdb = Just (map (dbs !!) rdbxs) }, QPTransaction2))
prepareTransaction dbs rdbxs qp@(_, QPReturn2 _) = return (rdbxs, qp)

prepareQueryPlan :: (Monad m, ResultRow row) => [Database m row] -> QueryPlan2 m row  -> m (QueryPlan2 m row )
prepareQueryPlan _ (_, (Exec2  form [])) = error ("prepareQueryPlan: Exec2: no database" ++ show form)
prepareQueryPlan dbs (qpd, e@(Exec2  form (x : _))) =
    if x >= length dbs || x < 0 then
        error "index out of range"
    else case dbs !! x of
        Database db -> do
            let vars = paramvs qpd
                vars2 = returnvs qpd
                qu = Query form
                (stmtshow0, paramvars) = translateQuery db vars2 qu vars
                stmtshow = "at " ++ show x ++ " " ++ "paramvs " ++ show paramvars ++ " " ++ stmtshow0 ++ " returnvs " ++ show vars2
            stmt <- prepareQuery db vars2 qu vars
            return (qpd {stmts = Just [(AbstractDBStatement stmt, stmtshow)]}, e)
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
prepareQueryPlan dbs  (qpd, QPNot2 qp1) = do
    qp1' <- prepareQueryPlan dbs  qp1
    return (qpd, QPNot2 qp1')
prepareQueryPlan dbs  (qpd, QPExists2 v qp1) = do
    qp1' <- prepareQueryPlan dbs  qp1
    return (qpd, QPExists2 v qp1')
prepareQueryPlan _ qp@(_, QPReturn2 _) = return qp
prepareQueryPlan dbs qp@(_, QPTransaction2) =
    return qp

addCleanupRS :: (Monad m) => (Bool -> m ()) -> ResultStream m row -> ResultStream m row
addCleanupRS a (ResultStream rs) = ResultStream (addCleanup a rs)

execQueryPlan :: (MonadIO m, MonadBaseControl IO m, ResultRow row) => ([Var], ResultStream m row) -> QueryPlan2 m row  -> ([Var], ResultStream m row     )
execQueryPlan (vars, rs) (qpd, Exec2 _ _) = do
    let [(stmt, stmtshow)] = fromJust (stmts qpd)
    case stmt of
        AbstractDBStatement stmt0 -> do
            (toAscList (combinedvs qpd), addCleanupRS (\_ -> dbStmtClose stmt0) (do
                        row <- rs
                        liftIO $ infoM "QA" ("current row " ++ show row)
                        liftIO $ infoM "QA" ("execute " ++ stmtshow)
                        row2 <- dbStmtExec stmt0 vars (pure row)
                        liftIO $ infoM "QA" ("returns row")
                        return (transform vars (toAscList (combinedvs qpd)) (row <> row2))))

execQueryPlan  r (qpd, QPSequencing2 qp1 qp2) =
    (toAscList (combinedvs qpd), do
        let r1 = execQueryPlan  r qp1
            (vars2, rs2) = execQueryPlan r1 qp2 in
            transformResultStream vars2 (toAscList (combinedvs qpd)) rs2)

execQueryPlan  (vars, rs) (qpd, QPChoice2 qp1 qp2) =
    (toAscList (combinedvs qpd), do
        row <- rs
        let (vars1, rs1) = execQueryPlan  (vars, pure row) qp1
            (vars2, rs2) = execQueryPlan  (vars, pure row) qp2 in
            transformResultStream vars1 (toAscList (combinedvs qpd)) rs1 <|> transformResultStream vars2 (toAscList (combinedvs qpd)) rs2)

execQueryPlan  (vars, rs) (qpd, QPPar2 qp1 qp2) =
    (toAscList (combinedvs qpd), do
        row <- rs
        let (vars1, rs1) = execQueryPlan  (vars, pure row) qp1
            (vars2, rs2) = execQueryPlan  (vars, pure row) qp2
        (rs1', rs2') <- lift $ concurrently (getAllResultsInStream rs1) (getAllResultsInStream rs2)
        transformResultStream vars1 (toAscList (combinedvs qpd)) (listResultStream rs1') <|> transformResultStream vars2 (toAscList (combinedvs qpd)) (listResultStream rs2'))

execQueryPlan (vars, rs) (qpd, QPTransaction2 ) =
    (toAscList (combinedvs qpd), do
        let (begin, prepare, commit, rollback) = case tdb qpd of
                Nothing -> error "execQueryPlan: tranaction without db"
                Just dbs ->
                    (mapM_ (\db -> case db of
                        Database db' -> dbBegin db') dbs,
                    mapM (\db -> case db of
                        Database db' -> dbPrepare db') dbs,
                    mapM (\db -> case db of
                        Database db' -> dbCommit db') dbs,
                    mapM_ (\db -> case db of
                        Database db' -> dbRollback db') dbs)
        row <- rs
        let commitCleanup = do
                b' <- prepare
                if and b'
                    then do
                        b'' <- commit
                        if and b''
                            then return ()
                            else do
                                liftIO $ errorM "QA" ("execQueryPlan: commit failed " ++ show b'')
                    else do
                        rollback
                        liftIO $ errorM "QA" ("execQueryPlan: prepare failed rollback " ++ show b')
        let rollbackCleanup = do
                rollback
                liftIO $ errorM "QA" ("execQueryPlan: stream terminated rollback ")
        lift $ begin
        ResultStream (do
            yieldOr row rollbackCleanup
            lift $ commitCleanup))

execQueryPlan  r (_, QPOne2) = r
execQueryPlan  (vars', rs) (_, QPReturn2 vars) = (vars, transformResultStream vars' vars rs)
execQueryPlan  (vars, rs) (_, QPZero2) = (vars, closeResultStream rs)
execQueryPlan  (vars, rs) (_, QPNot2 qp) = -- assume no unbounded vars under not
            (vars, filterResultStream rs (\row -> do
                let (_, rs2) = execQueryPlan  (vars, (pure row)) qp
                isResultStreamEmpty rs2))
execQueryPlan  (vars, rs) (_, QPExists2 _ qp) = -- assume no unbounded vars under exists except v
            (vars, filterResultStream rs (\row -> do
                let (_, rs2) = execQueryPlan (vars, (pure row)) qp
                emp <- isResultStreamEmpty rs2
                return (not emp)))
