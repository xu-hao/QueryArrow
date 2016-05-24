{-# LANGUAGE ExistentialQuantification, MultiParamTypeClasses, FunctionalDependencies, TypeSynonymInstances, FlexibleInstances, FlexibleContexts, TypeFamilies, ScopedTypeVariables #-}
module QueryPlan where

import ResultStream
import FO.Data
import FO.Domain

import Prelude  hiding (lookup)
import Data.Map.Strict (Map, empty, insert, lookup, intersectionWith, delete)
import Data.List ((\\), intercalate, union, intersect)
import Control.Monad.Except
import Control.Applicative ((<|>))
import Data.Convertible.Base
import Data.Maybe
import Data.Monoid  ((<>))
import Data.Tree
import Data.Conduit
import qualified Data.Text as T
import Debug.Trace

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
-- query
data Query = Query { select :: [Var], cond :: Formula }

checkQuery :: Query -> Except String ()
checkQuery (Query _ form) = checkFormula form

instance Show Query where
    show (Query vars disjs) = show disjs ++ (if (null vars) then "" else " return " ++ intercalate " " (map show vars))


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
    -- domainSize function is a function from arguments to domain size
    -- it is used to compute the optimal query plan
    domainSize :: db -> DomainSizeMap -> DomainSizeFunction (m) Atom
    prepareQuery :: db -> Query -> [Var] -> m stmt
    supported :: db -> Formula -> [Var] -> Bool
    supported' :: db -> PureFormula -> [Var] -> Bool
    translateQuery :: db -> Query -> [Var] -> (String, [Var])

doQuery :: Database_ db m row stmt => db -> Query -> [Var] -> ResultStream (m) (row) -> ResultStream (m) (row)
doQuery db qu vars rs = do
        stmt <- lift $ prepareQuery db qu vars
        dbStmtExec stmt vars rs


-- https://wiki.haskell.org/Existential_type#Dynamic_dispatch_mechanism_of_OOP
data Database m row = forall db stmt. (Database_ db m row stmt) => Database { unDatabase :: db }

data QueryPlan = Exec Formula [Int]
                | QPClassical PureQueryPlan
                | QPTransaction QueryPlan
                | QPChoice QueryPlan QueryPlan
                | QPSequencing QueryPlan QueryPlan
                | QPZero
                | QPOne

data PureQueryPlan = If PureFormula [Int]
                | QPAnd (PureQueryPlan) (PureQueryPlan)
                | QPOr (PureQueryPlan) (PureQueryPlan)
                | QPTrue
                | QPFalse
                | QPNot (PureQueryPlan)
                | QPExists Var (PureQueryPlan) deriving Show


data AbstractDBStatement m row = forall stmt. (DBStatementClose m stmt, DBStatementExec m row stmt) => AbstractDBStatement {unAbstractDBStatement :: stmt}

data QueryPlanData m row  = QueryPlanData {
    inscopevs :: [Var],
    freevs :: [Var],
    determinevs :: [Var], -- determined vars
    paramvs :: [Var], -- parameter vars
    returnvs :: [Var], -- return vars
    combinedvs :: [Var], -- combined return and available vars that are still in scope
    availablevs :: [Var],
    stmts :: Maybe [(AbstractDBStatement m row, String)], -- stmt, show
    tdb :: Maybe [Database m row]
}

dqdb :: QueryPlanData m row
dqdb = QueryPlanData [] [] [] [] [] [] [] Nothing Nothing

data QueryPlanNode2 m row  = Exec2 Formula [Int]
                | QPClassical2 (PureQueryPlan2 m row )
                | QPTransaction2 (QueryPlan2 m row )
                | QPChoice2 (QueryPlan2 m row ) (QueryPlan2 m row )
                | QPSequencing2 (QueryPlan2 m row ) (QueryPlan2 m row )
                | QPZero2
                | QPOne2

data PureQueryPlanNode2 m row  = If2 PureFormula [Int]
                | QPAnd2 (PureQueryPlan2 m row ) (PureQueryPlan2 m row )
                | QPOr2 (PureQueryPlan2 m row ) (PureQueryPlan2 m row )
                | QPTrue2
                | QPFalse2
                | QPNot2 (PureQueryPlan2 m row )
                | QPExists2 Var (PureQueryPlan2 m row )

type QueryPlan2 m row  =  (QueryPlanData m row , QueryPlanNode2 m row )
type PureQueryPlan2 m row  =  (QueryPlanData m row , PureQueryPlanNode2 m row )

class ToTree a where
    toTree :: a -> Tree String
instance Show (QueryPlanData m row ) where
    show qp = "[" ++ show (availablevs qp) ++ "|" ++ show (paramvs qp) ++ "|" ++ show (freevs qp) ++ "|" ++ show (determinevs qp) ++ "|"++ show (returnvs qp) ++ "|" ++ show (combinedvs qp) ++ "|" ++ show (inscopevs qp) ++ "]"
instance ToTree (QueryPlan2 m row ) where
    toTree (qpd, Exec2 f  dbs) = Node ("exec " ++ show f ++ " at " ++ show dbs ++ show qpd ) []
    toTree (qpd, QPSequencing2 qp1 qp2) = Node ("sequencing"++ show qpd ) [toTree qp1, toTree qp2]
    toTree (qpd, QPChoice2 qp1 qp2) = Node ("choice"++ show qpd ) [toTree qp1, toTree qp2]
    toTree (qpd, QPClassical2 qp1) = Node ("where" ++ show qpd) [toTree qp1]
    toTree (qpd, QPTransaction2 qp1) = Node ("transaction" ++ show qpd) [toTree qp1]
    toTree (qpd, QPZero2) = Node ("zero"++ show qpd ) []
    toTree (qpd, QPOne2) = Node ("one"++ show qpd ) []
instance ToTree (PureQueryPlan2 m row ) where
    toTree (qpd, If2 f  dbs) = Node ("if " ++ show f ++ " at " ++ show dbs ++ show qpd ) []
    toTree (qpd, QPAnd2 qp1 qp2) = Node ("and"  ++ show qpd) [toTree qp1, toTree qp2]
    toTree (qpd, QPOr2 qp1 qp2) = Node ("union"++ show qpd ) [toTree qp1, toTree qp2]
    toTree (qpd, QPNot2 qp1) = Node ("not" ++ show qpd) [toTree qp1]
    toTree (qpd, QPExists2 v qp1) = Node ("exists " ++ show v ++ " where" ++ show qpd) [toTree qp1]
    toTree (qpd, QPTrue2) = Node ("(generate [[]])" ++ show qpd) []
    toTree (qpd, QPFalse2) = Node ("(generate [])" ++ show qpd) []

formulaToQueryPlan :: (Monad m)=> [Database m row] -> Formula -> QueryPlan
formulaToQueryPlan dbs  form@(FAtomic (Atom pred0  _)) =
    Exec form  (filter (\x -> case dbs !! x of
                                Database db -> pred0 `elem` getPreds db) [0..(length dbs-1)])
formulaToQueryPlan _ FOne = QPOne
formulaToQueryPlan _ FZero = QPZero
formulaToQueryPlan dbs  (FClassical form) = QPClassical (formulaToQueryPlan' dbs  form)
formulaToQueryPlan dbs  (FTransaction form) = QPTransaction (formulaToQueryPlan dbs  form)
formulaToQueryPlan dbs  (FChoice form1 form2) = QPChoice (formulaToQueryPlan dbs form1) (formulaToQueryPlan dbs form2)
formulaToQueryPlan dbs  (FSequencing form1 form2) = QPSequencing (formulaToQueryPlan dbs form1) (formulaToQueryPlan dbs form2)
formulaToQueryPlan dbs  ins@(FInsert (Lit _ (Atom pred1 _))) =
    let p pred0 x = case dbs !! x of Database db -> pred0 `elem` getPreds db
        xs = [0..length dbs - 1]
        findPredicateDB pred0 = filter (p pred0) xs
        xs2 = findPredicateDB pred1 in
        Exec ins (filter (\x ->
            case dbs !! x of
                 Database db ->
                    supported db ins []) xs2)


formulaToQueryPlan' :: (Monad m)=> [Database m row] -> PureFormula -> PureQueryPlan
formulaToQueryPlan' dbs  form@(Atomic (Atom pred0 _)) = If form (filter (\x -> case dbs !! x of
                            Database db -> pred0 `elem` getPreds db) [0..(length dbs-1)])
formulaToQueryPlan' dbs  (Not form) = QPNot (formulaToQueryPlan' dbs  form)
formulaToQueryPlan' dbs  (Disjunction form1 form2) = QPOr  (formulaToQueryPlan' dbs form1) (formulaToQueryPlan' dbs form2)
formulaToQueryPlan' dbs  (Conjunction form1 form2) = QPAnd  (formulaToQueryPlan' dbs form1) (formulaToQueryPlan' dbs form2)
formulaToQueryPlan' dbs  (Exists v form) = QPExists v (formulaToQueryPlan' dbs  form)
formulaToQueryPlan' _ CFalse = QPFalse
formulaToQueryPlan' _ CTrue = QPTrue
formulaToQueryPlan' _  (Forall _ _) = error "formulaToQueryPlan': Forall is not supported"


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
simplifyQueryPlan  (QPSequencing qp1 qp2) =
    let qp1' = simplifyQueryPlan  qp1
        qp2' = simplifyQueryPlan  qp2 in
        case (qp1', qp2') of
            (QPOne, _) -> qp2'
            (QPZero, _) -> QPZero
            (_, QPOne) -> qp1'
            (_, _) -> QPSequencing qp1' qp2'
simplifyQueryPlan  (QPClassical qp1) =
    QPClassical (simplifyPureQueryPlan  qp1)
simplifyQueryPlan  (QPTransaction qp1) =
    QPTransaction (simplifyQueryPlan  qp1)
simplifyQueryPlan  qp = qp

simplifyPureQueryPlan :: PureQueryPlan -> PureQueryPlan
simplifyPureQueryPlan  qp@(If _ _) = qp
simplifyPureQueryPlan  (QPOr qp1 qp2) =
    let qp1' = simplifyPureQueryPlan  qp1
        qp2' = simplifyPureQueryPlan  qp2 in
        case (qp1', qp2') of
            (QPTrue, _) -> QPTrue
            (QPFalse, _) -> qp2'
            (_, QPTrue) -> QPTrue
            (_, QPFalse) -> qp1'
            (_, _) -> QPOr qp1' qp2'
simplifyPureQueryPlan  (QPAnd qp1 qp2) =
    let qp1' = simplifyPureQueryPlan  qp1
        qp2' = simplifyPureQueryPlan  qp2 in
        case (qp1', qp2') of
            (QPTrue, _) -> qp2'
            (QPFalse, _) -> QPFalse
            (_, QPTrue) -> qp1'
            (_, QPFalse) -> QPFalse
            (_, _) -> QPAnd qp1' qp2'
simplifyPureQueryPlan  (QPExists v qp1) =
    let qp1' = simplifyPureQueryPlan  qp1 in
        QPExists v qp1'
simplifyPureQueryPlan  (QPNot qp1) =
    let qp1' = simplifyPureQueryPlan  qp1 in
        case qp1' of
            QPTrue -> QPFalse
            QPFalse -> QPTrue
            _ -> QPNot qp1'
simplifyPureQueryPlan (QPTrue) = QPTrue
simplifyPureQueryPlan (QPFalse) = QPFalse


combineQPSequencingData :: QueryPlanData  m row -> QueryPlanData m row -> QueryPlanData m row
combineQPSequencingData qp1 qp2 =
    QueryPlanData {
        availablevs = availablevs qp1,
        freevs = freevs qp1 `union` freevs qp2,
        determinevs = determinevs qp1 `union` determinevs qp2,
        paramvs = paramvs qp1 `union` (paramvs qp2 \\ returnvs qp1),
        returnvs = (returnvs qp1 `union` returnvs qp2) `intersect` inscopevs qp2,
        inscopevs = inscopevs qp2,
        combinedvs = (combinedvs qp1 `union` combinedvs qp2) `intersect` inscopevs qp2,
        stmts = Nothing,
        tdb = Nothing
    }

combineQPChoiceData :: QueryPlanData m row -> QueryPlanData m row -> QueryPlanData m row
combineQPChoiceData qp1 qp2 =
    QueryPlanData {
        availablevs = availablevs qp1,
        freevs = freevs qp1 `union` freevs qp2,
        determinevs = determinevs qp1 `intersect` determinevs qp2,
        paramvs = paramvs qp1 `union` paramvs qp2,
        returnvs = returnvs qp1 `union` returnvs qp2,
        inscopevs = inscopevs qp1,
        combinedvs = combinedvs qp2 `union` combinedvs qp2,
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
                    dbs' = filter (\x -> case dbsx !! x of (Database db) -> supported db fse (returnvs qpd)) dbs in
                    if null dbs' then
                        (qpd, QPSequencing2 qp1' qp2')
                    else
                        (qpd, Exec2 fse dbs')
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
                    dbs' = filter (\x -> case dbsx !! x of (Database db) -> supported db fch (returnvs qpd)) dbs in
                    if null dbs' then
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
optimizeQueryPlan dbsx (qpd, QPClassical2 qp) =
    (qpd, QPClassical2 (optimizePureQueryPlan dbsx qp))
optimizeQueryPlan dbsx (qpd, QPTransaction2 qp) =
    (qpd, QPTransaction2 (optimizeQueryPlan dbsx qp))
optimizeQueryPlan _ qp = qp

optimizePureQueryPlan :: (Monad m ) => [Database m row] -> PureQueryPlan2 m row -> PureQueryPlan2 m row

optimizePureQueryPlan dbsx (qpd, QPOr2 qp1 qp2) =
    let qp1' = optimizePureQueryPlan dbsx qp1
        qp2' = optimizePureQueryPlan dbsx qp2 in
        case (qp1', qp2') of
            ((_, If2 formula1 dbs1), (_, If2 formula2 dbs2)) ->
                let dbs = dbs1 `intersect` dbs2
                    disjs = disj [formula1, formula2]
                    dbs' = filter (\x -> case dbsx !! x of (Database db) -> supported' db disjs (returnvs qpd)) dbs in
                    if null dbs' then
                        (qpd, QPOr2 qp1' qp2')
                    else
                        (qpd, If2 disjs  dbs')
            ((qpd1, If2 _ _), (_, QPOr2 qp21@(qpd3, _) qp22)) ->
                (qpd, QPOr2 (optimizePureQueryPlan dbsx (combineQPChoiceData qpd1 qpd3, QPOr2 qp1' qp21)) qp22)
            ((_, QPOr2 qp11@(qpd3, _) qp12), (qpd2, If2 _ _)) ->
                (qpd, QPOr2 qp11 (optimizePureQueryPlan dbsx (combineQPChoiceData qpd3 qpd2, QPOr2 qp12 qp2')))
            ((qpd1, QPOr2 qp11 qp12@(qpd3, _)), (_, QPOr2 qp21@(qpd4, _) qp22)) ->
                (qpd, QPOr2 (combineQPChoiceData qpd1 qpd4, QPOr2 qp11 (optimizePureQueryPlan dbsx (combineQPChoiceData qpd3 qpd4, QPOr2 qp12 qp21))) qp22)
            _ -> (qpd, QPOr2 qp1' qp2')
optimizePureQueryPlan dbsx (qpd, QPAnd2 qp1 qp2) =
    let qp1' = optimizePureQueryPlan dbsx qp1
        qp2' = optimizePureQueryPlan dbsx qp2 in
        case (qp1', qp2') of
            ((_, If2 formula1 dbs1), (_, If2 formula2 dbs2)) ->
                let dbs = dbs1 `intersect` dbs2
                    conjs = Conjunction formula1 formula2
                    dbs' = filter (\x -> case dbsx !! x of (Database db) -> supported' db conjs (returnvs qpd)) dbs in
                    if null dbs' then
                        (qpd, QPAnd2 qp1' qp2')
                    else
                        (qpd, If2 conjs  dbs')
            ((qpd1, If2 _ _), (_, QPAnd2 qp21@(qpd3, _) qp22)) ->
                (qpd, QPAnd2 (optimizePureQueryPlan dbsx (combineQPSequencingData qpd1 qpd3, QPAnd2 qp1' qp21)) qp22)
            ((_, QPAnd2 qp11 qp12@(qpd3, _)), (qpd2, If2 _ _)) ->
                (qpd, QPAnd2 qp11 (optimizePureQueryPlan dbsx (combineQPSequencingData qpd3 qpd2, QPAnd2 qp12 qp2')))
            ((qpd1, QPAnd2 qp11 qp12@(qpd3, _)), (_, QPAnd2 qp21@(qpd4, _) qp22)) ->
                (qpd, QPAnd2 (combineQPSequencingData qpd1 qpd4, QPAnd2 qp11 (optimizePureQueryPlan dbsx (combineQPSequencingData qpd3 qpd4, QPAnd2 qp12 qp21))) qp22)
            _ -> (qpd, QPAnd2 qp1' qp2')
optimizePureQueryPlan dbsx (qpd, QPExists2 v qp1) =
    let qp1' = optimizePureQueryPlan dbsx qp1 in
        case qp1' of
            (_, If2 formula1 dbs) ->
                        let exi = (Exists v formula1)
                            dbs' = filter (\x -> case dbsx !! x of (Database db) -> supported' db exi (returnvs qpd)) dbs  in
                            if null dbs' then
                                (qpd, QPExists2 v qp1')
                            else
                                (qpd, If2 exi  dbs')
            _ -> (qpd, QPExists2 v qp1')
optimizePureQueryPlan dbsx (qpd, QPNot2 qp1) =
    let qp1' = optimizePureQueryPlan dbsx qp1 in
        case qp1' of
            (_, If2 formula1 dbs) ->
                        let notform = Not formula1
                            dbs' = filter (\x -> case dbsx !! x of (Database db) -> supported' db notform (returnvs qpd)) dbs  in
                            if null dbs' then
                                (qpd, QPNot2 qp1')
                            else
                                (qpd, If2 notform  dbs')
            _ -> (qpd, QPNot2 qp1')
optimizePureQueryPlan _ qp = qp

domainSizeFormula :: (Monad m) => DomainSizeMap -> Database m row -> Formula -> m DomainSizeMap
domainSizeFormula map1 (Database db_) form = do
        map1' <- determinedVars (domainSize db_ map1)  form
        return (mmin map1 map1')

domainSizeFormula' :: (Monad m) => DomainSizeMap -> Database m row -> PureFormula -> m DomainSizeMap
domainSizeFormula' map1 (Database db_) form = do
        map1' <- determinedVars (domainSize db_ map1)  form
        return (mmin map1 map1')

checkQueryPlan2 :: (Monad m ) => [Database m row] -> QueryPlan -> ExceptT (String, Formula, DomainSizeMap) m DomainSizeMap
checkQueryPlan2 dbs qp = do
    checkQueryPlan dbs empty qp

checkQueryPlan :: (Monad m) => [Database m row] -> DomainSizeMap -> QueryPlan -> ExceptT (String, Formula, DomainSizeMap) m DomainSizeMap
checkQueryPlan _ map1 (Exec form []) = throwError ("no database", form, map1)
checkQueryPlan dbs map1 (Exec form (x : _)) = do
    let fvs0 = freeVars form
    map2 <- lift (domainSizeFormula map1 (dbs !! x) form)
    let vec = map (\v ->
            case lookupDomainSize v map2 of
                Bounded _ -> True
                Unbounded -> False) fvs0
    if and vec
        then return (  map2)
        else throwError ("checkQueryPlan: unbounded vars: " ++ show fvs0 ++ " " ++ show vec ++ ", the formula is " ++ show form ++ " " ++ show map1 ++ " " ++ show map2, form, map2)

checkQueryPlan dbs map1 (QPChoice qp1 qp2) = do
    map2 <- checkQueryPlan dbs map1 qp1
    map3 <- checkQueryPlan dbs map1 qp2
    return (intersectionWith (+) map2 map3)

checkQueryPlan dbs map1 (QPSequencing qp1 qp2) = do
    map2 <- checkQueryPlan dbs map1 qp1
    trace (show "checkQueryPlan: " ++ show map1 ++ " " ++ show map2) $ checkQueryPlan dbs map2 qp2

checkQueryPlan _ map1 (QPOne) = do
    return map1

checkQueryPlan _ map1 (QPZero) = do
    return map1

checkQueryPlan dbs map1 (QPClassical qp) = do
    _ <- checkQueryPlan' dbs Pos map1 qp
    return empty

checkQueryPlan dbs map1 (QPTransaction qp) = do
    checkQueryPlan dbs map1 qp


checkQueryPlan' :: (Monad m) => [Database m row] -> Sign -> DomainSizeMap -> PureQueryPlan -> ExceptT (String, Formula, DomainSizeMap) m DomainSizeMap
checkQueryPlan' _ Pos map1 (If form []) =
    throwError ("checkQueryPlan': no database", FClassical form, map1)
checkQueryPlan' dbs Pos map1 (If form (x : _)) = do
    let fvs0 = freeVars form
    map2 <- lift (domainSizeFormula' map1 (dbs !! x) form)
    if all (\v ->
        case lookupDomainSize v map2 of
            Bounded _ -> True
            Unbounded -> False) fvs0
        then return map2
        else throwError ("checkQueryPlan': unbounded vars: " ++ show fvs0 ++ " " ++ show map2, FClassical form, map2)

checkQueryPlan' _ Neg map1 (If form _) = do
    let fvs0 = freeVars form
    if all (\v ->
        case lookupDomainSize v map1 of
            Bounded _ -> True
            Unbounded -> False) fvs0
        then return map1
        else throwError ("checkQueryPlan': unbounded vars: " ++ show fvs0, FClassical form, map1)

checkQueryPlan' dbs Pos map1 (QPOr qp1 qp2) = do
    map2 <- checkQueryPlan' dbs Pos map1 qp1
    map3 <- checkQueryPlan' dbs Pos map1 qp2
    return (intersectionWith (+) map2 map3)

checkQueryPlan' dbs Neg map1 (QPOr qp1 qp2) = do
    _ <- checkQueryPlan' dbs Neg map1 qp1
    _ <- checkQueryPlan' dbs Neg map1 qp2
    return map1

checkQueryPlan' dbs Pos map1 (QPAnd qp1 qp2) = do
    map2 <- checkQueryPlan' dbs Pos map1 qp1
    checkQueryPlan' dbs Pos map2 qp2

checkQueryPlan' dbs Neg map1 (QPAnd qp1 qp2) = do
    _ <- checkQueryPlan' dbs Neg map1 qp1
    _ <- checkQueryPlan' dbs Neg map1 qp2
    return map1

checkQueryPlan' _ _ map1 (QPTrue) = do
    return map1

checkQueryPlan' _ _ map1 (QPFalse) = do
    return map1

checkQueryPlan' dbs _ map1 (QPNot qp) = do
    _ <- checkQueryPlan' dbs Neg map1 qp
    return map1

checkQueryPlan' dbs _ map1 (QPExists v qp) = do
    map2 <- checkQueryPlan' dbs Pos map1 qp
    return (delete v map2)

calculateVars :: [Var] -> [Var] -> QueryPlan -> QueryPlan2 m row
calculateVars lvars rvars qp = calculateVars2 lvars (calculateVars1 rvars qp)

calculateVars1 :: [Var] -> QueryPlan -> QueryPlan2 m row
calculateVars1  rvars (Exec form dbxs) =
    let fvs = freeVars form in
        ( dqdb{freevs = fvs, determinevs = fvs, inscopevs = rvars}, (Exec2  form dbxs))

calculateVars1  rvars (QPSequencing qp1 qp2) =
    let qp2'@(qpd2, _) = calculateVars1 rvars qp2
        qp1'@(qpd1, _) = calculateVars1 (freevs qpd2 `union` rvars) qp1 in
        ( dqdb{freevs = freevs qpd1 `union` freevs qpd2, determinevs = determinevs qpd1 `union` determinevs qpd2, inscopevs = rvars} , (QPSequencing2  qp1' qp2'))

calculateVars1 rvars  (QPChoice qp1 qp2) =
    let qp1'@(qpd1, _) = calculateVars1 rvars qp1
        qp2'@(qpd2, _) = calculateVars1 rvars qp2 in
        ( dqdb{freevs = freevs qpd1 `union` freevs qpd2, determinevs = determinevs qpd1 `intersect` determinevs qpd2, inscopevs = rvars} , (QPChoice2  qp1' qp2'))
calculateVars1 rvars  (QPOne) = (dqdb{freevs = [], determinevs = [], inscopevs = rvars} , QPOne2)
calculateVars1 rvars  (QPZero) = (dqdb{freevs = [], determinevs = [], inscopevs = rvars} , QPZero2)
calculateVars1 rvars  (QPClassical qp1) =
    let qp1'@(qpd1, _) = calculateVars1' rvars qp1 in
        (dqdb{freevs = freevs qpd1, determinevs = [], inscopevs = rvars} , QPClassical2 qp1')
calculateVars1 rvars  (QPTransaction qp1) =
    let qp1'@(qpd1, _) = calculateVars1 rvars qp1 in
        (dqdb{freevs = freevs qpd1, determinevs = [], inscopevs = rvars} , QPTransaction2 qp1')

calculateVars1' :: [Var] -> PureQueryPlan -> PureQueryPlan2 m row
calculateVars1' rvars  (QPOr qp1 qp2) =
    let qp1'@(qpd1, _) = calculateVars1' rvars qp1
        qp2'@(qpd2, _) = calculateVars1' rvars qp2 in
        ( dqdb{freevs = freevs qpd1 `union` freevs qpd2, determinevs = determinevs qpd1 `intersect` determinevs qpd2, inscopevs = rvars} , (QPOr2  qp1' qp2'))

calculateVars1'  rvars (QPAnd qp1 qp2) =
    let qp2'@(qpd2, _) = calculateVars1' rvars qp2
        qp1'@(qpd1, _) = calculateVars1' (freevs qpd2 `union` rvars) qp1 in
        ( dqdb{freevs = freevs qpd1 `union` freevs qpd2, determinevs = determinevs qpd1 `union` determinevs qpd2, inscopevs = rvars} , (QPAnd2  qp1' qp2'))

calculateVars1' rvars  (QPExists v qp1) =
    let qp1'@(qpd1, _) = calculateVars1' rvars qp1 in
        ( dqdb{freevs = freevs qpd1 \\ [v], determinevs = determinevs qpd1 \\ [v], inscopevs = rvars} , (QPExists2 v qp1'))

calculateVars1' rvars  (QPNot qp1) =
    let qp1'@(qpd1, _) = calculateVars1' rvars qp1 in
        ( dqdb{freevs = freevs qpd1, determinevs = [], inscopevs = rvars} , (QPNot2  qp1'))
calculateVars1' rvars  (QPTrue) = (dqdb{freevs = [], determinevs = [], inscopevs = rvars} , QPTrue2)
calculateVars1' rvars  (QPFalse) = (dqdb{freevs = [], determinevs = [], inscopevs = rvars} , QPFalse2)
calculateVars1' rvars  (If form dbs) =
        (dqdb{freevs = freeVars form, determinevs = [], inscopevs = rvars} , If2 form dbs)

calculateVars2 :: [Var] -> QueryPlan2 m row  -> QueryPlan2 m row
calculateVars2  lvars (qpd, Exec2 form dbxs) =
            ( qpd{availablevs = lvars, paramvs = lvars `intersect` (freevs qpd), returnvs = (inscopevs qpd `intersect` determinevs qpd \\ lvars), combinedvs = inscopevs qpd `intersect` (determinevs qpd `union` lvars) }, (Exec2  form dbxs))
calculateVars2  lvars (qpd, QPChoice2 qp1 qp2) =
            let qp1' = calculateVars2 lvars qp1
                qp2' = calculateVars2 lvars qp2 in
                ( qpd{availablevs = lvars, paramvs = lvars `intersect` (freevs qpd), returnvs = (inscopevs qpd `intersect` determinevs qpd \\ lvars), combinedvs = inscopevs qpd `intersect` (determinevs qpd `union` lvars)} , (QPChoice2  qp1' qp2'))
calculateVars2  lvars (qpd, QPSequencing2 qp1 qp2) =
            let qp1'@(qpd1, _) = calculateVars2 lvars qp1
                qp2' = calculateVars2 (lvars `union` freevs qpd1) qp2 in
                ( qpd{availablevs = lvars, paramvs = lvars `intersect` (freevs qpd), returnvs = (inscopevs qpd `intersect` determinevs qpd \\ lvars), combinedvs = inscopevs qpd `intersect` (determinevs qpd `union` lvars)} , (QPSequencing2  qp1' qp2'))

calculateVars2 lvars  (qpd, QPOne2) = ( qpd{availablevs = lvars, paramvs = [], returnvs = [], combinedvs = inscopevs qpd `intersect` lvars} , QPOne2)
calculateVars2 lvars  (qpd, QPZero2) = ( qpd{availablevs = lvars, paramvs = [], returnvs = [],combinedvs = inscopevs qpd `intersect` lvars} , QPZero2)
calculateVars2 lvars  (qpd, QPClassical2 qp1) =
                let qp1' = calculateVars2' lvars qp1 in
                    (qpd{availablevs = lvars, paramvs = lvars `intersect` (freevs qpd), returnvs = [],combinedvs = inscopevs qpd `intersect` (determinevs qpd `union` lvars)} , QPClassical2 qp1')
calculateVars2 lvars  (qpd, QPTransaction2 qp1) =
                let qp1' = calculateVars2 lvars qp1 in
                    (qpd{availablevs = lvars, paramvs = lvars `intersect` (freevs qpd), returnvs = [],combinedvs = inscopevs qpd `intersect` (determinevs qpd `union` lvars)} , QPTransaction2 qp1')

calculateVars2' :: [Var] -> PureQueryPlan2 m row  -> PureQueryPlan2 m row
calculateVars2'  lvars (qpd, If2 form dbs)  =
                ( qpd{availablevs = lvars, paramvs = lvars `intersect` (freevs qpd), returnvs = (inscopevs qpd `intersect` determinevs qpd) \\ lvars,combinedvs = inscopevs qpd `intersect` (determinevs qpd `union` lvars)} , If2 form dbs)
calculateVars2'  lvars (qpd, QPOr2 qp1 qp2) =
            let qp1' = calculateVars2' lvars qp1
                qp2' = calculateVars2' lvars qp2 in
                ( qpd{availablevs = lvars, paramvs = lvars `intersect` (freevs qpd), returnvs = (inscopevs qpd `intersect` determinevs qpd) \\ lvars,combinedvs = inscopevs qpd `intersect` (determinevs qpd `union` lvars)} , (QPOr2  qp1' qp2'))

calculateVars2'  lvars (qpd, QPAnd2 qp1 qp2) =
            let qp1'@(qpd1, _) = calculateVars2' lvars qp1
                qp2' = calculateVars2' (lvars `union` freevs qpd1) qp2 in
                ( qpd{availablevs = lvars, paramvs = lvars `intersect` (freevs qpd), returnvs = (inscopevs qpd `intersect` determinevs qpd) \\ lvars,combinedvs = inscopevs qpd `intersect` (determinevs qpd `union` lvars)} , (QPAnd2  qp1' qp2'))

calculateVars2' lvars  (qpd, QPExists2 v qp1) =
            let qp1' = calculateVars2' (lvars \\ [v]) qp1 in
                ( qpd{availablevs = lvars, paramvs = lvars `intersect` (freevs qpd), returnvs = (inscopevs qpd `intersect` determinevs qpd) \\ lvars,combinedvs = inscopevs qpd `intersect` (determinevs qpd `union` lvars)} , (QPExists2 v qp1'))

calculateVars2' lvars  (qpd, QPNot2 qp1) =
            let qp1' = calculateVars2' lvars qp1 in
                ( qpd{availablevs = lvars, paramvs = lvars `intersect` (freevs qpd), returnvs = (inscopevs qpd `intersect` determinevs qpd) \\ lvars,combinedvs = inscopevs qpd `intersect` (determinevs qpd `union` lvars)} , (QPNot2  qp1'))
calculateVars2' lvars  (qpd, QPTrue2) =
            ( qpd{availablevs = lvars, paramvs = [], returnvs = [],combinedvs = inscopevs qpd `intersect` lvars } , (QPTrue2))

calculateVars2' lvars  (qpd, QPFalse2) =
            ( qpd{availablevs = lvars, paramvs = [], returnvs = [],combinedvs = inscopevs qpd `intersect` lvars } , (QPFalse2))

addTransaction :: QueryPlan2 m row -> QueryPlan2 m row
addTransaction qp@(qpd, (Exec2 _ _)) = (qpd, QPTransaction2 qp)
addTransaction (qpd, (QPSequencing2 qp1 qp2)) = (qpd, QPSequencing2 (addTransaction qp1) (addTransaction qp2))
addTransaction (qpd, (QPChoice2 qp1 qp2)) = (qpd, QPChoice2 (addTransaction qp1) (addTransaction qp2))
addTransaction qp = qp


prepareQueryPlan :: (Monad m, ResultRow row) => [Database m row] -> QueryPlan2 m row  -> m (QueryPlan2 m row )
prepareQueryPlan _ (_, (Exec2  form [])) = error ("prepareQueryPlan: Exec2: no database" ++ show form)
prepareQueryPlan dbs (qpd, e@(Exec2  form (x : _))) =
    if x >= length dbs || x < 0 then
        error "index out of range"
    else case dbs !! x of
        Database db -> do
            let vars = paramvs qpd
                vars2 = returnvs qpd
                qu = (Query vars2 form)
                stmtshow = "at " ++ show x ++ " " ++ show (translateQuery db qu vars ) ++ " returnvs " ++ show vars2
            trace ("prepare " ++ stmtshow) $ return ()
            stmt <- prepareQuery db qu vars
            return (qpd {stmts = Just [(AbstractDBStatement stmt, stmtshow)]}, e)
prepareQueryPlan dbs  (qpd, QPChoice2 qp1 qp2) = do
    qp1' <- prepareQueryPlan dbs  qp1
    qp2' <- prepareQueryPlan dbs  qp2
    return (qpd, QPChoice2 qp1' qp2')
prepareQueryPlan dbs  (qpd, QPSequencing2 qp1 qp2) = do
    qp1' <- prepareQueryPlan dbs  qp1
    qp2' <- prepareQueryPlan dbs  qp2
    return (qpd, QPSequencing2 qp1' qp2')
prepareQueryPlan _ qp@(_, QPOne2) = return qp
prepareQueryPlan _ qp@(_, QPZero2) = return qp
prepareQueryPlan dbs (qpd, QPClassical2 qp) = do
    qp' <- prepareQueryPlan' dbs qp
    return (qpd, QPClassical2 qp')
prepareQueryPlan dbs (qpd, QPTransaction2 qp) = do
    qp' <- prepareQueryPlan dbs qp
    let dbxs = scandb qp'
    return (qpd{tdb = Just (map (dbs !!) dbxs) }, QPTransaction2 qp')


prepareQueryPlan' :: (Monad m, ResultRow row) => [Database m row] -> PureQueryPlan2 m row  -> m (PureQueryPlan2 m row )
prepareQueryPlan' _  (_, (If2 form [])) = error ("prepareQueryPlan': If2: no database" ++ show form)
prepareQueryPlan' dbs  (qpd, e@(If2 form (x:_))) =
    if x >= length dbs || x < 0 then
        error "index out of range"
    else case dbs !! x of
        Database db -> do
            let vars = paramvs qpd
                vars2 = returnvs qpd
                qu = Query vars2 (convert form)
                stmtshow = "at " ++ show x ++ " " ++ show (translateQuery db qu vars) ++ " returnvs " ++ show vars2
            trace ("prepare " ++ stmtshow) $ return ()
            stmt <- prepareQuery db qu vars
            return (qpd {stmts = Just [(AbstractDBStatement stmt, stmtshow)]}, e)
prepareQueryPlan' dbs  (qpd, QPOr2 qp1 qp2) = do
    qp1' <- prepareQueryPlan' dbs  qp1
    qp2' <- prepareQueryPlan' dbs  qp2
    return (qpd, QPOr2 qp1' qp2')
prepareQueryPlan' dbs  (qpd, QPAnd2 qp1 qp2) = do
    qp1' <- prepareQueryPlan' dbs  qp1
    qp2' <- prepareQueryPlan' dbs  qp2
    return (qpd, QPAnd2 qp1' qp2')
prepareQueryPlan' dbs  (qpd, QPNot2 qp1) = do
    qp1' <- prepareQueryPlan' dbs  qp1
    return (qpd, QPNot2 qp1')
prepareQueryPlan' dbs  (qpd, QPExists2 v qp1) = do
    qp1' <- prepareQueryPlan' dbs  qp1
    return (qpd, QPExists2 v qp1')

prepareQueryPlan' _ qp@(_, QPTrue2) = return qp
prepareQueryPlan' _ qp@(_, QPFalse2) = return qp


scandb :: QueryPlan2 m row  -> [Int]
scandb (_, Exec2 _ []) = []
scandb (_, Exec2 _ (x : _)) = [x]
scandb (_, QPChoice2 qp1 qp2) = scandb qp1 `union` scandb qp2
scandb (_, QPSequencing2 qp1 qp2) = scandb qp1 `union` scandb qp2
scandb (_, QPOne2) = []
scandb (_, QPZero2) = []
scandb (_, QPClassical2 qp) = scandb' qp
scandb (_, QPTransaction2 qp) = scandb qp

scandb' :: PureQueryPlan2 m row  -> [Int]
scandb' (_, (If2 _ [])) = []
scandb' (_, (If2 _ (x:_))) = [x]
scandb' (_, QPOr2 qp1 qp2) = scandb'  qp1 `union` scandb'  qp2
scandb' (_, QPAnd2 qp1 qp2) = scandb'   qp1 `union` scandb'   qp2
scandb' (_, QPNot2 qp1) = scandb'   qp1
scandb' (_, QPExists2 _ qp1) = scandb'   qp1
scandb' (_, QPTrue2) = []
scandb' (_, QPFalse2) = []

addCleanupRS :: (Monad m) => (Bool -> m ()) -> ResultStream m row -> ResultStream m row
addCleanupRS a (ResultStream rs) = ResultStream (addCleanup a rs)

execQueryPlan :: (Monad m, ResultRow row) => ([Var], ResultStream m row) -> QueryPlan2 m row  -> ([Var], ResultStream m row     )
execQueryPlan (vars, rs) (qpd, Exec2 _ _) = do
    let [(stmt, stmtshow)] = fromJust (stmts qpd)
    case stmt of
        AbstractDBStatement stmt0 -> do
            (inscopevs qpd, addCleanupRS (\_ -> dbStmtClose stmt0) (do
                        row <- rs
                        trace ("exec " ++ stmtshow) $ return ()
                        row2 <- dbStmtExec stmt0 vars (pure row)
                        trace ("result row") $ return ()
                        return (transform vars (combinedvs qpd) (row <> row2))))

execQueryPlan  r (qpd, QPSequencing2 qp1 qp2) =
    (inscopevs qpd, do
        let r1 = execQueryPlan  r qp1
            (vars2, rs2) = execQueryPlan r1 qp2 in
            transformResultStream vars2 (combinedvs qpd) rs2)

execQueryPlan  (vars, rs) (qpd, QPChoice2 qp1 qp2) =
    (inscopevs qpd, do
        row <- rs
        let (vars1, rs1) = execQueryPlan  (vars, pure row) qp1
            (vars2, rs2) = execQueryPlan  (vars, pure row) qp2 in
            transformResultStream vars1 (combinedvs qpd) rs1 <|> transformResultStream vars2 (combinedvs qpd) rs2)

execQueryPlan (vars, rs) (qpd, QPClassical2 qp) =
        (inscopevs qpd, filterResultStream rs (\row -> do
            let (_, rs2) = execQueryPlan' (vars, (pure row)) qp
            emp <- isResultStreamEmpty rs2
            return (not emp)))
execQueryPlan (vars, rs) (qpd, QPTransaction2 qp) =
    (inscopevs qpd, do
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
        lift $ begin
        addCleanupRS (\b ->
                if b
                    then do
                        b' <- prepare
                        if and b'
                            then do
                                b'' <- commit
                                if and b''
                                    then return ()
                                    else error ("execQueryPlan: commit failed " ++ show b'' ++ " " ++ show (toTree qp))
                            else do
                                rollback
                                error ("execQueryPlan: prepare failed rollback " ++ show b' ++ " " ++ show (toTree qp))
                    else do
                        rollback
                        error ("execQueryPlan: stream terminated rollback " ++ show (toTree qp))) (snd (execQueryPlan (vars, (pure row)) qp)))

execQueryPlan  r (_, QPOne2) = r
execQueryPlan  (vars, rs) (_, QPZero2) = (vars, closeResultStream rs)

execQueryPlan' :: (Monad m, ResultRow row) => ([Var], ResultStream m row) -> PureQueryPlan2 m row  -> ([Var], ResultStream m row     )
execQueryPlan' (vars, rs) (qpd, If2 _ _) = do
    let [(stmt, stmtshow)] = fromJust (stmts qpd)
    case stmt of
        AbstractDBStatement stmt0 -> do
            (inscopevs qpd, addCleanupRS (\_ -> dbStmtClose stmt0) (do
                    row <- rs
                    trace ("exec " ++ stmtshow) $ return ()
                    row2 <- dbStmtExec stmt0 vars (pure row)
                    return (transform vars (combinedvs qpd) (row <> row2))))
execQueryPlan'  (vars, rs) (qpd, QPOr2 qp1 qp2) =
    (inscopevs qpd, do
        row <- rs
        let (vars1, rs1) = execQueryPlan'  (vars, pure row) qp1
            (vars2, rs2) = execQueryPlan'  (vars, pure row) qp2
        transformResultStream vars1 (combinedvs qpd) rs1 <|> transformResultStream vars2 (combinedvs qpd) rs2)
execQueryPlan'  r (qpd, QPAnd2 qp1 qp2) =
    (inscopevs qpd, do
        let r1 = execQueryPlan' r qp1
            (vars2, rs2) = execQueryPlan'  r1 qp2
        transformResultStream vars2 (combinedvs qpd) rs2)

execQueryPlan'  (vars, rs) (_, QPNot2 qp) = -- assume no unbounded vars under not
            (vars, filterResultStream rs (\row -> do
                let (_, rs2) = execQueryPlan'  (vars, (pure row)) qp
                isResultStreamEmpty rs2))
execQueryPlan'  (vars, rs) (_, QPExists2 _ qp) = -- assume no unbounded vars under exists except v
            (vars, filterResultStream rs (\row -> do
                let (_, rs2) = execQueryPlan' (vars, (pure row)) qp
                emp <- isResultStreamEmpty rs2
                return (not emp)))
execQueryPlan' r (_, QPTrue2) = r
execQueryPlan' (vars, rs) (_, QPFalse2) = (vars, closeResultStream rs)
