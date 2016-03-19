{-# LANGUAGE ExistentialQuantification, MultiParamTypeClasses, FunctionalDependencies, TypeSynonymInstances, FlexibleInstances, FlexibleContexts #-}
module QueryPlan where


import ResultStream
import FO.Data
import FO.Domain
import FO.FunSat

import Prelude  hiding (lookup)
import Data.Map.Strict (Map, (!), empty, member, insert, foldrWithKey, foldlWithKey, alter, lookup, fromList, toList, unionWith, unionsWith, intersectionWith, elems, delete)
import Data.List ((\\), intercalate, union, intersect, elem, (!!), find)
import Control.Monad.Trans.State.Strict (StateT, evalStateT,evalState, execState, get, put, State, runState   )
import Control.Monad.Except
import Control.Applicative ((<$>), liftA2, (<|>))
import qualified Control.Applicative as Appl
import Data.Convertible.Base
import Control.Monad.Logic
import Data.Maybe
import Data.Monoid  (mempty, (<>))
import Data.Tree
import Control.Monad.Writer.Class
import Debug.Trace

-- result value
data ResultValue = StringValue String | IntValue Int | Null deriving (Eq , Show)

instance Convertible ResultValue Expr where
    safeConvert (StringValue s) = Right (StringExpr s)
    safeConvert (IntValue i) = Right (IntExpr i)
    safeConvert _ = Left (ConvertError "" "" "" "")

instance Convertible Expr ResultValue where
    safeConvert (StringExpr s) = Right (StringValue s)
    safeConvert (IntExpr i) = Right (IntValue i)
    safeConvert _ = Left (ConvertError "" "" "" "")

-- result row
type MapResultRow = Map Var ResultValue

instance ResultRow MapResultRow where
    transform vars vars2 map1 = foldr (\var map2 -> case lookup var map1 of
                                                        Nothing -> insert var Null map1
                                                        Just rv -> insert var rv map1) empty vars2
-- query
data Query = Query { select :: [Var], cond :: Formula }
data Insert = Insert { ilits :: [Lit], icond :: Formula }

instance Show Query where
    show (Query vars disjs) = show disjs ++ " return " ++ intercalate " " (map show vars)

instance Show Insert where
    show (Insert atoms disjs) = show disjs ++ " insert " ++ intercalate " " (map show atoms)

-- database
class Database_ db m row | db -> m row where
    dbBegin :: db -> m ()
    dbCommit :: db -> m ()
    dbRollback :: db -> m ()
    dbBeginCommand :: db -> m ()
    dbEndCommand :: db -> m ()
    getName :: db -> String
    getPreds :: db -> [Pred]
    -- domainSize function is a function from arguments to domain size
    -- it is used to compute the optimal query plan
    domainSize :: db -> DomainSizeMap -> DomainSizeFunction m Atom
    -- filter takes a result stream, a list of input vars, a query, and generate a result stream
    doQuery :: db -> Query -> [Var] -> ResultStream m row -> ResultStream m row
    doInsert :: db -> Insert -> [Var] -> ResultStream m row -> ResultStream m row
    supported :: db -> Formula -> Bool
    supportedInsert :: db -> Formula -> [Lit] -> Bool
    translateQuery :: db -> Query -> [Var] -> row -> (String, [Var])
    translateInsert :: db -> Insert -> [Var] -> row -> (String, [Var])

-- https://wiki.haskell.org/Existential_type#Dynamic_dispatch_mechanism_of_OOP
data Database m  row = forall db. Database_ db m  row => Database { unDatabase :: db }

data InsertPlan = InsertPlan [Lit] Formula [Int]
data QueryPlan = Exec Formula [Int]
                | ExecInsert  [InsertPlan]
                | QPAnd (QueryPlan) (QueryPlan)
                | QPOr (QueryPlan) (QueryPlan )
                | QPNot (QueryPlan)
                | QPExists Var (QueryPlan)
                | QPTrue
                | QPFalse

class ToTree a where
    toTree :: a -> Tree String

instance ToTree InsertPlan where
    toTree (InsertPlan lits formula dbs) = Node ("insert " ++ show lits ++ " where " ++ show formula ++ " at " ++ show dbs) []
instance ToTree QueryPlan where
    toTree (Exec f dbs) = Node ("exec " ++ show f ++ " at " ++ show dbs) []
    toTree (ExecInsert ips) = Node "exec" (map toTree ips)
    toTree (QPAnd qp1 qp2) = Node "and" [toTree qp1, toTree qp2]
    toTree (QPOr qp1 qp2) = Node "union" [toTree qp1, toTree qp2]
    toTree (QPNot qp1) = Node "not" [toTree qp1]
    toTree (QPExists v qp1) = Node ("exists " ++ show v ++ " where") [toTree qp1]
    toTree (QPTrue) = Node ("(generate [[]]") []
    toTree (QPFalse) = Node ("(generate [])") []


true = Conjunction []

formulaToQueryPlan :: (Monad m)=> [Database m row] -> Formula -> QueryPlan
formulaToQueryPlan dbs formula@(Atomic (Atom pred  _)) =
    let aggr = case pred of
            Pred _ (PredType EssentialPred _) -> False
            _ -> True
    in
        if aggr then
            foldl (\qp x -> case dbs !! x of
                                Database db ->
                                    if pred `elem` getPreds db then
                                        QPOr qp (Exec formula [x])
                                    else
                                        qp) QPFalse [0..(length dbs-1)]
        else
            Exec formula (filter (\x -> case dbs !! x of
                                    Database db -> pred `elem` getPreds db) [0..(length dbs-1)])
formulaToQueryPlan dbs formula@(Disjunction disjuncts) = foldl QPOr QPFalse (map (formulaToQueryPlan dbs) disjuncts)
formulaToQueryPlan dbs formula@(Conjunction conjuncts) = foldl QPAnd QPTrue (map (formulaToQueryPlan dbs) conjuncts)
formulaToQueryPlan dbs (Exists v formula) = QPExists v (formulaToQueryPlan dbs formula)
formulaToQueryPlan dbs (Not formula) = QPNot (formulaToQueryPlan dbs formula)

type InsertMap = Map Pred ([Int], [Int])

insertToQueryPlan :: (Monad m)=> [Database m row] -> InsertMap -> Insert -> QueryPlan
insertToQueryPlan dbs insmap (Insert lits formula) =
    let p pred x = case dbs !! x of Database db -> pred `elem` getPreds db
        xs = [0..length dbs - 1]
        findPredicateDB pred = case lookup pred insmap of
            Just xs -> xs
            Nothing -> ([fromMaybe (error ("insertToQueryPlan: cannot find predicate " ++ show pred)) (find (p pred) xs)],  filter (p pred) xs)
        litToQP lit@(Lit sign (Atom pred _)) =
            let xs = (case sign of
                        Pos -> fst
                        Neg -> snd) (findPredicateDB pred) in
                InsertPlan [lit] true (filter (\x ->
                    case dbs !! x of
                         Database db ->
                            supportedInsert db true [lit]) xs)
    in
        QPAnd (formulaToQueryPlan dbs formula) (ExecInsert (map litToQP lits))


simplifyQueryPlan :: QueryPlan -> QueryPlan
simplifyQueryPlan qp@(Exec _ _) = qp
simplifyQueryPlan qp@(ExecInsert _) = qp
simplifyQueryPlan (QPOr qp1 qp2) =
    let qp1' = simplifyQueryPlan qp1
        qp2' = simplifyQueryPlan qp2 in
        case (qp1', qp2') of
            (QPTrue, _) -> QPTrue
            (QPFalse, _) -> qp2'
            (_, QPTrue) -> QPTrue
            (_, QPFalse) -> qp1'
            (_, _) -> QPOr qp1' qp2'
simplifyQueryPlan (QPAnd qp1 qp2) =
    let qp1' = simplifyQueryPlan qp1
        qp2' = simplifyQueryPlan qp2 in
        case (qp1', qp2') of
            (QPTrue, _) -> qp2'
            (QPFalse, _) -> QPFalse
            (_, QPTrue) -> qp1'
            (_, QPFalse) -> QPFalse
            (_, _) -> QPAnd qp1' qp2'
simplifyQueryPlan (QPExists v qp1) =
    let qp1' = simplifyQueryPlan qp1 in
        QPExists v qp1'
simplifyQueryPlan (QPNot qp1) =
    let qp1' = simplifyQueryPlan qp1 in
        case qp1' of
            QPTrue -> QPFalse
            QPFalse -> QPTrue
            _ -> QPNot qp1'
simplifyQueryPlan (QPTrue) = QPTrue
simplifyQueryPlan (QPFalse) = QPFalse

superset :: (Eq a) => [a] -> [a] -> Bool
superset s = all (`elem` s)

optimizeQueryPlan :: (Monad m ) => [Database m row] -> QueryPlan -> QueryPlan
optimizeQueryPlan dbsx qp@(Exec formula dbs) = qp

optimizeQueryPlan dbsx qp@(ExecInsert (ip1@(InsertPlan lits1 formula1  dbs1) : ips1@(InsertPlan lits2 formula2  dbs2 : ips ) )) | formula1 == formula2 =
    let dbs = dbs1 `intersect` dbs2
        lits = lits1 ++ lits2
        dbs' = filter (\x -> case dbsx !! x of (Database db) -> supportedInsert db formula1 lits) dbs in
        if null dbs' then
            let (ExecInsert ips1') = optimizeQueryPlan dbsx (ExecInsert ips1) in
                ExecInsert (ip1 : ips1')
        else
            let dbs1' = dbs1 \\ dbs'
                dbs2' = dbs2 \\ dbs'
                (ExecInsert ips') = optimizeQueryPlan dbsx (ExecInsert (InsertPlan lits formula1 dbs' : ips)) in
                ExecInsert ((if null dbs1' then [] else [InsertPlan lits1 formula1  dbs1']) ++ (if null dbs2' then [] else [InsertPlan lits2 formula2  dbs2']) ++ ips')
optimizeQueryPlan dbsx (ExecInsert (ip : ips ))  =
    let (ExecInsert ips') = optimizeQueryPlan dbsx (ExecInsert ips) in
        ExecInsert (ip : ips')
optimizeQueryPlan dbsx qp@(ExecInsert _ ) = qp

optimizeQueryPlan dbsx (QPOr qp1 qp2) =
    let qp1' = optimizeQueryPlan dbsx qp1
        qp2' = optimizeQueryPlan dbsx qp2 in
        case (qp1', qp2') of
            (Exec formula1 dbs1, Exec formula2 dbs2) ->
                let dbs = dbs1 `intersect` dbs2
                    dbs' = filter (\x -> case dbsx !! x of (Database db) -> supported db (Disjunction [formula1, formula2])) dbs in
                    if null dbs' then
                        QPOr qp1' qp2'
                    else
                        Exec (Disjunction [formula1, formula2]) dbs'
            (Exec formula1 dbs1, QPOr qp21 qp22) ->
                QPOr (optimizeQueryPlan dbsx (QPOr qp1' qp21)) qp22
            (QPOr qp11 qp12, Exec formula2 dbs2) ->
                QPOr qp11 (optimizeQueryPlan dbsx (QPOr qp12 qp2'))
            (QPOr qp11 qp12, QPOr qp21 qp22) ->
                QPOr (QPOr qp11 (optimizeQueryPlan dbsx (QPOr qp12 qp21))) qp22
            _ -> QPOr qp1' qp2'
optimizeQueryPlan dbsx (QPAnd qp1 qp2) =
    let qp1' = optimizeQueryPlan dbsx qp1
        qp2' = optimizeQueryPlan dbsx qp2 in
        case (qp1', qp2') of
            (Exec formula1 dbs1, Exec formula2 dbs2) ->
                let dbs = dbs1 `intersect` dbs2
                    dbs' = filter (\x -> case dbsx !! x of (Database db) -> supported db (Conjunction [formula1, formula2])) dbs in
                    if null dbs' then
                        QPAnd qp1' qp2'
                    else
                        Exec (Conjunction [formula1, formula2]) dbs'
            (Exec formula1 dbs1, ExecInsert [InsertPlan lits formula2 dbs2]) ->
                if (dbs1 `superset` dbs2) && all (\x -> case dbsx !! x of (Database db) -> supportedInsert db (Conjunction [formula1, formula2]) lits) dbs2 then
                        ExecInsert [InsertPlan lits (Conjunction [formula1, formula2]) dbs2]
                else
                        QPAnd qp1' qp2'
            (Exec formula1 dbs1, QPAnd qp21 qp22) ->
                QPAnd (optimizeQueryPlan dbsx (QPAnd qp1' qp21)) qp22
            (QPAnd qp11 qp12, Exec formula2 dbs2) ->
                QPAnd qp11 (optimizeQueryPlan dbsx (QPAnd qp12 qp2'))
            (QPAnd qp11 qp12, ExecInsert _) ->
                QPAnd qp11 (optimizeQueryPlan dbsx (QPAnd qp12 qp2'))
            (QPOr qp11 qp12, ExecInsert _) ->
                QPOr (optimizeQueryPlan dbsx (QPAnd qp11 qp2')) (optimizeQueryPlan dbsx (QPAnd qp12 qp2'))
            (QPAnd qp11 qp12, QPAnd qp21 qp22) ->
                QPAnd (QPAnd qp11 (optimizeQueryPlan dbsx (QPAnd qp12 qp21))) qp22
            _ -> QPAnd qp1' qp2'
optimizeQueryPlan dbsx (QPExists v qp1) =
    let qp1' = optimizeQueryPlan dbsx qp1 in
        case qp1' of
            Exec formula1 dbs ->
                        let dbs' = filter (\x -> case dbsx !! x of (Database db) -> supported db (Exists v formula1)) dbs  in
                            if null dbs' then
                                QPExists v qp1'
                            else
                                Exec (Exists v formula1) dbs'
            _ -> QPExists v qp1'
optimizeQueryPlan dbsx (QPNot qp1) =
    let qp1' = optimizeQueryPlan dbsx qp1 in
        case qp1' of
            Exec formula1 dbs ->
                        let dbs' = filter (\x -> case dbsx !! x of (Database db) -> supported db (Not formula1)) dbs  in
                            if null dbs' then
                                QPNot qp1'
                            else
                                Exec (Not formula1) dbs'
            _ -> QPNot qp1'


domainSizeFormula :: (Monad m) => DomainSizeMap -> Database m row -> Formula -> Sign -> m DomainSizeMap
domainSizeFormula map1 (Database db_) formula sign = do
        map1' <- determinedVars (domainSize db_ map1) sign formula
        return (mmin map1 map1')


checkQueryPlan' :: (Monad m ) => [Database m row] -> QueryPlan -> ExceptT (Formula, DomainSizeMap) m DomainSizeMap
checkQueryPlan' dbs qp = do
    checkQueryPlan dbs empty qp

checkQueryPlan :: (Monad m) => [Database m row] -> DomainSizeMap -> QueryPlan -> ExceptT (Formula, DomainSizeMap) m DomainSizeMap
checkQueryPlan dbs map1 (Exec formula []) = throwError (formula, map1)
checkQueryPlan dbs map1 (Exec formula (x : _)) = do
    let fvs = freeVars formula
    map2 <- lift (domainSizeFormula map1 (dbs !! x) formula Pos)
    if all (\v ->
        case lookupDomainSize v map2 of
            Bounded _ -> True
            Unbounded -> False) fvs
        then return map2
        else throwError (formula, map2)

checkQueryPlan dbsx map1 (ExecInsert ips) = do
    let litToFormula lit = case lit of
                                       Lit Pos atom -> Atomic atom
                                       Lit Neg atom -> Not (Atomic atom)
    mapM (\(InsertPlan lits formula dbs)  -> do
        if null dbs
            then throwError (litToFormula (head lits), map1)
            else
                mapM (\db -> do
                    mapM (\lit ->  do
                        map1' <- checkQueryPlan dbsx map1 (Exec formula [db])
                        let fvs = freeVars lit
                        if all (\v ->
                            case lookupDomainSize v map1 of
                                Bounded _ -> True
                                Unbounded -> False) fvs
                            then return ()
                            else throwError (litToFormula lit, map1)) lits) dbs) ips
    return map1

checkQueryPlan dbs map1 (QPOr qp1 qp2) = do
    map2 <- checkQueryPlan dbs map1 qp1
    map3 <- checkQueryPlan dbs map1 qp2
    return (intersectionWith (+) map2 map3)

checkQueryPlan dbs map1 (QPAnd qp1 qp2) = do
    map2 <- checkQueryPlan dbs map1 qp1
    checkQueryPlan dbs map2 qp2

checkQueryPlan dbs map1 (QPExists v qp1) = do
    map2 <- checkQueryPlan dbs map1 qp1
    return (case lookup v map1 of
             Nothing -> delete v map2
             Just b -> insert v b map2)

checkQueryPlan dbs map1 (QPNot qp1) = do
    map2 <- checkQueryPlan dbs map1 qp1
    return map1

execQueryPlan :: (Monad m, ResultRow row) => [Database m row] -> ([Var], ResultStream m row) -> QueryPlan -> ([Var], ResultStream m row     )
execQueryPlan dbs (vars, rs) (Exec formula (x : _)) =
    if x >= length dbs || x < 0 then
        error "index out of range"
    else case dbs !! x of
        Database db -> let fvs = freeVars formula \\ vars
                           vars' = (vars `union` fvs) in
                           (vars', do
                               row <- rs
                               let qu = (Query fvs formula)
                               trace ("exec at " ++ show x ++ " " ++ show (translateQuery db qu vars row)) $ return ()
                               row2 <- doQuery db qu vars (pure row)
                               return (row <> row2)
                               )
execQueryPlan dbs (vars, rs) (ExecInsert ips) =
        (vars, do
            row <- rs
            mapM_ (\(InsertPlan lits formula xs) ->
                mapM (\x ->
                    if x >= length dbs || x < 0 then
                        error "index out of range"
                    else
                        case dbs !! x of
                            Database db -> do
                                let qu = (Insert lits formula)
                                trace ("exec at " ++ show x ++ " " ++ show (translateInsert db qu vars row)) $ return ()
                                doInsert db qu vars (pure row)) xs) ips
            return row)
execQueryPlan dbs r (QPOr qp1 qp2) =
    let (vars1, rs1) = execQueryPlan dbs r qp1
        (vars2, rs2) = execQueryPlan dbs r qp2
        vars = vars1 `union` vars2 in
        (vars, transformResultStream vars1 vars rs1 <|> transformResultStream vars2 vars rs2)
execQueryPlan dbs r (QPAnd qp1 qp2) =
    let r1 = execQueryPlan dbs r qp1 in
        execQueryPlan dbs r1 qp2
execQueryPlan dbs (vars, rs) (QPNot qp) = -- assume no unbounded vars under not
            (vars, filterResultStream rs (\row -> do
                let (_, rs2) = execQueryPlan dbs (vars, (pure row)) qp
                isResultStreamEmpty rs2))
execQueryPlan dbs (vars, rs) (QPExists v qp) = -- assume no unbounded vars under exists except v
            (vars, filterResultStream rs (\row -> do
                let (_, rs2) = execQueryPlan dbs  (vars, (pure row)) qp
                emp <- isResultStreamEmpty rs2
                return (not emp)))

execQueryPlan dbs r (QPTrue) = ([], pure mempty)
execQueryPlan dbs r (QPFalse) = ([], Appl.empty)
