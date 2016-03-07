{-# LANGUAGE ExistentialQuantification, MultiParamTypeClasses, FunctionalDependencies #-}
module QueryPlan where
    

import ResultStream
import FO.Data
import FO.Domain
import FO.FunSat

import Prelude  hiding (lookup)
import Data.Map.Strict (Map, (!), empty, member, insert, foldrWithKey, foldlWithKey, alter, lookup, fromList, toList, unionWith, unionsWith, intersectionWith, elems, delete)
import Data.List ((\\), intercalate, union, intersect, elem, (!!))
import Control.Monad.Trans.State.Strict (StateT, evalStateT,evalState, get, put, State, runState   )
import Control.Applicative ((<$>), liftA2, (<|>))
import qualified Control.Applicative as Appl
import Data.Convertible.Base
import Control.Monad.Logic
import Data.Maybe
import Data.Monoid  (mempty)

-- query
data Query = Query { select :: [Var], cond :: Formula }
data Insert = Insert { ilits :: [Lit], icond :: Formula }

-- database
class Database_ db m row | db -> m row where
    dbBegin :: db -> m ()
    dbCommit :: db -> m ()
    dbRollback :: db -> m ()
    getName :: db -> String
    getPreds :: db -> [Pred]
    -- domainSize function is a function from arguments to domain size
    -- it is used to compute the optimal query plan
    domainSize :: db -> DomainSizeMap -> DomainSizeFunction m Atom
    doQuery :: db -> Query -> ResultStream m row
    -- filter takes a result stream, a list of input vars, a query, and generate a result stream
    doFilter :: db -> Query -> ResultStream m row -> ResultStream m row
    doInsert :: db -> Insert -> m [Int]
    supported :: db -> Formula -> Bool

-- https://wiki.haskell.org/Existential_type#Dynamic_dispatch_mechanism_of_OOP
data Database m  row = forall db. Database_ db m  row => Database { unDatabase :: db }

data QueryPlan = Exec Formula [Int] 
                     | QPAnd (QueryPlan ) (QueryPlan)
                     | QPOr (QueryPlan ) (QueryPlan )
                     | QPNot (QueryPlan)
                     | QPExists Var (QueryPlan)
                     | QPTrue
                     | QPFalse
                     
instance Show QueryPlan where
    show (Exec f dbs) = "exec " ++ show f ++ " at " ++ show dbs
    show (QPAnd qp1 qp2) = "(" ++ show qp1 ++ " filter by " ++ show qp2 ++ ")"
    show (QPOr qp1 qp2) = "(" ++ show qp1 ++ " union with " ++ show qp2 ++ ")"
    show (QPNot qp1) = "(not " ++ show qp1 ++ ")"
    show (QPExists v qp1) = "(exists " ++ show v ++ " where " ++ show qp1 ++  ")"
    show (QPTrue) = "(generate [[]]"
    show (QPFalse) = "(generate [])"
    
    
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
    

    
simplifyQueryPlan :: QueryPlan -> QueryPlan
simplifyQueryPlan qp@(Exec _ _) = qp
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
            
            
optimizeQueryPlan :: (Monad m ) => [Database m row] -> QueryPlan -> QueryPlan
optimizeQueryPlan dbsx qp@(Exec formula dbs) = qp
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
            (Exec formula1 dbs1, QPAnd qp21 qp22) ->
                QPAnd (optimizeQueryPlan dbsx (QPAnd qp1' qp21)) qp22
            (QPAnd qp11 qp12, Exec formula2 dbs2) ->
                QPAnd qp11 (optimizeQueryPlan dbsx (QPAnd qp12 qp2'))
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
        
        
checkQueryPlan' :: (Monad m ) => [Database m row] -> QueryPlan -> m (Either (Formula, DomainSizeMap) DomainSizeMap)
checkQueryPlan' dbs qp = 
    checkQueryPlan dbs empty qp
         
checkQueryPlan :: (Monad m) => [Database m row] -> DomainSizeMap -> QueryPlan -> m (Either (Formula, DomainSizeMap) DomainSizeMap)
checkQueryPlan dbs map1 (Exec formula (x : _)) = do
    let fvs = freeVars formula
    map2 <- domainSizeFormula map1 (dbs !! x) formula Pos
    return (if all (\v -> 
               case lookupDomainSize v map2 of
                    Bounded _ -> True
                    Unbounded -> False) fvs then
                Right map2
            else
                Left (formula, map2))
            
checkQueryPlan dbs map1 (QPOr qp1 qp2) = do
    map2 <- checkQueryPlan dbs map1 qp1 
    map3 <- checkQueryPlan dbs map1 qp2 
    return (case map2 of
        Left f -> Left f
        Right map2' -> case map3 of
            Left f -> Left f
            Right map3 -> Right (unionWith dmax map2' map3))
            
checkQueryPlan dbs map1 (QPAnd qp1 qp2) = do
    map2 <- checkQueryPlan dbs map1 qp1
    case map2 of 
         Left f -> return (Left f)
         Right map2' -> checkQueryPlan dbs map2' qp2

checkQueryPlan dbs map1 (QPExists v qp1) = do
    r <- checkQueryPlan dbs map1 qp1
    return (do
        map2 <- r
        return (case lookup v map1 of
             Nothing -> delete v map2
             Just b -> insert v b map2))

checkQueryPlan dbs map1 (QPNot qp1) = do
    r <- checkQueryPlan dbs map1 qp1
    return (do
        map2 <- r
        return map1)


execQueryPlan :: (Monad m, ResultRow row) => [Database m row] -> Maybe ([Var], ResultStream m row) -> QueryPlan -> ([Var], ResultStream m row     )                                                                
execQueryPlan dbs r (Exec formula (x : _)) =
    if x >= length dbs || x < 0 then
        error "index out of range"
    else case if x >= length dbs then error (show (length dbs) ++ " " ++ show x) else dbs !! x of
        Database db ->
            case r of
                Nothing -> let vars = (freeVars formula) in (vars , doQuery db (Query vars formula))
                Just (vars, rs) -> let vars' = (vars `union` freeVars formula) in (vars', doFilter db (Query vars' formula) rs)
execQueryPlan dbs r (QPOr qp1 qp2) =
    let (vars1, rs1) = execQueryPlan dbs r qp1
        (vars2, rs2) = execQueryPlan dbs r qp2 
        vars = vars1 `union` vars2 in
        (vars, transformResultStream vars1 vars rs1 <|> transformResultStream vars2 vars rs2)
execQueryPlan dbs r (QPAnd qp1 qp2) =
    let r1 = execQueryPlan dbs r qp1 in
        execQueryPlan dbs (Just r1) qp2
execQueryPlan dbs r (QPNot qp) = -- assume no unbounded vars under not
    case r of
        Nothing -> 
            ([], filterResultStream (pure mempty) (\_ -> do
                let (_, rs2) = execQueryPlan dbs Nothing qp
                isResultStreamEmpty rs2)) 
        Just (vars, rs) -> 
            (vars, filterResultStream rs (\row -> do
                let (_, rs2) = execQueryPlan dbs (Just (vars, (pure row))) qp
                isResultStreamEmpty rs2)) 
execQueryPlan dbs r (QPExists v qp) = -- assume no unbounded vars under exists except v
    case r of
        Nothing -> 
            ([], filterResultStream (pure mempty) (\_ -> do
                let (_, rs2) = execQueryPlan dbs Nothing qp
                emp <- isResultStreamEmpty rs2
                return (not emp)))
        Just (vars, rs) -> 
            (vars, filterResultStream rs (\row -> do
                let (_, rs2) = execQueryPlan dbs (Just (vars, (pure row))) qp
                emp <- isResultStreamEmpty rs2
                return (not emp))) 

execQueryPlan dbs r (QPTrue) = ([], pure mempty)
execQueryPlan dbs r (QPFalse) = ([], Appl.empty)

