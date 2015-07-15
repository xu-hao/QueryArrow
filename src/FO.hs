{-# LANGUAGE TypeFamilies, MultiParamTypeClasses, FunctionalDependencies, ExistentialQuantification, FlexibleInstances, OverloadedStrings #-}
module FO where

import Data.Map.Strict (Map, (!), empty, member, insert, foldrWithKey, alter)
import Control.Applicative ((<$>))
import Data.Functor.Identity
import Data.List ((\\))
import Control.Monad (liftM2)
import Control.Monad.Trans.Writer.Strict (WriterT,tell,runWriterT)
import Control.Monad.IO.Class (MonadIO, liftIO)
import Control.Monad.Trans.State.Strict (StateT)
import Control.Monad.Trans.Class (lift)

-- variable types
type Type = String

-- predicate types
type PredType = [Type]

-- predicate
data Pred = Pred { predName :: String, predType :: PredType} deriving Eq

-- variables
type Var = String

-- expression
data Expr = VarExpr Var | IntExpr Int | StringExpr String deriving (Eq, Ord)

-- result value
data ResultValue = StringValue String | IntValue Int deriving (Eq , Show)

-- atoms
data Atom = Atom { atomPred :: Pred, atomArgs :: [Expr] } 
          | Exists { boundvar :: Var, conjunction :: [Disjunction] } deriving Eq

-- sign of literal
data Sign = Pos | Neg deriving Eq

-- literals
data Lit = Lit { sign :: Sign,  atom :: Atom } deriving Eq

-- disjunction
type Disjunction = [Lit]

-- query          
data Query = Query { select :: [Var], cond :: [Disjunction] }
 
-- result stream see Takusen
type Iteratee row seed m = row -> seed -> m (Either seed seed)

type Filter a = a -> a

newtype ResultStream m seed row = ResultStream {runResultStream :: Iteratee row seed m -> seed -> m seed}
    
-- database
class Database_ db m  row | db -> m  row where
    dbStartSession :: db -> m ()
    dbStopSession :: db -> m ()
    getName :: db -> String
    getPreds :: db -> [Pred] 
    -- domainSize function is a function from arguments to domain size
    -- it is used to compute the optimal query plan
    domainSize :: db -> (Var -> DomainSize) -> Sign -> Pred -> [Expr] -> DomainSize
    doQuery :: db -> Query -> ResultStream  m seed row
    -- filter takes a result stream, a list of input vars, a query, and generate a result stream
    doFilter :: db -> Query -> ResultStream m seed row -> ResultStream m seed row
    
-- https://wiki.haskell.org/Existential_type#Dynamic_dispatch_mechanism_of_OOP
data Database m  row = forall db. Database_ db m  row => Database { unDatabase :: db }

-- predicate map
type PredMap = Map String Pred

-- map from predicate name to database names
type PredDBMap = Map String [String]

appendNew :: forall a. Eq a => [a] -> [a] -> [a]
appendNew xs ys = xs ++ (ys \\ xs)

unique :: [a] -> a
unique as = if length as /= 1 
        then error "More than one primary parameters and nonzero secondary parameters"
        else head as        

allTrue :: forall a. (a -> Bool) -> [a] -> Bool
allTrue f = foldr ((&&) . f) True

someTrue :: forall a. (a -> Bool) -> [a] -> Bool
someTrue f = foldr ((||) . f) False

isConst :: Expr -> Bool
isConst (VarExpr _) = False
isConst _ = True

type DomainSize = Maybe Int

dmin :: DomainSize -> DomainSize -> DomainSize
dmin Nothing Nothing = Nothing
dmin Nothing b = b
dmin a _ = a

dmax :: DomainSize -> DomainSize -> DomainSize
dmax = liftM2 max

dmul :: DomainSize -> DomainSize -> DomainSize
dmul = liftM2 (*)

dmaxList :: [DomainSize] -> DomainSize
dmaxList = foldr dmax (Just 0)
exprDomainSize :: (Var -> DomainSize) -> DomainSize -> Expr -> DomainSize
exprDomainSize varDomainSize maxval expr = case expr of
    VarExpr var -> dmin (varDomainSize var) maxval 
    _ -> Just 1
 
-- free variables
class FreeVars a where
    freeVars :: a -> [Var]
    determinedVars :: a -> [Var]
    
instance FreeVars Expr where
    freeVars (VarExpr var) = [var]
    freeVars _ = []
    determinedVars = freeVars
    
instance FreeVars Atom where
    freeVars (Atom _ theargs) = foldl (\fvs expr -> 
        fvs `appendNew` freeVars expr) [] theargs
    freeVars (Exists var conj) = freeVars conj \\ [var]
        
    determinedVars a = case a of
        Atom _ _ -> freeVars a
        Exists var conj -> determinedVars conj \\ [var]
        
instance FreeVars Lit where
    freeVars (Lit _ theatom) = freeVars theatom
    determinedVars lit = case lit of
        Lit Pos _ -> freeVars lit
        Lit Neg _ -> [] -- negative literal doesn't determine any variables
    
instance FreeVars [Disjunction] where
    freeVars = foldl (\fvs disj ->
        fvs `appendNew` (union . map freeVars) disj) []
    determinedVars = foldl (\fvs disj ->
        fvs `appendNew` (intersect . map freeVars) disj) []
            
intersect :: Eq a => [[a]] -> [a]
intersect [] = error "can't intersect zero lists"
intersect (hd : tl) = foldl (\ as bs -> [ x | x <- as, x `elem` bs ]) hd tl
             
union :: Eq a => [[a]] -> [a]
union = foldl appendNew []

type Report = [[Disjunction]]

-- exec query from dbname
getAllResults :: (Monad m) => [Database m  row] -> Query -> m ([row], Report)
getAllResults dbs query = do
    (stream, report) <- execQuery dbs query
    results <- runResultStream stream (\row seed  -> return (Right (seed ++ [row]))) []
    return (results, report)

execQuery :: (Monad m) => [Database m  row] -> Query -> m (ResultStream m seed row, Report)
execQuery dbs query  = do
    mapM_ (\(Database db)->dbStartSession db) dbs
    (stream, report) <- runWriterT (execQuery' dbs query)
    return (ResultStream (\ iteratee seed -> do 
        seednew <- runResultStream stream iteratee seed
        mapM_ (\(Database db)->dbStopSession db) dbs
        return seednew), report)
    
execQuery' :: (Monad m) => [Database m  row] -> Query -> WriterT Report m (ResultStream m seed row)  
execQuery' dbs (Query vars lits) = doTheQuery >>= (\(x,y,z) -> doTheFilters x y z)  where
    doTheQuery = 
        let (dbsnew, effectivelits, restLits) = findEffectiveDisjsPrefix [] (dbs, [], lits)
            dtvars = determinedVars effectivelits
            freevarsnew = freeVars restLits in
            if null effectivelits 
                then error ("can't find effective literals, try reordering the literals: " ++ show lits)
                else case head dbsnew of 
                    Database db -> do
                        tell [effectivelits]
                        return (restLits, dtvars, doQuery db (Query (vars `appendNew` freevarsnew) effectivelits))
        
    doTheFilters [] _ results2 = return results2
    doTheFilters lits2  dtvars results2 = do
        (restLitsnew, dtvarsnew, resultsnew) <- doTheFilter lits2  dtvars results2 
        doTheFilters restLitsnew  dtvarsnew resultsnew
            
    doTheFilter lits2  dtvars results2 = 
        let (dbsnew, effectivelits, restLits) = findEffectiveDisjsPrefix dtvars (dbs, [], lits2)
            dtvarsnew = dtvars `appendNew` determinedVars effectivelits
            freevarsnew = freeVars restLits in
            if null effectivelits 
                then error ("can't find effective literals, try reordering the literals 2: " ++ show dtvars ++ show lits2)
                else case head dbsnew of 
                    Database db -> do
                        tell [effectivelits] 
                        return (restLits, dtvarsnew, doFilter db (Query (vars `appendNew` freevarsnew) effectivelits) results2)


constructPredMap :: [Database m  row] -> PredMap
constructPredMap = foldr addPredFromDBToMap empty where
    addPredFromDBToMap (Database db) predmap = foldr addPredToMap predmap preds where
        addPredToMap thepred = insert (predName thepred) thepred
        preds = getPreds db
            
-- construct map from predicates to db names
constructPredDBMap :: [Database m  row] -> PredDBMap
constructPredDBMap = foldr addPredFromDBToMap empty where
    addPredFromDBToMap (Database db) predmap = foldr addPredToMap predmap preds where
        dbname = getName db
        addPredToMap thepred = alter alterValue (predName thepred) where
            alterValue Nothing = Just [dbname]
            alterValue (Just dbnames) = Just (dbname : dbnames) 
        preds = getPreds db

-- a literal is called "effective" only when it has a finite domain size.
-- a variable is called "determined" only when it has a finite domain size.
-- the first stage is a query and subsequent stages are filters
-- each stage T is associated with a database D
-- a set S of literals is assigned a stage T. Denote by A -> T -> D. The assignment must satisfy the following
-- for each L in S
-- (1) the predicate in L is provided by D and
-- (2) all variable in L are determined by S and D or previous stages 
-- a variable is determined by S and D, iff
-- there is a literal L in S such that
-- (1) x in fv(L) and 
-- (2) L has a finite domain size in D given the variables determined in previous stages

-- given 
-- a list of variables that has already been determined
-- a list of databases
-- a list of effective literals 
-- a list of literal
-- find the longest prefix of the literal list the literals in which become effective in at least one databases
findEffectiveDisjsPrefix :: [Var] -> ([Database m  row], [Disjunction], [Disjunction]) -> ([Database m  row], [Disjunction], [Disjunction])
findEffectiveDisjsPrefix _ (dbs, effective, []) = (dbs, effective, [])
findEffectiveDisjsPrefix determinedvars (dbs, effective, disj : rest) =
    if null dbsnew 
        then (dbs, effective, disj : rest) 
        else findEffectiveDisjsPrefix (determinedvars `appendNew` intersect (fmap determinedVars disj)) (dbsnew, effective ++ [disj], rest) where
        dbsnew = filter isDisjEffective dbs
        isDisjEffective db = allTrue (isLitEffective db) disj
        isLitEffective (Database db) (Lit thesign theatom) = case theatom of
            Atom thepred theargs -> case domainSize db (\ var -> 
                if var `elem` determinedvars 
                    then Just 1 -- here we don't have to calculate the actually domain size 
                    else Nothing) thesign thepred theargs of
                        Nothing -> False
                        _ -> True
            Exists _ conj -> 
                let (_, prefix, _) = findEffectiveDisjsPrefix determinedvars (dbs, [], conj) in
                    length prefix == length conj -- only allows subqueries that are in one database
                
                
splitPosNegLits :: [Lit] -> ([Atom], [Atom])
splitPosNegLits = foldr (\ (Lit thesign theatom) (pos, neg) -> case thesign of 
    Pos -> (theatom : pos, neg)
    Neg -> (pos, theatom : neg)) ([],[])
    
    
-- example MapDB

-- result row
type MapResultRow = Map Var ResultValue

filterResults :: [Var] -> (Pred-> ResultValue -> ResultValue -> MapResultRow -> [MapResultRow])
    -> (Pred->Var -> ResultValue -> MapResultRow -> [MapResultRow])
    -> (Pred->ResultValue -> Var -> MapResultRow -> [MapResultRow])
    -> (Pred->Var -> Var -> MapResultRow -> [MapResultRow])
    -> ([Var] -> Var -> [Disjunction] -> MapResultRow -> [MapResultRow])
    -> (Pred-> ResultValue -> ResultValue -> MapResultRow -> [MapResultRow])
    -> ([Var] -> Var -> [Disjunction] -> MapResultRow -> [MapResultRow])
    -> [MapResultRow]
    -> [Disjunction]
    -> [MapResultRow]
filterResults freevars filterBy expand1 expand2 expand12 exists excludeBy notExists = 
    foldl (filterResultsDisj freevars filterBy expand1 expand2 expand12 exists excludeBy notExists)

filterResultsDisj :: [Var] -> (Pred-> ResultValue -> ResultValue -> MapResultRow -> [MapResultRow])
    -> (Pred->Var -> ResultValue -> MapResultRow -> [MapResultRow])
    -> (Pred->ResultValue -> Var -> MapResultRow -> [MapResultRow])
    -> (Pred->Var -> Var -> MapResultRow -> [MapResultRow])
    -> ([Var] -> Var -> [Disjunction] -> MapResultRow -> [MapResultRow])
    -> (Pred-> ResultValue -> ResultValue -> MapResultRow -> [MapResultRow])
    -> ([Var] -> Var -> [Disjunction] -> MapResultRow -> [MapResultRow])
    -> [MapResultRow]
    -> Disjunction
    -> [MapResultRow]
filterResultsDisj freevars filterBy expand1 expand2 expand12 exists excludeBy notExists results = 
    concatMap (filterResultsLit freevars filterBy expand1 expand2 expand12 exists excludeBy notExists results)
    
filterResultsLit :: [Var] -> (Pred-> ResultValue -> ResultValue -> MapResultRow -> [MapResultRow])
    -> (Pred-> Var -> ResultValue -> MapResultRow -> [MapResultRow])
    -> (Pred-> ResultValue -> Var -> MapResultRow -> [MapResultRow])
    -> (Pred-> Var -> Var -> MapResultRow -> [MapResultRow])
    -> ([Var]-> Var -> [Disjunction] -> MapResultRow -> [MapResultRow])
    -> (Pred-> ResultValue -> ResultValue -> MapResultRow -> [MapResultRow])
    -> ([Var]-> Var -> [Disjunction] -> MapResultRow -> [MapResultRow])
    -> [MapResultRow]
    -> Lit
    -> [MapResultRow]
filterResultsLit _ filterBy expand1 expand2 expand12 _ _ _ results (Lit Pos (Atom thepred args)) =
    concatMap (\resrow -> case args of 
        VarExpr var1 : (VarExpr var2 : _) 
            | var1 `member` resrow ->
                if var2 `member` resrow then
                    filterBy thepred (resrow ! var1) (resrow ! var2) resrow else
                    expand2 thepred (resrow ! var1) var2 resrow
            | var2 `member` resrow -> expand1 thepred var1 (resrow ! var2) resrow
            | otherwise -> expand12 thepred var1 var2 resrow
        VarExpr var1 : StringExpr str2 : _ ->
            if var1 `member` resrow
                then filterBy thepred (resrow ! var1) (StringValue str2) resrow
                else expand1 thepred var1 (StringValue str2) resrow
        StringExpr str1 : VarExpr var2 : _ ->
            if var2 `member` resrow
                then filterBy thepred (StringValue str1) (resrow ! var2) resrow
                else expand2 thepred (StringValue str1) var2 resrow
        StringExpr str1 : StringExpr str2 : _ ->
            filterBy thepred (StringValue str1) (StringValue str2) resrow
        _ -> []) results

filterResultsLit _ _ _ _ _ _ excludeBy _ results (Lit Neg (Atom thepred args)) =
    concatMap (\resrow -> case args of
        VarExpr var1 : VarExpr var2 : _ ->
            if var1 `member` resrow && var2 `member` resrow
                then excludeBy thepred (resrow ! var1) (resrow ! var2) resrow
                else error ("unconstrained variable " ++ var1 ++ " or " ++ var2)
        VarExpr var1 : StringExpr str2 : _ ->
            if var1 `member` resrow
                then excludeBy thepred (resrow ! var1) (StringValue str2) resrow
                else error ("unconstrained variable " ++ var1 ++ " in " ++ show (Lit Neg (Atom thepred args)))
        StringExpr str1 : VarExpr var2 : _ ->
            if var2 `member` resrow
                then excludeBy thepred (StringValue str1) (resrow ! var2) resrow
                else error ("unconstrained variable " ++ var2 ++ " in " ++ show (Lit Neg (Atom thepred args)))
        StringExpr str1 : StringExpr str2 : _ ->
            excludeBy thepred (StringValue str1) (StringValue str2) resrow
        _ -> [resrow]) results

filterResultsLit freevars2 _ _ _ _ exists _ _ results (Lit Pos (Exists var conj)) = 
    concatMap (exists (freevars2 `appendNew` (freeVars conj \\ [var])) var conj) results
    
filterResultsLit freevars2 _ _ _ _ _ _ notExists results (Lit Neg (Exists var conj)) =
    concatMap (notExists freevars2 var conj) results

limitvarsInRow :: [Var] -> MapResultRow -> MapResultRow
limitvarsInRow vars = foldrWithKey (\ var val newresrow -> if var `elem` vars then insert var val newresrow else newresrow) empty

data MapDB = MapDB String String [(ResultValue, ResultValue)] deriving Show

listResultStream :: (Monad m) => [MapResultRow] -> ResultStream m seed MapResultRow
listResultStream results  = ResultStream (\iteratee seed-> 
    foldEitherM (flip iteratee) seed results) where
        foldEitherM _ a [] = return a
        foldEitherM g a (b : bs) = do
            c <- g a b
            case c of
                Left d -> return d
                Right d -> foldEitherM g d bs

instance Database_ MapDB Identity MapResultRow where
    dbStartSession _ = return ()
    dbStopSession _ = return ()
    getName (MapDB name _ _) = name
    getPreds (MapDB _ predname _) = [ Pred predname ["String", "String"] ]
    domainSize db varDomainSize thesign thepred [arg1, arg2] 
        | thepred `elem` getPreds db = case thesign of
            Pos -> case db of 
                MapDB _ _ rows -> 
                    let nrows = length rows
                        d1 = exprDomainSize varDomainSize (Just nrows) arg1 
                        d2 = exprDomainSize varDomainSize (Just nrows) arg2 in
                        dmin (d1 `dmul` d2) (Just nrows)
            Neg -> 
                let d1 = exprDomainSize varDomainSize Nothing arg1 
                    d2 = exprDomainSize varDomainSize Nothing arg2 in
                    d1 `dmul` d2
    domainSize _ _ _ _ _ = Nothing
        
    doQuery db query = doFilter db query (return empty)
    doFilter db (Query vars disjs) stream  = do
        row <- stream 
        limitvarsInRow vars <$> listResultStream (filterResults vars filterBy expand1 expand2 expand12 exists excludeBy notExists [row] disjs) where
            (MapDB _ _ rows) = db
            filterBy :: Pred -> ResultValue -> ResultValue -> MapResultRow -> [MapResultRow]
            filterBy _ str1 str2 resrow = [resrow | (str1, str2) `elem` rows]
            excludeBy _ str1 str2 resrow = [resrow | (str1, str2) `notElem` rows ] 
            exists freevars _ conj resrow =
                case runResultStream (doFilter db (Query freevars conj) (return resrow)) (\row seed -> return (Right (row : seed))) [] of Identity results -> results
            notExists freevars _ conj resrow =
                case runResultStream (doFilter db (Query freevars conj) (return resrow)) (\_ _ -> return (Left [])) [resrow] of Identity results -> results
            expand1 _ var1 str2 resrow = [insert var1 (fst row) resrow | row <- rows, snd row == str2 ] 
            expand2 _ str1 var2 resrow = [insert var2 (snd row) resrow | row <- rows, fst row == str1 ] 
            expand12 _ var1 var2 resrow = [ insert var1 (fst row) (insert var2 (snd row) resrow) | row <- rows ]
                    
-- example EqDB
 
data EqDB = EqDB String
instance Database_ EqDB Identity MapResultRow where
    dbStartSession _ = return ()
    dbStopSession _ = return ()
    getName (EqDB name) = name
    getPreds _ = [ Pred "eq" ["String", "String"] ]
    domainSize db varDomainSize thesign thepred [arg1, arg2] 
        | thepred `elem` getPreds db = 
            let d1 = exprDomainSize varDomainSize Nothing arg1
                d2 = exprDomainSize varDomainSize Nothing arg2 in
                case thesign of
                    Pos -> dmin d1 d2
                    Neg -> dmul d1 d2
    domainSize _ _ _ _ _ = Nothing
        
    doQuery db query = doFilter db query (return empty)
    doFilter db (Query vars disjs) stream = do
        row <- stream
        limitvarsInRow vars <$> listResultStream (filterResults vars filterBy expand1 expand2 expand12 exists excludeBy notExists [row] disjs) where
            filterBy _ str1 str2 resrow = [resrow | str1 == str2]
            excludeBy _ str1 str2 resrow = [resrow | str1 /= str2 ] 
            exists freevars _ conj resrow =
                case runResultStream (doFilter db (Query freevars conj) (return resrow)) (\row seed -> return (Right (row : seed))) [] of Identity results -> results
            notExists freevars _ conj resrow =
                case runResultStream (doFilter db (Query freevars conj) (return resrow)) (\_ _ -> return (Left [])) [resrow] of Identity results -> results
            expand1 _ var1 str2 resrow = [insert var1 str2 resrow ] 
            expand2 _ str1 var2 resrow = [insert var2 str1 resrow ] 
            expand12 _ _ _ _ = error "unconstrained eq predicate"
      
instance Functor (ResultStream m seed) where
    fmap f (ResultStream enumerator) = ResultStream (\iteratee seed ->
        enumerator (iteratee . f) seed)
        
-- instance Applicative m => Applicative (ResultStream m seed) where

instance Functor m => Monad (ResultStream m seed) where
    return a = ResultStream (\iteratee seed -> 
                                (\seedneweither -> case seedneweither of 
                                    Left seednew -> seednew
                                    Right seednew -> seednew) <$> iteratee a seed)
    (ResultStream enumerator) >>= f = ResultStream (\iteratee seed ->
        enumerator (\row seed2 -> Right <$> runResultStream (f row) iteratee seed2) seed)
 
instance MonadIO (ResultStream IO seed) where
    liftIO f = ResultStream (\iteratee seed -> f >>= \a-> runResultStream (return a) iteratee seed)

instance MonadIO (ResultStream (StateT s IO) seed) where
    liftIO f = ResultStream (\iteratee seed -> lift f >>= \a-> runResultStream (return a) iteratee seed)

-- UI

instance Show Pred where
    show (Pred name t) = name ++ show t
     
instance Show Atom where
    show (Atom (Pred name _) args) = name ++ show args
    show (Exists var conj) = "(exists " ++ var ++ "." ++ show conj ++ ")"
    
instance Show Expr where
    show (VarExpr var) = var
    show (IntExpr i) = show i
    show (StringExpr s) = show s
    
instance Show Lit where
    show (Lit thesign theatom) = case thesign of 
        Pos -> show theatom
        Neg -> "~" ++ show theatom
    
    