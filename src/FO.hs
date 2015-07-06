{-# LANGUAGE TypeFamilies, MultiParamTypeClasses, FunctionalDependencies, ExistentialQuantification, FlexibleInstances #-}
module FO where

import Data.Map.Strict (Map, (!), empty, member, insert, foldrWithKey, alter)
import Text.ParserCombinators.Parsec
import Control.Applicative ((<$>), (<*>), Applicative, pure)
import Data.Functor.Identity
import Data.List ((\\))
import Control.Monad (liftM2)

-- variable types
type Type = String

-- predicate types
type PredType = [Type]

-- predicate
data Pred = Pred { predName :: String, predType :: PredType} deriving Eq

-- variables
type Var = String

-- expression
data Expr = VarExpr Var | IntExpr Int | StringExpr String deriving Eq
 
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
class ResultStream m re where
    type ResultRow re
    enumerate :: re -> (ResultRow re -> seed -> m (Either seed seed)) -> seed -> m seed
    limitvars :: [Var] -> m re -> m re
    
-- database
class ResultStream m re => Database_ db m re | db -> m re where
    getName :: db -> String
    getPreds :: db -> [Pred] 
    -- domainSize function is a function from arguments to domain size
    -- it is used to compute the optimal query plan
    domainSize :: db -> (Var -> DomainSize) -> Lit -> DomainSize
    doQuery :: db -> Query -> m re
    -- filter takes a result stream, a list of input vars, a query, and generate a result stream
    doFilter :: db -> Query -> m re -> m re
    
-- https://wiki.haskell.org/Existential_type#Dynamic_dispatch_mechanism_of_OOP
data Database m row = forall db. Database_ db m row => Database { unDatabase :: db }

-- predicate map
type PredMap = Map String Pred

-- map from predicate name to database names
type PredDBMap = Map String [String]

-- database map
type DBMap m re = Map String (Database m re)

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
    freevars :: a -> [Var]
    determinedVars :: a -> [Var]
    
instance FreeVars Expr where
    freevars (VarExpr var) = [var]
    freevars _ = []
    determinedVars = freevars
    
instance FreeVars Atom where
    freevars (Atom _ theargs) = foldl (\fvs expr -> 
        fvs `appendNew` freevars expr) [] theargs
    freevars (Exists var conj) = freevars conj \\ [var]
        
    determinedVars a = case a of
        Atom _ _ -> freevars a
        Exists var conj -> determinedVars conj \\ [var]
        
instance FreeVars Lit where
    freevars (Lit _ theatom) = freevars theatom
    determinedVars lit = case lit of
        Lit Pos _ -> freevars lit
        Lit Neg _ -> [] -- negative literal doesn't determine any variables
    
instance FreeVars [Disjunction] where
    freevars = foldl (\fvs disj ->
        fvs `appendNew` (union . map freevars) disj) []
    determinedVars = foldl (\fvs disj ->
        fvs `appendNew` (intersect . map freevars) disj) []
            
intersect :: Eq a => [[a]] -> [a]
intersect [] = error "can't intersect zero lists"
intersect (hd : tl) = foldl (\ as bs -> [ x | x <- as, x `elem` bs ]) hd tl
             
union :: Eq a => [[a]] -> [a]
union = foldl appendNew []

type Report = [[Disjunction]]

-- exec query from dbname
execQuery :: ResultStream m re => [Database m re] -> Query -> (m re, Report)
execQuery dbs (Query vars lits) = (\(x,allvars,y,z,w) -> doTheFilters x allvars y z w) doTheQuery where
    doTheQuery = 
        let (dbsnew, effectivelits) = findEffectiveDisjsPrefix [] (dbs, []) lits
            dtvars = determinedVars effectivelits
            freevarsnew = freevars effectivelits in
            if null effectivelits 
                then error ("can't find effective literals, try reordering the literals: " ++ show lits)
                else case head dbsnew of 
                    Database db -> 
                        (lits \\ effectivelits, freevarsnew, dtvars, doQuery db (Query (vars `appendNew` freevarsnew) effectivelits), [effectivelits])
        
    doTheFilters [] _ _ results2 w = (limitvars vars results2, w)
    doTheFilters lits2 allvars dtvars results2 w = 
        doTheFilters restLitsnew allvarsnew dtvarsnew resultsnew (w ++ w2) where
            (restLitsnew, allvarsnew, dtvarsnew, resultsnew, w2) = doTheFilter lits2 allvars dtvars results2
    doTheFilter lits2 allvars dtvars results2 = 
        let (dbsnew, effectivelits) = findEffectiveDisjsPrefix dtvars (dbs, []) lits2
            dtvarsnew = dtvars `appendNew` determinedVars effectivelits
            freevarsnew = freevars effectivelits in
            if null effectivelits 
                then error ("can't find effective literals, try reordering the literals: " ++ show dtvars ++ show lits2)
                else case head dbsnew of 
                    Database db -> 
                        (lits2 \\ effectivelits, allvars `appendNew` freevarsnew, dtvarsnew, doFilter db (Query (allvars `appendNew` freevarsnew) effectivelits) results2, [effectivelits])


-- construct map from db name to db
constructDBMap :: [Database m rs] -> DBMap m rs
constructDBMap = foldr addDBToMap empty where
    addDBToMap (Database db) = insert (getName db) (Database db)
    
constructPredMap :: [Database m rs] -> PredMap
constructPredMap = foldr addPredFromDBToMap empty where
    addPredFromDBToMap (Database db) predmap = foldr addPredToMap predmap preds where
        addPredToMap thepred = insert (predName thepred) thepred
        preds = getPreds db
            
-- construct map from predicates to db names
constructPredDBMap :: [Database m rs] -> PredDBMap
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
findEffectiveDisjsPrefix :: [Var] -> ([Database m rs], [Disjunction]) -> [Disjunction] -> ([Database m rs], [Disjunction])
findEffectiveDisjsPrefix _ effective [] = effective
findEffectiveDisjsPrefix determinedvars effective (disj : disjs) =
    if null dbsnew 
        then effective 
        else findEffectiveDisjsPrefix (determinedvars `appendNew` intersect (fmap determinedVars disj)) (dbsnew, effectivedisjs ++ [disj]) disjs where
        (dbs, effectivedisjs) = effective
        dbsnew = filter isDisjEffective dbs
        isDisjEffective db = allTrue (isLitEffective db) disj
        isLitEffective (Database db) lit = case lit of
            (Lit _ (Atom _ _)) -> domainSize db (\ var -> 
                if var `elem` determinedvars 
                    then Just 1 -- here we don't have to calculate the actually domain size 
                    else Nothing) lit /= Nothing
            (Lit _ (Exists _ conj)) -> 
                let (_, prefix) = findEffectiveDisjsPrefix determinedvars (fst effective, []) conj in
                    length prefix == length conj -- only allows subqueries that are in one database
                
                
splitPosNegLits :: [Lit] -> ([Atom], [Atom])
splitPosNegLits = foldr (\ (Lit thesign theatom) (pos, neg) -> case thesign of 
    Pos -> (theatom : pos, neg)
    Neg -> (pos, theatom : neg)) ([],[])
    
    
-- example MapDB
-- result value
type ResultValue = String

-- result row
type MapResultRow = Map Var ResultValue

filterResults :: [Var] -> (Pred->String -> String -> MapResultRow -> [MapResultRow])
    -> (Pred->Var -> String -> MapResultRow -> [MapResultRow])
    -> (Pred->String -> Var -> MapResultRow -> [MapResultRow])
    -> (Pred->Var -> Var -> MapResultRow -> [MapResultRow])
    -> ([Var] -> Var -> [Disjunction] -> MapResultRow -> [MapResultRow])
    -> (Pred->String -> String -> MapResultRow -> [MapResultRow])
    -> ([Var] -> Var -> [Disjunction] -> MapResultRow -> [MapResultRow])
    -> [MapResultRow]
    -> Disjunction
    -> [MapResultRow]
filterResults freevars filterBy expand1 expand2 expand12 exists excludeBy notExists results = 
    concatMap (filterResultsLit freevars filterBy expand1 expand2 expand12 exists excludeBy notExists results)
    
filterResultsLit :: [Var] -> (Pred->String -> String -> MapResultRow -> [MapResultRow])
    -> (Pred->Var -> String -> MapResultRow -> [MapResultRow])
    -> (Pred->String -> Var -> MapResultRow -> [MapResultRow])
    -> (Pred->Var -> Var -> MapResultRow -> [MapResultRow])
    -> ([Var]-> Var -> [Disjunction] -> MapResultRow -> [MapResultRow])
    -> (Pred->String -> String -> MapResultRow -> [MapResultRow])
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
                then filterBy thepred (resrow ! var1) str2 resrow
                else expand1 thepred var1 str2 resrow
        StringExpr str1 : VarExpr var2 : _ ->
            if var2 `member` resrow
                then filterBy thepred str1 (resrow ! var2) resrow
                else expand2 thepred str1 var2 resrow
        StringExpr str1 : StringExpr str2 : _ ->
            filterBy thepred str1 str2 resrow
        _ -> []) results

filterResultsLit _ _ _ _ _ _ excludeBy _ results (Lit Neg (Atom thepred args)) =
    concatMap (\resrow -> case args of
        VarExpr var1 : VarExpr var2 : _ ->
            if var1 `member` resrow && var2 `member` resrow
                then excludeBy thepred (resrow ! var1) (resrow ! var2) resrow
                else error ("unconstrained variable " ++ var1 ++ " or " ++ var2)
        VarExpr var1 : StringExpr str2 : _ ->
            if var1 `member` resrow
                then excludeBy thepred (resrow ! var1) str2 resrow
                else error ("unconstrained variable " ++ var1 ++ " in " ++ show (Lit Neg (Atom thepred args)))
        StringExpr str1 : VarExpr var2 : _ ->
            if var2 `member` resrow
                then excludeBy thepred str1 (resrow ! var2) resrow
                else error ("unconstrained variable " ++ var2 ++ " in " ++ show (Lit Neg (Atom thepred args)))
        StringExpr str1 : StringExpr str2 : _ ->
            excludeBy thepred str1 str2 resrow
        _ -> [resrow]) results

filterResultsLit freevars2 _ _ _ _ exists _ _ results (Lit Pos (Exists var conj)) = 
    concatMap (exists (freevars2 `appendNew` (freevars conj \\ [var])) var conj) results
    
filterResultsLit freevars2 _ _ _ _ _ _ notExists results (Lit Neg (Exists var conj)) =
    concatMap (notExists freevars2 var conj) results

limitvarsInRow :: [Var] -> MapResultRow -> MapResultRow
limitvarsInRow vars = foldrWithKey (\ var val newresrow -> if var `elem` vars then insert var val newresrow else newresrow) empty

data MapDB = MapDB String String [(String, String)]

newtype ListResultStream = ListResultStream [MapResultRow]

instance ResultStream Identity ListResultStream where
    type ResultRow ListResultStream = MapResultRow
    enumerate (ListResultStream results) iteratee seed = foldEitherM f seed results where
        f x y = iteratee y x 
        foldEitherM _ a [] = return a
        foldEitherM g a (b : bs) = do
            c <- g a b
            case c of
                Left d -> return d
                Right d -> foldEitherM g d bs
    limitvars vars (Identity (ListResultStream results)) = Identity (ListResultStream (map (limitvarsInRow vars) results)) 
        

instance Database_ MapDB Identity ListResultStream where
    getName (MapDB name _ _) = name
    getPreds (MapDB _ predname _) = [ Pred predname ["String", "String"] ]
    domainSize db varDomainSize (Lit thesign (Atom thepred [arg1, arg2])) 
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
    domainSize _ _ _ = Nothing
        
    doQuery db query = doFilter db query (pure (ListResultStream [empty]))
    doFilter db (Query vars disjs) = fmap (\(ListResultStream results)-> ListResultStream (filteredResults results)) where
        (MapDB _ _ rows) = db
        filteredResults results = [ limitvarsInRow vars resrow | 
            resrow <- foldl (filterResults vars filterBy expand1 expand2 expand12 exists excludeBy notExists) results disjs ]
        filterBy _ str1 str2 resrow = [resrow | (str1, str2) `elem` rows]
        excludeBy _ str1 str2 resrow = [resrow | (str1, str2) `notElem` rows ] 
        exists freevars var conj resrow = case doFilter db (Query freevars conj) (Identity (ListResultStream [resrow])) of Identity (ListResultStream rowsnew) -> rowsnew
        notExists freevars var conj resrow = [ resrow | null (case doFilter db (Query freevars conj) (Identity (ListResultStream [resrow])) of Identity (ListResultStream rowsnew) -> rowsnew) ]
        expand1 _ var1 str2 resrow = [insert var1 (fst row) resrow | row <- rows, snd row == str2 ] 
        expand2 _ str1 var2 resrow = [insert var2 (snd row) resrow | row <- rows, fst row == str1 ] 
        expand12 _ var1 var2 resrow = [ insert var1 (fst row) (insert var2 (snd row) resrow) | row <- rows ]
                
-- example EqDB
 
data EqDB = EqDB String
instance Database_ EqDB Identity ListResultStream where
    getName (EqDB name) = name
    getPreds _ = [ Pred "eq" ["String", "String"] ]
    domainSize db varDomainSize (Lit thesign (Atom thepred [arg1, arg2])) 
        | thepred `elem` getPreds db = 
            let d1 = exprDomainSize varDomainSize Nothing arg1
                d2 = exprDomainSize varDomainSize Nothing arg2 in
                case thesign of
                    Pos -> dmin d1 d2
                    Neg -> dmul d1 d2
    domainSize _ _ _ = Nothing
        
    doQuery db query = doFilter db query (return (ListResultStream [empty]))
    doFilter db (Query vars disjs) = fmap (\ (ListResultStream results) -> ListResultStream ( filteredResults results) ) where
        filteredResults res = [ limitvarsInRow vars resrow | resrow <- foldl (filterResults vars filterBy expand1 expand2 expand12 exists excludeBy notExists) res disjs ]
        filterBy _ str1 str2 resrow = [resrow | str1 == str2]
        excludeBy _ str1 str2 resrow = [resrow | str1 /= str2 ] 
        exists freevars var conj resrow = case doFilter db (Query freevars conj) (Identity (ListResultStream [resrow])) of Identity (ListResultStream rowsnew) -> rowsnew
        notExists freevars var conj resrow = [ resrow | null (case doFilter db (Query freevars conj) (Identity (ListResultStream [resrow])) of Identity (ListResultStream rowsnew) -> rowsnew) ]
        expand1 _ var1 str2 resrow = [insert var1 str2 resrow ] 
        expand2 _ str1 var2 resrow = [insert var2 str1 resrow ] 
        expand12 _ _ _ _ = error "unconstrained eq predicate"
      


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
    
type FOParser = GenParser Char (Map String Pred)
-- parser
identifier :: FOParser String
identifier  = do
    c  <- letter
    cs <- many (alphaNum <|> char '_')
    return (c:cs)
    
oper :: Char -> FOParser ()
oper ch = do
    spaces
    char ch        
    spaces
    return ()
            
argp :: FOParser Expr
argp = 
    (VarExpr <$> identifier)
    <|> (IntExpr . read <$> many1 digit)
    <|> (char '\"' >> (StringExpr <$> many (noneOf "\"")) >>= \expr -> char '\"' >> return expr)

arglistp :: FOParser [Expr]
arglistp =
    oper '(' >> (
        (oper ')' >> return [])
        <|> ((:) <$> argp <*> arglisttail)
    ) where
        arglisttail :: FOParser [Expr]
        arglisttail = (spaces >> oper ')' >> return [])
            <|> (oper ',' >> spaces >> ((:) <$> argp <*> arglisttail))

atomp :: FOParser Atom
atomp = (do
    oper '('
    string "exists"
    spaces
    var <- identifier
    oper '.'
    conj <- disjsp
    oper ')'
    return (Exists var conj)) <|> (do
    predmap <- getState
    predname <- identifier
    if not (predname `member` predmap) 
        then error ("undefined predicate " ++ predname) 
        else do
            let thepred = predmap ! predname
            arglist <- arglistp
            return (Atom thepred arglist))
    
litp :: FOParser Lit
litp = (oper '~' >> Lit <$> return Neg <*> atomp)
    <|> Lit <$> return Pos <*> atomp
    
disjp :: FOParser Disjunction
disjp = spaces >> ((:) <$> litp <*> disjptail) where
    disjptail = (spaces >> char '|' >> disjp) <|> return []
        
paramtypep :: FOParser Type
paramtypep = identifier

predtypep :: FOParser [Type]
predtypep =
    oper '(' >>
    ((oper ')' >> return [])
    <|> (:) <$> paramtypep <*> predtypetail) where
        predtypetail = (oper ')' >> return [])
            <|> (oper ',' >> spaces >> (:) <$> paramtypep <*> predtypetail)
    
predp :: FOParser ()
predp = do
    string "predicate"
    spaces
    name <- identifier
    spaces
    t <- predtypep
    let thepred = Pred name t
    updateState (insert name thepred)
    
    
disjsp :: FOParser [Disjunction]
disjsp = spaces >> ((predp >> disjsp)
    <|> (:) <$> disjp <*> disjsp
    <|> return [])
    
progp :: FOParser ([Disjunction], Map String Pred)
progp = do
    disjs <- disjsp
    predmap <- getState
    return (disjs, predmap)

       
    