{-# LANGUAGE TypeSynonymInstances, FlexibleInstances, MultiParamTypeClasses, FlexibleContexts, RankNTypes, GADTs, PatternSynonyms #-}
module SQL.SQL where

import QueryPlan
import FO.Data hiding (Subst, subst)
import FO.Domain
import DBQuery
import ListUtils
import Utils

import Prelude hiding (lookup)
import Data.List (intercalate, (\\),union, nub)
import Control.Monad.Trans.State.Strict (StateT, get, put, evalStateT, runStateT, modify)
import Control.Monad.Trans.Class (lift)
import Data.Map.Strict (empty, Map, insert, member, singleton, lookup, fromList, keys, alter, toList, elems, size, delete)
import Data.Monoid
import Data.Maybe
import Control.Monad.Trans.Except
import Debug.Trace
import Data.Convertible
import qualified Data.Text as T
import Data.Set (toAscList)
import qualified Data.Set as Set
import Algebra.Lattice
import Algebra.Lattice.Dropped
import Algebra.Lattice.Ordered
import Algebra.SemiBoundedLattice

type Col = String
type TableName = String
data Table = OneTable TableName SQLVar deriving (Eq, Ord)

type SQLTableList = [Table]

-- merge two lists of sql tables
-- only support onetable
mergeTables :: SQLTableList -> SQLTableList -> SQLTableList
mergeTables = union

newtype SQLVar = SQLVar {unSQLVar :: String} deriving (Eq, Ord)

type SQLQualifiedCol = (SQLVar, Col)

type SQLOper = String

data SQLExpr = SQLColExpr SQLQualifiedCol
             | SQLIntConstExpr Int
             | SQLStringConstExpr T.Text
             | SQLPatternExpr T.Text
             | SQLNullExpr
             | SQLParamExpr String
             | SQLExprText String
             | SQLFuncExpr String [SQLExpr] deriving (Eq, Ord)

isSQLConstExpr :: SQLExpr -> Bool
isSQLConstExpr (SQLIntConstExpr _ ) = True
isSQLConstExpr (SQLStringConstExpr _ ) = True
isSQLConstExpr (SQLParamExpr _) = True
isSQLConstExpr _ = False

data SQLCond = SQLCompCond SQLOper SQLExpr SQLExpr
             | SQLAndCond SQLCond SQLCond
             | SQLOrCond SQLCond SQLCond
             | SQLExistsCond SQL
             | SQLNotCond SQLCond
             | SQLTrueCond
             | SQLFalseCond
             deriving (Eq, Ord)

getSQLConjuncts :: SQLCond -> [SQLCond]
getSQLConjuncts (SQLAndCond conj1 conj2) = getSQLConjuncts conj1 ++ getSQLConjuncts conj2
getSQLConjuncts cond = [cond]

getSQLDisjuncts :: SQLCond -> [SQLCond]
getSQLDisjuncts (SQLOrCond conj1 conj2) = getSQLConjuncts conj1 ++ getSQLConjuncts conj2
getSQLDisjuncts cond = [cond]

(.&&.) :: SQLCond -> SQLCond -> SQLCond
a .&&. b = SQLAndCond a b

(.||.) :: SQLCond -> SQLCond -> SQLCond
a .||. b = SQLOrCond a b


(.=.) :: SQLExpr -> SQLExpr -> SQLCond
a .=. b = SQLCompCond "=" a b

(.<>.) :: SQLExpr -> SQLExpr -> SQLCond
a .<>. b = SQLCompCond "<>" a b

data SQLOrder = ASC | DESC deriving (Eq, Ord)

type IntLattice = Dropped (Ordered Int)
pattern IntLattice a = Drop (Ordered a)

data SQL = SQLQuery {sqlSelect :: [ (Var, SQLExpr) ], sqlFrom :: SQLTableList, sqlWhere :: SQLCond, sqlOrderBy :: [(SQLExpr, SQLOrder)], sqlLimit :: IntLattice } deriving (Eq, Ord)

data SQLStmt = SQLQueryStmt SQL
    | SQLInsertStmt TableName [(Col,SQLExpr)] [Table] SQLCond
    | SQLUpdateStmt (TableName, SQLVar) [(Col,SQLExpr)] SQLCond
    | SQLDeleteStmt (TableName, SQLVar) SQLCond deriving (Eq, Ord)

instance Show Table where
    show (OneTable tablename var) = tablename ++ " " ++ show var

instance Show SQLCond where
    show a = show2 a []
instance Show SQLExpr where
    show a = show2 a []

class Show2 a where
    show2 :: a -> [SQLVar] -> String

instance Show2 SQLCond where
    show2 (SQLCompCond op lhs rhs) sqlvar = show2 lhs sqlvar ++ " " ++ op ++ " " ++ show2 rhs sqlvar
    show2 (SQLAndCond a b) sqlvar = "(" ++ show2 a sqlvar ++ " AND " ++ show2 b sqlvar ++ ")"
    show2 (SQLOrCond a b) sqlvar = "(" ++ show2 a sqlvar ++ " OR " ++ show2 b sqlvar ++ ")"
    show2 (SQLTrueCond) _ = "True"
    show2 (SQLFalseCond) _ = "False"
    show2 (SQLExistsCond sql) sqlvar = "(EXISTS (" ++ show2 sql sqlvar ++ "))"
    show2 (SQLNotCond sql) sqlvar = "(NOT (" ++ show2 sql sqlvar ++ "))"
instance Show2 SQLExpr where
    show2 (SQLColExpr (var, col)) sqlvar = if var `elem` sqlvar
        then col
        else show var ++ "." ++ col
    show2 (SQLIntConstExpr i) _ = show i
    show2 (SQLStringConstExpr s) _ = "'" ++ sqlStringEscape (T.unpack s) ++ "'"
    show2 (SQLPatternExpr s) _ = "'" ++ sqlPatternEscape (T.unpack s) ++ "'"
    show2 (SQLParamExpr _) _ = "?"
    show2 (SQLFuncExpr fn args) sqlvar = fn ++ "(" ++ intercalate "," (map (\a -> show2 a sqlvar) args) ++ ")"
    show2 (SQLExprText s) _ = s
    show2 SQLNullExpr _ = "NULL"

instance Show SQLVar where
    show (SQLVar var) = var

showWhereCond2 :: SQLCond -> [SQLVar] -> String
showWhereCond2 cond sqlvar = case cond of
    SQLTrueCond -> ""
    _ -> " WHERE " ++ show2 cond sqlvar

instance Show2 SQL where
    show2 (SQLQuery cols tables conds orderby limit) sqlvar = "SELECT " ++ (if null cols then "1" else intercalate "," (map (\(var, expr) -> show2 expr sqlvar ++ " AS " ++ show var) cols)) ++
            (if null tables
                then ""
                else " FROM " ++ intercalate "," (map show tables)) ++
            (showWhereCond2 conds sqlvar) ++
            (if null orderby
                then ""
                else " ORDER BY " ++ intercalate "," (map (\(expr, ord) -> show expr ++ " " ++ case ord of
                                                                                                    ASC -> "ASC"
                                                                                                    DESC -> "DESC") orderby)) ++
            (case limit of
                Top -> ""
                IntLattice n -> " LIMIT " ++ show n)


instance Show SQLStmt where
    show (SQLQueryStmt sql) = show2 sql []

    show (SQLInsertStmt tname colsexprs tables cond) =
        let (cols, exprs) = unzip colsexprs in
            "INSERT INTO " ++ tname ++ " (" ++ intercalate "," cols ++ ")" ++
                if null tables && all isSQLConstExpr exprs
                    then " VALUES (" ++ intercalate "," (map (\a -> show2 a []) exprs)++ ")"
                    else " SELECT " ++ intercalate "," (map (\a -> show2 a []) exprs) ++ (if null tables
                        then ""
                        else " FROM " ++ intercalate "," (map show tables)) ++ showWhereCond2 cond []
    show (SQLDeleteStmt (tname, sqlvar) cond)  =
        "DELETE FROM " ++ tname ++ showWhereCond2  cond [sqlvar]

    show (SQLUpdateStmt (tname, sqlvar) colsexprs cond)  =
        "UPDATE " ++ tname ++ " SET " ++ intercalate "," (map (\(col, expr)-> col ++ " = " ++ show2  expr [sqlvar]) colsexprs) ++ showWhereCond2  cond [sqlvar]

sqlStringEscape :: String -> String
sqlStringEscape = concatMap f where
    f '\'' = "''"
    f a = [a]

sqlPatternEscape :: String -> String
sqlPatternEscape = concatMap f where
    f '\\' = "\\\\"
    f a = [a]

class Subst a where
    subst :: Map SQLVar SQLVar -> a -> a

class SQLFreeVars a where
    fv :: a -> [(TableName, SQLVar)]

instance Subst SQLVar where
    subst varmap var = case lookup  var varmap of
        Nothing -> var
        Just var2 -> var2


instance Subst Table where
    subst varmap (OneTable tablename var) = OneTable tablename (subst varmap var)

instance SQLFreeVars Table where
    fv (OneTable tablename var) = [(tablename, var)]

instance Subst SQLCond where
    subst varmap (SQLCompCond op a b) = SQLCompCond op (subst varmap a) (subst varmap b)
    subst varmap (SQLAndCond a b) = SQLAndCond (subst varmap a) (subst varmap b)
    subst varmap (SQLOrCond a b) = SQLOrCond (subst varmap a) (subst varmap b)
    subst _ (SQLTrueCond ) = SQLTrueCond
    subst _ (SQLFalseCond ) = SQLFalseCond
    subst varmap (SQLNotCond a) = SQLNotCond (subst varmap a)
    -- subst varmap (SQLExistsCond sql) = SQLExistsCond (subst varmap sql)
    subst _ _ = error "unsupported SQLCond"

instance Subst SQLExpr where
    subst varmap (SQLColExpr qcol) = SQLColExpr (subst varmap qcol)
    subst _ a = a

instance Subst SQLQualifiedCol where
    subst varmap (var, col) = (subst varmap var, col)

instance Subst a => Subst [a] where
    subst varmap = map (subst varmap)

type SQLQuery = ([Var], SQLStmt, [Var]) -- return vars, sql, param vars

instance Monoid SQL where
    (SQLQuery sselect1 sfrom1 swhere1 orderby1 limit1) `mappend` (SQLQuery sselect2 sfrom2 swhere2 orderby2 limit2) =
        SQLQuery (sselect1 ++ sselect2) (sfrom1 `mergeTables` sfrom2) (swhere1 .&&. swhere2) (orderby1 <> orderby2) (limit1 /\ limit2)
    mempty = SQLQuery [] [] SQLTrueCond [] top

sor :: SQL -> SQL -> SQL
sor (SQLQuery sselect1 sfrom1 swhere1 [] Top) (SQLQuery sselect2 sfrom2 swhere2 [] Top) = SQLQuery (sselect1 ++ sselect2) (sfrom1 `mergeTables` sfrom2) (swhere1 .||. swhere2) [] top
sor _ _ = error "sor: incompatible order by and limit"

snot :: SQL -> SQL
snot (SQLQuery sselect sfrom swhere orderby limit) = SQLQuery sselect sfrom (SQLNotCond swhere) [] top

swhere :: SQLCond -> SQL
swhere swhere1 = SQLQuery [] [] swhere1 [] top
-- translate relational calculus to sql
-- If P maps to table T col_1 ... col_n
-- {xs : P(e_1,e_2,...,e_n)}
-- translates to SELECT cols FROM T P WHERE ...
-- if x_i = e_j then "col_j", if there are multiple j's, choose any one
-- if e_i is a const, then "P.col_i = e_i"
-- if e_i is a variable, and e_j = e_i then "P.col_j = P.col_i"
-- otherwise True

-- rep map maps a FO variable to a qualified column in sql so that all implicit equality constraints are
-- compared with this column
-- table map maps a table name and a list of primary key expressions to a sql var so that all predicates
-- that share this list of primary key expressions uses the same sql var
type RepMap = Map Var SQLExpr
type TableMap = Map (TableName, [Expr]) SQLVar
-- predicate -> table
type PredTableMap = Map Pred (Table, [SQLQualifiedCol])
-- table -> cols, primary key
type Schema = Map TableName ([Col], [Col])
-- builtin predicate -> op, neg op
newtype BuiltIn = BuiltIn (Map Pred (Sign -> [Expr] -> TransMonad SQL))

simpleBuildIn :: String -> (Sign -> [SQLExpr] -> TransMonad SQL) -> Sign -> [Expr] -> TransMonad SQL
simpleBuildIn n builtin sign args = do
    let err m = do
                a <- m
                case a of
                    Left expr -> return expr
                    Right _ -> error ("unconstrained argument to built-in predicate " ++ n)
    sqlExprs <- mapM (err . sqlExprFromArg) args
    builtin sign sqlExprs


data TransState = TransState {
    builtin :: BuiltIn,
    predtablemap :: PredTableMap,
    repmap :: RepMap,
    tablemap :: TableMap -- this is a list of free vars that appear in atoms to be deleted they must be linear
}
type TransMonad a = StateT TransState NewEnv a

freshSQLVar :: TableName -> TransMonad SQLVar
freshSQLVar tablename = lift $ SQLVar <$> new (StringWrapper tablename)

sqlExprFromArg :: Expr -> TransMonad (Either SQLExpr Var)
sqlExprFromArg arg = do
    ts <- get
    case arg of
        VarExpr var2 ->
            return (case lookup  var2 (repmap ts) of
                    Just e -> Left e
                    Nothing -> Right var2)
        IntExpr i ->
            return (Left (SQLIntConstExpr i))
        StringExpr s ->
            return (Left (SQLStringConstExpr s))
        PatternExpr s ->
            return (Left (SQLPatternExpr s))

addVarRep :: Var -> SQLExpr -> TransMonad ()
addVarRep var expr =
    modify (\ts-> ts {repmap =  insert var expr (repmap ts)})

condFromArg :: (Expr, SQLQualifiedCol) -> TransMonad SQLCond
condFromArg (arg, col) = do
    v <- sqlExprFromArg arg
    case v of
        Left expr -> return ((SQLColExpr col) .=. expr)
        Right var2 -> do
            addVarRep var2 (SQLColExpr col)
            return SQLTrueCond


-- add a sql representing the row identified by the keys
addTable :: TableName -> [Expr] -> SQLVar -> TransMonad ()
addTable tablename prikeyargs sqlvar2 =
    modify (\ts  -> ts {tablemap = insert (tablename, prikeyargs) sqlvar2 (tablemap ts)})

class Params a where
    params :: a -> [Var]

instance Params a => Params [a] where
    params = foldMap params

instance Params SQLExpr where
    params (SQLParamExpr p) = [Var p]
    params (SQLFuncExpr _ es) = foldMap params es
    params _ = []

instance Params SQLCond where
    params (SQLCompCond _ e1 e2) = params e1 ++ params e2
    params (SQLOrCond c1 c2 ) = params c1 ++ params c2
    params (SQLAndCond c1 c2) = params c1 ++ params c2
    params (SQLNotCond c) = params c
    params (SQLFalseCond) = []
    params (SQLTrueCond) = []
    params (SQLExistsCond sql) = params sql

instance Params SQLStmt where
    params (SQLQueryStmt (SQLQuery sel _ cond _ _)) = params sel ++ params cond
    params (SQLInsertStmt _ vs _ cond) = params (map snd vs) ++ params cond
    params (SQLUpdateStmt _ vs cond) = params (map snd vs) ++ params cond
    params (SQLDeleteStmt _ cond) = params cond

instance Params SQL where
    params (SQLQuery sel _ cond _ _) = params sel ++ params cond

instance Params (Var, SQLExpr) where
    params (_, expr) = params expr

instance Params SQLQuery where
    params (_, _, params) = params

translateQueryToSQL :: [Var] -> Query -> TransMonad SQLQuery
translateQueryToSQL vars qu@(Query formula) = do
    (vars, sql, vars2) <- if pureF formula
        then do
            (SQLQuery _ tablelist cond1 orderby limit) <-  translateFormulaToSQL formula
            ts <- get
            let extractCol var = case lookup var (repmap ts) of
                                    Just col -> col
                                    _ -> error ("translateQueryToSQL: " ++ show var ++ " doesn't correspond to a column while translating query " ++  show qu ++ " to SQL, available " ++ show (repmap ts))
            let cols = map extractCol vars
                sql = SQLQueryStmt (SQLQuery (zip vars cols) tablelist cond1 orderby limit)
            return (vars, sql, (params sql))
        else translateInsertToSQL formula
    return (vars, simplifySQLCond sql, vars2)

simplifySQLCond :: SQLStmt -> SQLStmt
simplifySQLCond (SQLQueryStmt (SQLQuery s f cond orderby limit)) = SQLQueryStmt (SQLQuery s f (simplifySQLCond' cond) orderby limit)
simplifySQLCond (SQLInsertStmt t s f cond) = SQLInsertStmt t s f (simplifySQLCond' cond)
simplifySQLCond (SQLUpdateStmt t cs cond) = SQLUpdateStmt t cs (simplifySQLCond' cond)
simplifySQLCond (SQLDeleteStmt t cond) = SQLDeleteStmt t (simplifySQLCond' cond)

simplifySQLCond2 :: SQL -> SQL
simplifySQLCond2 (SQLQuery s f cond orderby limit) = SQLQuery s f (simplifySQLCond' cond) orderby limit

simplifySQLCond' :: SQLCond -> SQLCond
simplifySQLCond' c@(SQLCompCond "=" a b) | a == b = SQLTrueCond
simplifySQLCond' c@(SQLCompCond _ _ _) = c
simplifySQLCond' (SQLTrueCond) = SQLTrueCond
simplifySQLCond' (SQLFalseCond) = SQLFalseCond
simplifySQLCond' c@(SQLAndCond _ _) =
    let conj = getSQLConjuncts c
        conj2 = concatMap (getSQLConjuncts . simplifySQLCond') conj
        conj3 = filter (\a -> case a of
                                SQLTrueCond -> False
                                _ -> True) conj2
        conj4 = if all (\a -> case a of
                                SQLFalseCond -> False
                                _ -> True) conj3
                    then conj3
                    else [SQLFalseCond]
        conj5 = nub conj4 in
        if null conj5
            then SQLTrueCond
            else foldl1 (.&&.) conj5
simplifySQLCond' (SQLOrCond a b) = case simplifySQLCond' a of
    SQLFalseCond -> simplifySQLCond' b
    SQLTrueCond -> SQLTrueCond
    a' -> case simplifySQLCond' b of
        SQLFalseCond -> a'
        SQLTrueCond -> SQLTrueCond
        b' -> SQLOrCond a' b'
simplifySQLCond' (SQLNotCond a) = case simplifySQLCond' a of
    SQLTrueCond -> SQLFalseCond
    SQLFalseCond -> SQLTrueCond
    a' -> SQLNotCond a'
simplifySQLCond' (SQLExistsCond sql) = SQLExistsCond (simplifySQLCond2 sql)


sqlexists :: SQL -> SQL
sqlexists sql@(SQLQuery [] tablelist (SQLExistsCond _) _ _) = sql
sqlexists (SQLQuery cols tablelist cond orderby limit) = SQLQuery [] [] (SQLExistsCond (SQLQuery cols tablelist cond orderby limit)) [] top

sqlfalse :: SQL
sqlfalse = SQLQuery [] [] (SQLFalseCond) [] top

sqlsummarize :: [(Var, SQLExpr)] -> SQL -> SQL
sqlsummarize funcs (SQLQuery _ from whe orderby limit) =
    SQLQuery funcs from whe orderby limit

sqlorderby :: SQLOrder -> SQLExpr -> SQL -> SQL
sqlorderby ord expr (SQLQuery sel from whe orderby limit) =
    SQLQuery sel from whe ((expr, ord) : orderby) limit

sqllimit :: IntLattice -> SQL -> SQL
sqllimit n (SQLQuery sel from whe orderby limit) =
    SQLQuery sel from whe orderby (n /\ limit)

findRep :: Var -> TransMonad SQLExpr
findRep v =  do
  ts <- get
  case lookup v (repmap ts) of
        Nothing -> error ("cannot find representative for variable " ++ show v)
        Just expr -> return expr

translateFormulaToSQL :: Formula -> TransMonad SQL
translateFormulaToSQL (FAtomic a) = translateAtomToSQL Pos a
translateFormulaToSQL (FSequencing form1 form2) =
    mappend <$> translateFormulaToSQL form1 <*> translateFormulaToSQL form2
translateFormulaToSQL (FOne) =
    return mempty

translateFormulaToSQL (FChoice form1 form2) =
    sor <$> translateFormulaToSQL form1 <*> translateFormulaToSQL form2
translateFormulaToSQL (FPar form1 form2) =
    sor <$> translateFormulaToSQL form1 <*> translateFormulaToSQL form2
translateFormulaToSQL (FZero) =
    return sqlfalse

translateFormulaToSQL (Aggregate Exists conj) = do
    sql <- translateFormulaToSQL conj
    return (sqlexists sql)

translateFormulaToSQL (Aggregate Not (FAtomic a)) =
    translateAtomToSQL Neg a

translateFormulaToSQL (Aggregate Not form) =
    snot <$> translateFormulaToSQL form

translateFormulaToSQL (Aggregate (Summarize funcs) conj) = do
    sql <- translateFormulaToSQL conj
    funcs' <- mapM (\(v, s) ->
        case s of
            Max v2 -> do
                rep <- findRep v2
                return (v, SQLFuncExpr "max" [rep])
            Min v2 -> do
                rep <- findRep v2
                return (v, SQLFuncExpr "min" [rep])
            Count -> return (v, SQLExprText "count(*)")) funcs
    return (sqlsummarize funcs' sql)

translateFormulaToSQL (Aggregate (Limit n) form) =
    sqllimit (IntLattice n) <$> translateFormulaToSQL form

translateFormulaToSQL (Aggregate (OrderByAsc v) form) = do
    sql <- translateFormulaToSQL form
    rep <- findRep v
    return (sqlorderby ASC rep sql)


translateFormulaToSQL (Aggregate (OrderByDesc v) form) = do
    sql <- translateFormulaToSQL form
    rep <- findRep v
    return (sqlorderby DESC rep sql)
translateFormulaToSQL form = error "unsupported"


lookupTableVar :: String -> [Expr] -> TransMonad (Bool, SQLVar)
lookupTableVar tablename prikeyargs = do
    ts <- get
    case lookup (tablename, prikeyargs) (tablemap ts) of -- check if there already is a table with same primary key
        Just v ->
            return (False, v)
        Nothing -> do
            sqlvar2 <- freshSQLVar tablename
            addTable tablename prikeyargs sqlvar2
            return (True, sqlvar2)

translateAtomToSQL :: Sign -> Atom -> TransMonad SQL
translateAtomToSQL thesign (Atom name args) = do
    ts <- get
    let (BuiltIn builtints) = builtin ts
    --try builtin first
    case lookup name builtints of
        Just builtinpred ->
            builtinpred thesign args
        Nothing -> case lookup name (predtablemap ts) of
            Just (table, cols) -> do
                (tables, varmap, cols2, args2) <- case table of
                    OneTable tablename sqlvar -> do
                        let prikeyargs = keyComponents name args
                        let prikeyargcols = keyComponents name cols
                         -- if primary key columns correspond to args
                        (new, v) <- lookupTableVar tablename prikeyargs
                        if new
                            then
                                return ([table], singleton sqlvar v, cols, args)
                            else do
                                let cols2 = cols \\ prikeyargcols
                                let args2 = args \\ prikeyargs
                                return ([], singleton sqlvar v, cols2 , args2)

                let tables2 = map (subst varmap) tables
                let cols3 = map (subst varmap) cols2
                condsFromArgs <- mapM condFromArg (zip args2 cols3)
                let cond3 = foldl (.&&.) SQLTrueCond condsFromArgs
                return (case thesign of
                            Pos -> SQLQuery [] tables2 cond3 [] top
                            Neg -> SQLQuery [] [] (SQLNotCond (SQLExistsCond (SQLQuery [] tables2 cond3 [] top))) [] top)
            Nothing -> error (show name ++ " is not defined")



-- formula must be pure
translateInsertToSQL :: Formula -> TransMonad SQLQuery
translateInsertToSQL form = do
    let conjs = getFsequencings form
    let f p [] = (p,[])
        f p forms0@(form : forms)
            | pureF form = f  (p ++ [form]) forms
            | otherwise = (p, forms0)
    let (p, e) = f [] conjs
    let lits = map (\(FInsert lit) -> lit) e
    let form' = fsequencing p
    translateInsertToSQL' lits form'

translateInsertToSQL' :: [Lit] -> Formula -> TransMonad SQLQuery
translateInsertToSQL' lits conj = do
    (SQLQuery _ tablelist cond orderby limit) <- translateFormulaToSQL conj
    let keymap = sortByKey lits
    if size keymap > 1
        then error ("translateInsertToSQL: more than one key " ++ show keymap)
        else do
            insertparts <- sortParts <$> (concat <$> mapM combineLitsSQL (elems keymap))
            case insertparts of
                [insertpart] -> do
                    ts <- get
                    let sql = toInsert tablelist cond insertpart
                    return ([], sql, params sql)
                _ -> error "translateInsertToSQL: more than one actions"

-- each SQLStmt must be an Insert statement
sortParts :: [SQLStmt] -> [SQLStmt]
sortParts [] = []
sortParts (p : ps) = b++[a] where
    (a, b) = foldl (\(active, done) part ->
        case (active, part) of
            (SQLInsertStmt tname colexprs tablelist cond, SQLInsertStmt tname2 colexprs2 tablelist2 cond2)
                | tname == tname2 && compatible colexprs colexprs2 ->
                    (SQLInsertStmt tname ( colexprs `union` colexprs2) (tablelist ++ tablelist2) (cond .&&. cond2), done)
            _ -> (part, active : done)) (p,[]) ps where
            compatible colexpr = all (\(col, expr) -> all (\(col2, expr2) ->col2 /= col || expr2 == expr) colexpr)

toInsert :: [Table] -> SQLCond -> SQLStmt -> SQLStmt
toInsert tablelist cond (SQLInsertStmt tname colexprs tablelist2 cond2) = SQLInsertStmt tname colexprs (tablelist ++ tablelist2) (cond .&&. cond2)
toInsert tablelist cond (SQLDeleteStmt tname cond2) = SQLDeleteStmt tname (cond .&&. cond2)
toInsert tablelist cond (SQLUpdateStmt tname colexprs cond2) = SQLUpdateStmt tname colexprs (cond .&&. cond2)


combineLitsSQL :: [Lit] -> TransMonad [SQLStmt]
combineLitsSQL lits = combineLits lits generateUpdateSQL generateInsertSQL generateDeleteSQL

preproc0 tname sqlvar qcols pred1 args = do
        let key = keyComponents pred1 args
        (_, sqlvar2) <- lookupTableVar tname key
        let varmap = singleton sqlvar sqlvar2
        let qcol_args = zip (subst varmap qcols) args
        return (qcol_args, sqlvar2)

preproc tname sqlvar qcols pred1 args = do
        (qcol_args , sqlvar2) <- preproc0 tname sqlvar qcols pred1 args
        let keyqcol_args = keyComponents pred1 qcol_args
        let propqcol_args = propComponents pred1 qcol_args
        return (keyqcol_args, propqcol_args, sqlvar2)

generateDeleteSQL :: Atom -> TransMonad [SQLStmt]
generateDeleteSQL atom = do
    sql <- translateDeleteAtomToSQL atom
    return [sql]

generateInsertSQL :: [Atom] -> TransMonad [SQLStmt]
generateInsertSQL atoms = do
    let map1 = sortAtomByPred atoms
    mapM generateInsertSQLForPred (toList map1)

generateUpdateSQL :: [Atom] -> [Atom] -> TransMonad [SQLStmt]
generateUpdateSQL pospropatoms negpropatoms = do
    let posprednamemap = sortAtomByPred pospropatoms
    let negprednamemap = sortAtomByPred negpropatoms
    let allkeys = keys posprednamemap `union` keys negprednamemap
    let poslist = [l | key <- allkeys, let l = case lookup key posprednamemap of Nothing -> []; Just l' -> l']
    let neglist = [l | key <- allkeys, let l = case lookup key negprednamemap of Nothing -> []; Just l' -> l']
    mapM generateUpdateSQLForPred (zip3 allkeys poslist neglist)

generateInsertSQLForPred :: (Pred, [Atom]) -> TransMonad SQLStmt
generateInsertSQLForPred (pred1, [posatom]) = translatePosInsertAtomToSQL posatom -- set property
generateInsertSQLForPred (pred1, _) = error "unsupported number of pos and neg literals" -- set property

generateUpdateSQLForPred :: (Pred, [Atom], [Atom]) -> TransMonad SQLStmt
generateUpdateSQLForPred (pred1, [posatom], _) = translatePosUpdateAtomToSQL posatom -- set property
generateUpdateSQLForPred (pred1, [], [negatom]) = translateNegUpdateAtomToSQL negatom -- set property
generateUpdateSQLForPred (pred1, _, _) = error "unsupported number of pos and neg literals" -- set property

translateDeleteAtomToSQL :: Atom -> TransMonad SQLStmt
translateDeleteAtomToSQL (Atom pred1 args) = do
    ts <- get
    case lookup pred1 (predtablemap ts) of
        Just (OneTable tname sqlvar, qcols) -> do
            (qcol_args, sqlvar2) <- preproc0 tname sqlvar qcols pred1 args
            cond <- foldl (.&&.) SQLTrueCond <$> mapM qcolArgToDelete qcol_args
            return (SQLDeleteStmt (tname, sqlvar2) cond)
        Nothing -> error "not an updatable predicate"

translatePosInsertAtomToSQL :: Atom -> TransMonad SQLStmt
translatePosInsertAtomToSQL (Atom pred1 args) = do
    ts <- get
    case lookup pred1 (predtablemap ts) of
        Just (OneTable tname _, qcols) -> do
            let qcol_args = zip qcols args
            colexprs <- mapM qcolArgToValue qcol_args
            return (SQLInsertStmt tname colexprs [] SQLTrueCond)
        Nothing -> error "not an updatable predicate"

translatePosUpdateAtomToSQL :: Atom -> TransMonad SQLStmt
translatePosUpdateAtomToSQL (Atom pred1 args) = do
    ts <- get
    case lookup pred1 (predtablemap ts) of
        Just (OneTable tname sqlvar, qcols) -> do
            (keyqcol_args, propqcol_args, sqlvar2) <- preproc tname sqlvar qcols pred1 args
            cond <- foldl (.&&.) SQLTrueCond <$> mapM qcolArgToUpdateCond keyqcol_args
            set <- mapM qcolArgToSet propqcol_args
            return (SQLUpdateStmt (tname, sqlvar2) set cond)
        Nothing -> error "not an updatable predicate"


translateNegUpdateAtomToSQL :: Atom -> TransMonad SQLStmt
translateNegUpdateAtomToSQL (Atom pred1 args) = do
    ts <- get
    case lookup pred1 (predtablemap ts) of
        Just (OneTable tname sqlvar, qcols) -> do
            (keyqcol_args, propqcol_args, sqlvar2) <- preproc tname sqlvar qcols pred1 args
            cond <- foldl (.&&.) SQLTrueCond <$> mapM qcolArgToUpdateCond keyqcol_args
            (set, conds) <- unzip <$> mapM qcolArgToSetNull propqcol_args
            return (SQLUpdateStmt (tname, sqlvar2) set (foldl (.&&.) cond conds))
        Nothing -> error "not an updatable predicate"

qcolArgToDelete :: (SQLQualifiedCol, Expr) -> TransMonad SQLCond
qcolArgToDelete (qcol, arg) = do
    sqlexpr <- sqlExprFromArg arg
    case sqlexpr of
        Left sqlexpr -> return (SQLColExpr qcol .=. sqlexpr)
        Right var -> error ("unbounded var " ++ show var)

qcolArgToValue :: (SQLQualifiedCol, Expr) -> TransMonad (Col, SQLExpr)
qcolArgToValue (qcol@(var, col), arg) = do
    sqlexpr <- sqlExprFromArg arg
    case sqlexpr of
        Left sqlexpr -> return (col, sqlexpr)
        Right _ -> error "set value to unbounded var"

qcolArgToUpdateCond :: (SQLQualifiedCol, Expr) -> TransMonad SQLCond
qcolArgToUpdateCond (qcol, arg) = do
    sqlexpr <- sqlExprFromArg arg
    case sqlexpr of
        Left sqlexpr -> return (SQLColExpr qcol .=. sqlexpr)
        Right var -> do
            addVarRep var (SQLColExpr qcol)
            return SQLTrueCond -- unbounded var

qcolArgToCond :: (SQLQualifiedCol, Expr) -> TransMonad SQLCond
qcolArgToCond (qcol, arg) = do
    sqlexpr <- sqlExprFromArg arg
    case sqlexpr of
        Left sqlexpr -> return (SQLColExpr qcol .=. sqlexpr)
        Right var -> do
            addVarRep var (SQLColExpr qcol)
            return SQLTrueCond -- unbounded var

qcolArgToSet :: (SQLQualifiedCol, Expr) -> TransMonad (Col, SQLExpr)
qcolArgToSet ((var, col), arg) = do
    sqlexpr <- sqlExprFromArg arg
    case sqlexpr of
        Left sqlexpr -> return (col, sqlexpr)
        Right _ -> error "set value to unbounded var"

qcolArgToSetNull :: (SQLQualifiedCol, Expr) -> TransMonad ((Col, SQLExpr), SQLCond)
qcolArgToSetNull (qcol@(var, col), arg) = do
    sqlexpr <- sqlExprFromArg arg
    case sqlexpr of
        Left sqlexpr -> return ((col, SQLNullExpr), SQLColExpr qcol .=. sqlexpr)
        Right var -> do
            addVarRep var (SQLColExpr qcol)
            return ((col, SQLNullExpr), SQLTrueCond)


data SQLTrans = SQLTrans  BuiltIn PredTableMap

data KeyState = KeyState {
    queryKeys:: [(String, [Expr])],
    updateKey:: Maybe (String, [Expr]),
    ksDeleteProp :: [Pred],
    ksDeleteObj :: Bool,
    ksInsertProp :: [Pred],
    ksInsertObj :: Bool,
    ksQuery :: Bool
}

pureOrExecF :: SQLTrans -> Formula -> StateT KeyState Maybe ()
pureOrExecF  (SQLTrans  builtin predtablemap) (FAtomic (Atom n args)) = do
    ks <- get
    if isJust (updateKey ks)
        then lift Nothing
        else case lookup n predtablemap of
                Nothing -> do
                    trace ("pureOrExecF': cannot find table for predicate " ++ show n ++ " ignored") $ return ()
                Just (OneTable tablename _, _) -> do
                    let key = keyComponents n args
                    put ks{queryKeys = queryKeys ks `union` [(tablename, key)]}
pureOrExecF  trans (FTransaction ) =  lift Nothing

pureOrExecF  trans (FSequencing form1 form2) = do
    pureOrExecF  trans form1
    pureOrExecF  trans form2

pureOrExecF  trans (FPar form1 form2) = do
    ks <- get
    if isJust (updateKey ks)
        then lift Nothing
        else do
            put ks {ksQuery = True}
            pureOrExecF  trans form1
            pureOrExecF  trans form2
pureOrExecF  trans (FChoice form1 form2) = do
    ks <- get
    if isJust (updateKey ks)
        then lift Nothing
        else do
            put ks {ksQuery = True}
            pureOrExecF  trans form1
            pureOrExecF  trans form2
pureOrExecF  (SQLTrans  builtin predtablemap) (FInsert (Lit sign0 (Atom pred0 args))) = do
            ks <- get
            if ksQuery ks
                then lift Nothing
                else do
                    let key = keyComponents pred0 args
                        tablename = case lookup pred0 predtablemap of
                            Just (OneTable tn _, _) -> tn
                            Nothing -> error ("pureOrExecF: cannot find table for predicate " ++ show pred0)
                    let isObject = isObjectPred pred0
                    let isDelete = case sign0 of
                            Pos -> False
                            Neg -> True
                    case updateKey ks of
                        Nothing ->
                            if isObject
                                then if isDelete
                                    then if not (superset [(tablename, key)] (queryKeys ks))
                                        then lift Nothing
                                        else put ks{updateKey = Just (tablename, key), ksDeleteObj = True}
                                    else put ks{updateKey = Just (tablename, key), ksInsertObj = True}
                                else if isDelete
                                    then if not (superset [(tablename, key)] (queryKeys ks))
                                        then lift Nothing
                                        else put ks{updateKey = Just (tablename, key), ksDeleteProp = [pred0]}
                                    else put ks{updateKey = Just (tablename, key), ksInsertProp = [pred0]}
                        Just key' ->
                            if isObject
                                then if isDelete
                                    then if not (null (ksInsertProp ks)) || ksInsertObj ks || ksDeleteObj ks || (tablename, key) /= key' || not (superset [(tablename, key)] (queryKeys ks))
                                        then lift Nothing
                                        else put ks{ksDeleteObj = True}
                                    else lift Nothing
                                else if isDelete
                                    then if not (null (ksInsertProp ks)) || ksInsertObj ks || ksDeleteObj ks || pred0 `elem` (ksDeleteProp ks) || (tablename, key) /= key' || not (superset [(tablename, key)] (queryKeys ks))
                                        then lift Nothing
                                        else put ks{ksDeleteProp = ksDeleteProp ks ++ [pred0]}
                                    else if not (null (ksDeleteProp ks)) || ksDeleteObj ks || pred0 `elem` (ksInsertProp ks) || (tablename, key) /= key'
                                        then lift Nothing
                                        else put ks{ksInsertProp = ksInsertProp ks ++ [pred0]}

pureOrExecF  _ FOne = return ()
pureOrExecF  _ FZero = return ()
pureOrExecF trans (Aggregate Not form) = do
    ks <- get
    if isJust (updateKey ks)
        then lift Nothing
        else do
            put ks {ksQuery = True}
            pureOrExecF trans form
pureOrExecF trans (Aggregate Exists form) = do
  ks <- get
  if isJust (updateKey ks)
      then lift Nothing
      else do
          put ks {ksQuery = True}
          pureOrExecF trans form
pureOrExecF _ (Aggregate _ _) =
  lift Nothing
pureOrExecF trans (FReturn _) = lift Nothing

limitF :: SQLTrans -> Formula -> StateT KeyState Maybe ()
limitF trans (Aggregate (Limit _) form) = do
  ks <- get
  put ks {ksQuery = True}
  limitF trans form
limitF trans form = orderByF trans form

orderByF :: SQLTrans -> Formula -> StateT KeyState Maybe ()
orderByF trans (Aggregate (OrderByAsc _) form) = do
  ks <- get
  put ks {ksQuery = True}
  orderByF trans form
orderByF trans form = summarizeF trans form

summarizeF :: SQLTrans -> Formula -> StateT KeyState Maybe ()
summarizeF trans (Aggregate (Summarize _) form) = do
  ks <- get
  put ks {ksQuery = True}
  pureOrExecF trans form
summarizeF trans form = pureOrExecF trans form



instance Translate SQLTrans MapResultRow SQLQuery where
    translateQueryWithParams trans ret query env =
        let (SQLTrans  builtin predtablemap) = trans
            env2 = foldl (\map2 key@(Var w)  -> insert key (SQLParamExpr w) map2) empty env
            (sql, ts') = runNew (runStateT (translateQueryToSQL (toAscList ret) query) (TransState {builtin = builtin, predtablemap = predtablemap, repmap = env2, tablemap = empty})) in
            (sql, params sql)
    translateable trans form vars = layeredF form && isJust (evalStateT (limitF  trans form) (KeyState [] Nothing [] False [] False (not (null vars))) )

instance ExtractDomainSize DBAdapterMonad conn SQLTrans where
    extractDomainSize _ trans varDomainSize (Atom name args) =
        if isBuiltIn
            then return bottom -- assume that builtins don't restrict domain size
            else return (if name `member` predtablemap
                    then Set.unions (map (\arg -> case arg of
                            (VarExpr v) -> Set.singleton v
                            _ -> bottom) args)
                    else bottom) where
                isBuiltIn = name `member` builtin
                (SQLTrans (BuiltIn builtin) predtablemap) = trans
