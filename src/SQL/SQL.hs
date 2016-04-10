{-# LANGUAGE TypeSynonymInstances, FlexibleInstances, MultiParamTypeClasses, FlexibleContexts, RankNTypes, GADTs, UndecidableInstances #-}
module SQL.SQL where

import QueryPlan
import FO.Data hiding (Subst, subst)
import FO.Domain
import DBQuery
import Utils

import Prelude hiding (lookup)
import Data.List (intercalate, (\\),union)
import Control.Monad.Trans.State.Strict (StateT, get, put, evalStateT, runStateT, modify)
import Control.Monad.Trans.Class (lift)
import Data.Map.Strict (empty, Map, insert, member, singleton, lookup, fromList, keys, alter, toList, elems, size, delete)
import Data.Monoid
import Data.Maybe
import Control.Monad.Trans.Except
import Debug.Trace
import Data.Convertible
import qualified Data.Text as T

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
             | SQLFuncExpr String [SQLExpr] deriving (Eq, Ord)

isSQLConstExpr :: SQLExpr -> Bool
isSQLConstExpr (SQLIntConstExpr _ ) = True
isSQLConstExpr (SQLStringConstExpr _ ) = True
isSQLConstExpr (SQLParamExpr _) = True
isSQLConstExpr _ = False

data SQLCond = SQLCompCond SQLOper SQLExpr SQLExpr
             | SQLAndCond SQLCond SQLCond
             | SQLOrCond SQLCond SQLCond
             | SQLExistsCond SQL0
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


data SQL0 = SQL0 {sqlSelect :: [ SQLExpr ], sqlFrom :: SQLTableList, sqlWhere :: SQLCond} deriving (Eq, Ord)

data SQL = SQLQ SQL0
    | SQLInsert TableName [(Col,SQLExpr)] [Table] SQLCond
    | SQLUpdate (TableName, SQLVar) [(Col,SQLExpr)] SQLCond
    | SQLDelete (TableName, SQLVar) SQLCond deriving (Eq, Ord)

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
    show2 SQLNullExpr _ = "NULL"

instance Show SQLVar where
    show (SQLVar var) = var

showWhereCond2 :: SQLCond -> [SQLVar] -> String
showWhereCond2 cond sqlvar = case cond of
    SQLTrueCond -> ""
    _ -> " WHERE " ++ show2 cond sqlvar

instance Show2 SQL0 where
    show2 (SQL0 cols tables conds) sqlvar = "SELECT " ++ intercalate "," (map show cols) ++
            (if null tables
                then ""
                else " FROM " ++ intercalate "," (map show tables)) ++
            (showWhereCond2 conds sqlvar)


instance Show SQL where
    show (SQLQ sql) = show2 sql []

    show (SQLInsert tname colsexprs tables cond) =
        let (cols, exprs) = unzip colsexprs in
            "INSERT INTO " ++ tname ++ " (" ++ intercalate "," cols ++ ")" ++
                if null tables && all isSQLConstExpr exprs
                    then " VALUES (" ++ intercalate "," (map (\a -> show2 a []) exprs)++ ")"
                    else " SELECT " ++ intercalate "," (map (\a -> show2 a []) exprs) ++ (if null tables
                        then ""
                        else " FROM " ++ intercalate "," (map show tables)) ++ showWhereCond2 cond []
    show (SQLDelete (tname, sqlvar) cond)  =
        "DELETE FROM " ++ tname ++ showWhereCond2  cond [sqlvar]

    show (SQLUpdate (tname, sqlvar) colsexprs cond)  =
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

type SQLQuery = ([Var], SQL, [Var]) -- return vars, sql, param vars

instance Monoid SQL0 where
    (SQL0 sselect1 sfrom1 swhere1) `mappend` (SQL0 sselect2 sfrom2 swhere2) =
        SQL0 (sselect1 ++ sselect2) (sfrom1 `mergeTables` sfrom2) (swhere1 .&&. swhere2)
    mempty = SQL0 [] [] SQLTrueCond

sor (SQL0 sselect1 sfrom1 swhere1) (SQL0 sselect2 sfrom2 swhere2) = SQL0 (sselect1 ++ sselect2) (sfrom1 `mergeTables` sfrom2) (swhere1 .||. swhere2)

negCond :: SQLCond -> SQLCond
negCond cond = SQLNotCond cond

snot :: SQL0 -> SQL0
snot (SQL0 sselect sfrom swhere) = SQL0 sselect sfrom (negCond swhere)

swhere swhere1 = SQL0 [] [] swhere1
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
type PredTableMap = Map String (Table, [SQLQualifiedCol])
-- table -> cols, primary key
type Schema = Map TableName ([Col], [Col])
-- builtin predicate -> op, neg op
newtype BuiltIn = BuiltIn (Map String (Sign -> [Expr] -> TransMonad SQL0))

simpleBuildIn :: (Sign -> [SQLExpr] -> TransMonad SQL0) -> Sign -> [Expr] -> TransMonad SQL0
simpleBuildIn builtin sign args = do
    let err m = do
                a <- m
                case a of
                    Left expr -> return expr
                    Right _ -> error "unconstrained argument to built-in predicate"
    sqlExprs <- mapM (err . sqlExprFromArg) args
    builtin sign sqlExprs


data TransState = TransState {
    schema :: Schema,
    builtin :: BuiltIn,
    predtablemap :: PredTableMap,
    repmap :: RepMap,
    tablemap :: TableMap,
    rigidVars :: [Var] -- this is a list of free vars that appear in atoms to be deleted they must be linear
}
type TransMonad a = StateT TransState NewEnv a

freshSQLVar :: TableName -> TransMonad SQLVar
freshSQLVar tablename = lift $ SQLVar <$> new (StringWrapper tablename)

sqlExprFromArg :: Expr -> TransMonad (Either SQLExpr Var)
sqlExprFromArg arg = do
    ts <- get
    case arg of
        VarExpr var2 ->
            return (if var2 `elem` rigidVars ts
                then error (show var2 ++ " is rigid")
                else case lookup  var2 (repmap ts) of
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

addRigidVar :: Var -> TransMonad ()
addRigidVar var =
    modify (\ts -> ts {rigidVars = rigidVars ts ++ [var]})

condFromArg :: (SQLExpr -> SQLExpr -> SQLCond) -> (Expr, SQLQualifiedCol) -> TransMonad SQLCond
condFromArg op (arg, col) = do
    v <- sqlExprFromArg arg
    case v of
        Left expr -> return (op (SQLColExpr col) expr)
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

instance Params SQL where
    params (SQLQ (SQL0 sel _ cond)) = params sel ++ params cond
    params (SQLInsert _ vs _ cond) = params (map snd vs) ++ params cond
    params (SQLUpdate _ vs cond) = params (map snd vs) ++ params cond
    params (SQLDelete _ cond) = params cond

instance Params SQL0 where
    params (SQL0 sel _ cond) = params sel ++ params cond

instance Params SQLQuery where
    params (_, _, params) = params

translateQueryToSQL :: Query -> TransMonad SQLQuery
translateQueryToSQL qu@(Query vars formula) = do
    (vars, sql, vars2) <- if pureF formula
        then do
            (SQL0 _ tablelist cond1) <-  translateFormulaToSQL formula
            ts <- get
            let extractCol var = case lookup var (repmap ts) of
                                    Just col -> col
                                    _ -> error ("translateQueryToSQL: " ++ show var ++ " doesn't correspond to a column while translating query " ++  show qu ++ " to SQL0, available " ++ show (repmap ts))
            let cols = map extractCol vars
                sql = SQLQ (SQL0 cols tablelist cond1)
            return (vars, sql, (params sql))
        else translateInsertToSQL formula
    return (vars, simplifySQLCond sql, vars2)

simplifySQLCond :: SQL -> SQL
simplifySQLCond (SQLQ (SQL0 s f cond)) = SQLQ (SQL0 s f (simplifySQLCond' cond))
simplifySQLCond (SQLInsert t s f cond) = SQLInsert t s f (simplifySQLCond' cond)
simplifySQLCond (SQLUpdate t cs cond) = SQLUpdate t cs (simplifySQLCond' cond)
simplifySQLCond (SQLDelete t cond) = SQLDelete t (simplifySQLCond' cond)

simplifySQLCond2 :: SQL0 -> SQL0
simplifySQLCond2 (SQL0 s f cond) = SQL0 s f (simplifySQLCond' cond)

simplifySQLCond' :: SQLCond -> SQLCond
simplifySQLCond' c@(SQLCompCond _ _ _) = c
simplifySQLCond' (SQLTrueCond) = SQLTrueCond
simplifySQLCond' (SQLFalseCond) = SQLFalseCond
simplifySQLCond' (SQLAndCond a b) = case simplifySQLCond' a of
    SQLTrueCond -> simplifySQLCond' b
    SQLFalseCond -> SQLFalseCond
    a' -> case simplifySQLCond' b of
        SQLTrueCond -> a'
        SQLFalseCond -> SQLFalseCond
        b' -> SQLAndCond a' b'
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


sqlexists sql@(SQL0 [] tablelist (SQLExistsCond _)) = sql
sqlexists (SQL0 cols tablelist cond) = SQL0 [] [] (SQLExistsCond (SQL0 cols tablelist cond))
sqlfalse = SQL0 [] [] (SQLFalseCond)

translateFormulaToSQL :: Formula -> TransMonad SQL0
translateFormulaToSQL (FClassical form) = do
    sql <- (translateFormulaToSQL (convert form))
    return (sqlexists sql)
translateFormulaToSQL (FAtomic a) = translateAtomToSQL Pos a
translateFormulaToSQL (FSequencing form1 form2) =
    mappend <$> translateFormulaToSQL form1 <*> translateFormulaToSQL form2
translateFormulaToSQL (FOne) =
    return mempty

translateFormulaToSQL (FChoice form1 form2) =
    sor <$> translateFormulaToSQL form1 <*> translateFormulaToSQL form2
translateFormulaToSQL (FZero) =
    return sqlfalse

translateFormulaToSQL' :: PureFormula -> TransMonad SQL0
translateFormulaToSQL' (CTrue) =
    return mempty

translateFormulaToSQL' (CFalse) =
    return sqlfalse
translateFormulaToSQL' (Conjunction form1 form2) =
    mappend <$> translateFormulaToSQL' form1 <*> translateFormulaToSQL' form2

translateFormulaToSQL' (Disjunction form1 form2) =
    sor <$> translateFormulaToSQL' form1 <*> translateFormulaToSQL' form2

translateFormulaToSQL' (Atomic a) = translateAtomToSQL Pos a

-- assume that all atoms in conj involves var
translateFormulaToSQL' (Exists var conj) = do
    ts <- get
    let repvar = lookup var (repmap ts)
    put ts{repmap = delete var (repmap ts)}
    sql <- translateFormulaToSQL (convert conj)
    ts' <- get
    put ts'{repmap = alter (\_ -> repvar) var (repmap ts')}
    return (sqlexists sql)

translateFormulaToSQL' (Not (Atomic a)) =
    translateAtomToSQL Neg a

-- assume that all atoms in conj involves var
translateFormulaToSQL' (Not form) = do
    snot <$> translateFormulaToSQL' form

translateFormulaToSQL' form = error ("translateFormulaToSQL: unsupported " ++ show form)

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

translateAtomToSQL :: Sign -> Atom -> TransMonad SQL0
translateAtomToSQL thesign (Atom (Pred name _) args) = do
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
                        let (_, prikeycols) = fromMaybe (error (tablename ++ " is not defined in the schema")) (lookup tablename (schema ts))
                        let (prikeyargs, prikeyargcols) = unzip [(arg, (sqlvar2, col)) | prikeycol <- prikeycols, (arg, (sqlvar2, col)) <- zip args cols, prikeycol == col ]
                        if length prikeyargs == length prikeycols -- if primary key columns correspond to args
                            then do
                                (new, v) <- lookupTableVar tablename prikeyargs
                                if new
                                    then
                                        return ([table], singleton sqlvar v, cols, args)
                                    else do
                                        let cols2 = cols \\ prikeyargcols
                                        let args2 = args \\ prikeyargs
                                        return ([], singleton sqlvar v, cols2 , args2)
                            else do
                                sqlvar2 <- freshSQLVar tablename
                                return ([table], singleton sqlvar sqlvar2, cols, args)

                let tables2 = map (subst varmap) tables
                let cols3 = map (subst varmap) cols2
                condsFromArgs <- mapM (condFromArg (.=.)) (zip args2 cols3)
                let cond3 = foldl (.&&.) SQLTrueCond condsFromArgs
                return (SQL0 [] tables2 cond3)
            Nothing -> error (name ++ " is not defined")



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
    (SQL0 _ tablelist cond) <- translateFormulaToSQL conj
    let keymap = sortByKey lits
    if size keymap > 1
        then error "translateInsertToSQL: more than one key"
        else do
            insertparts <- sortParts <$> (concat <$> mapM combineLitsSQL (elems keymap))
            case insertparts of
                [insertpart] -> do
                    ts <- get
                    let sql = toInsert tablelist cond insertpart
                    return ([], sql, params sql)
                _ -> error "translateInsertToSQL: more than one actions"

-- each SQL0 must be an Insert statement
sortParts :: [SQL] -> [SQL]
sortParts [] = []
sortParts (p : ps) = b++[a] where
    (a, b) = foldl (\(active, done) part ->
        case (active, part) of
            (SQLInsert tname colexprs tablelist cond, SQLInsert tname2 colexprs2 tablelist2 cond2)
                | tname == tname2 && compatible colexprs colexprs2 ->
                    (SQLInsert tname ( colexprs `union` colexprs2) (tablelist ++ tablelist2) (cond .&&. cond2), done)
            _ -> (part, active : done)) (p,[]) ps where
            compatible colexpr = all (\(col, expr) -> all (\(col2, expr2) ->col2 /= col || expr2 == expr) colexpr)

toInsert :: [Table] -> SQLCond -> SQL -> SQL
toInsert tablelist cond (SQLInsert tname colexprs tablelist2 cond2) = SQLInsert tname colexprs (tablelist ++ tablelist2) (cond .&&. cond2)
toInsert tablelist cond (SQLDelete tname cond2) = SQLDelete tname (cond .&&. cond2)
toInsert tablelist cond (SQLUpdate tname colexprs cond2) = SQLUpdate tname colexprs (cond .&&. cond2)


combineLitsSQL :: [Lit] -> TransMonad [SQL]
combineLitsSQL lits = combineLits lits generateUpdateSQL generateInsertSQL generateDeleteSQL

preproc0 tname sqlvar qcols predtype args = do
        let key = keyComponents predtype args
        (_, sqlvar2) <- lookupTableVar tname key
        let varmap = singleton sqlvar sqlvar2
        let qcol_args = zip (subst varmap qcols) args
        return (qcol_args, sqlvar2)

preproc tname sqlvar qcols predtype args = do
        (qcol_args , sqlvar2) <- preproc0 tname sqlvar qcols predtype args
        let keyqcol_args = keyComponents predtype qcol_args
        let propqcol_args = propComponents predtype qcol_args
        return (keyqcol_args, propqcol_args, sqlvar2)

generateDeleteSQL :: Atom -> TransMonad [SQL]
generateDeleteSQL atom = do
    sql <- translateDeleteAtomToSQL atom
    return [sql]

generateInsertSQL :: [Atom] -> TransMonad [SQL]
generateInsertSQL atoms = do
    let map1 = sortAtomByPred atoms
    mapM generateInsertSQLForPred (toList map1)

generateUpdateSQL :: [Atom] -> [Atom] -> TransMonad [SQL]
generateUpdateSQL pospropatoms negpropatoms = do
    let posprednamemap = sortAtomByPred pospropatoms
    let negprednamemap = sortAtomByPred negpropatoms
    let allkeys = keys posprednamemap `union` keys negprednamemap
    let poslist = [l | key <- allkeys, let l = case lookup key posprednamemap of Nothing -> []; Just l -> l]
    let neglist = [l | key <- allkeys, let l = case lookup key negprednamemap of Nothing -> []; Just l -> l]
    mapM generateUpdateSQLForPred (zip3 allkeys poslist neglist)

generateInsertSQLForPred :: (String, [Atom]) -> TransMonad SQL
generateInsertSQLForPred (pred, [posatom]) = translatePosInsertAtomToSQL posatom -- set property
generateInsertSQLForPred (pred, _) = error "unsupported number of pos and neg literals" -- set property

generateUpdateSQLForPred :: (String, [Atom], [Atom]) -> TransMonad SQL
generateUpdateSQLForPred (pred, [posatom], _) = translatePosUpdateAtomToSQL posatom -- set property
generateUpdateSQLForPred (pred, [], [negatom]) = translateNegUpdateAtomToSQL negatom -- set property
generateUpdateSQLForPred (pred, _, _) = error "unsupported number of pos and neg literals" -- set property

translateDeleteAtomToSQL :: Atom -> TransMonad SQL
translateDeleteAtomToSQL (Atom (Pred pred predtype) args) = do
    ts <- get
    case lookup pred (predtablemap ts) of
        Just (OneTable tname sqlvar, qcols) -> do
            (qcol_args, sqlvar2) <- preproc0 tname sqlvar qcols predtype args
            cond <- foldl (.&&.) SQLTrueCond <$> mapM qcolArgToDelete qcol_args
            return (SQLDelete (tname, sqlvar2) cond)
        Nothing -> error "not an updatable predicate"

translatePosInsertAtomToSQL :: Atom -> TransMonad SQL
translatePosInsertAtomToSQL (Atom (Pred pred predtype) args) = do
    ts <- get
    case lookup pred (predtablemap ts) of
        Just (OneTable tname _, qcols) -> do
            let qcol_args = zip qcols args
            colexprs <- mapM qcolArgToValue qcol_args
            return (SQLInsert tname colexprs [] SQLTrueCond)
        Nothing -> error "not an updatable predicate"

translatePosUpdateAtomToSQL :: Atom -> TransMonad SQL
translatePosUpdateAtomToSQL (Atom (Pred pred predtype) args) = do
    ts <- get
    case lookup pred (predtablemap ts) of
        Just (OneTable tname sqlvar, qcols) -> do
            (keyqcol_args, propqcol_args, sqlvar2) <- preproc tname sqlvar qcols predtype args
            cond <- foldl (.&&.) SQLTrueCond <$> mapM qcolArgToUpdateCond keyqcol_args
            set <- mapM qcolArgToSet propqcol_args
            return (SQLUpdate (tname, sqlvar2) set cond)
        Nothing -> error "not an updatable predicate"


translateNegUpdateAtomToSQL :: Atom -> TransMonad SQL
translateNegUpdateAtomToSQL (Atom (Pred pred predtype) args) = do
    ts <- get
    case lookup pred (predtablemap ts) of
        Just (OneTable tname sqlvar, qcols) -> do
            (keyqcol_args, propqcol_args, sqlvar2) <- preproc tname sqlvar qcols predtype args
            cond <- foldl (.&&.) SQLTrueCond <$> mapM qcolArgToUpdateCond keyqcol_args
            (set, conds) <- unzip <$> mapM qcolArgToSetNull propqcol_args
            return (SQLUpdate (tname, sqlvar2) set (foldl (.&&.) cond conds))
        Nothing -> error "not an updatable predicate"

qcolArgToDelete :: (SQLQualifiedCol, Expr) -> TransMonad SQLCond
qcolArgToDelete (qcol, arg) = do
    sqlexpr <- sqlExprFromArg arg
    case sqlexpr of
        Left sqlexpr -> return (SQLColExpr qcol .=. sqlexpr)
        Right var -> do
            addRigidVar var
            return SQLTrueCond -- unbounded var

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
        Left sqlexpr -> return SQLTrueCond
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


data SQLTrans = SQLTrans Schema BuiltIn PredTableMap

data KeyState = KeyState {
    queryKeys:: [[Expr]],
    updateKey:: Maybe [Expr],
    ksDeleteProp :: [Pred],
    ksDeleteObj :: Bool,
    ksInsertProp :: [Pred],
    ksInsertObj :: Bool,
    ksChoice :: Bool
}

pureOrExecF :: Bool -> SQLTrans -> Formula -> StateT KeyState Maybe ()
pureOrExecF _ trans (FAtomic (Atom (Pred _ predType) args)) = do
    ks <- get
    if isJust (updateKey ks)
        then lift $ Nothing
        else do
            let key = keyComponents predType args
            put ks{queryKeys = queryKeys ks `union` [key]}
pureOrExecF _ trans (FClassical form) = pureOrExecF' trans form
pureOrExecF top trans (FTransaction form) =  if top
    then           pureOrExecF False trans form
    else       lift $ Nothing

pureOrExecF _ trans (FSequencing form1 form2) = do
    pureOrExecF False trans form1
    pureOrExecF False trans form2

pureOrExecF _ trans (FChoice form1 form2) = do
    ks <- get
    if isJust (updateKey ks)
        then lift $ Nothing
        else do
            put ks {ksChoice = True}
            pureOrExecF False trans form1
            pureOrExecF False trans form2
pureOrExecF _ (SQLTrans schema builtin predtablemap) (FInsert (Lit sign0 (Atom pred0@(Pred _ predType@(PredType predKind _)) args))) = do
            ks <- get
            if ksChoice ks
                then lift $ Nothing
                else do
                    let key = keyComponents predType args
                    let isObject = case predKind of
                            ObjectPred -> True
                            PropertyPred -> False
                    let isDelete = case sign0 of
                            Pos -> False
                            Neg -> True
                    case updateKey ks of
                        Nothing ->
                            if isObject
                                then if isDelete
                                    then put ks{updateKey = Just key, ksDeleteObj = True}
                                    else put ks{updateKey = Just key, ksInsertObj = True}
                                else if isDelete
                                    then put ks{updateKey = Just key, ksDeleteProp = [pred0]}
                                    else put ks{updateKey = Just key, ksInsertProp = [pred0]}
                        Just key' ->
                            if isObject
                                then if isDelete
                                    then if not (null (ksInsertProp ks)) || ksInsertObj ks || ksDeleteObj ks || key /= key' || case queryKeys ks of
                                                                                                                                    [] -> False
                                                                                                                                    [key''] -> key /= key''
                                                                                                                                    _ -> True
                                        then lift $ Nothing
                                        else put ks{ksDeleteObj = True}
                                    else lift $ Nothing
                                else if isDelete
                                    then if not (null (ksInsertProp ks)) || ksInsertObj ks || ksDeleteObj ks || pred0 `elem` (ksDeleteProp ks) || key /= key'
                                        then lift $ Nothing
                                        else put ks{ksDeleteProp = ksDeleteProp ks ++ [pred0]}
                                    else if not (null (ksDeleteProp ks)) || ksDeleteObj ks || pred0 `elem` (ksInsertProp ks) || key /= key'
                                        then lift $ Nothing
                                        else put ks{ksInsertProp = ksInsertProp ks ++ [pred0]}

pureOrExecF _ _ FOne = return ()
pureOrExecF _ _ FZero = return ()

pureOrExecF' :: SQLTrans -> PureFormula -> StateT KeyState Maybe ()
pureOrExecF' trans (Atomic (Atom (Pred _ predType) args)) = do
    ks <- get
    if isJust (updateKey ks)
        then lift $ Nothing
        else do
            let key = keyComponents predType args
            put ks{queryKeys = queryKeys ks `union` [key]}
pureOrExecF' trans (Conjunction form1 form2) = do
    pureOrExecF' trans form1
    pureOrExecF' trans form2
pureOrExecF' trans (Disjunction form1 form2) = do
    pureOrExecF' trans form1
    pureOrExecF' trans form2
pureOrExecF' trans (Not form) = pureOrExecF' trans form
pureOrExecF' trans (Exists _ form) = pureOrExecF' trans form
pureOrExecF' trans (Forall _ _) = lift $ Nothing
pureOrExecF' trans (CTrue) = return ()
pureOrExecF' trans (CFalse) = return ()


instance Translate SQLTrans MapResultRow SQLQuery where
    translateQueryWithParams trans query env =
      trace ("translateQueryWithParams: translating " ++ show query ++ " with " ++ show env) $
        let (SQLTrans schema builtin predtablemap) = trans
            env2 = foldl (\map2 key@(Var w)  -> insert key (SQLParamExpr w) map2) empty env
            (sql, ts') = runNew (runStateT (translateQueryToSQL query) (TransState {schema = schema, builtin = builtin, predtablemap = predtablemap, repmap = env2, tablemap = empty, rigidVars = []})) in
            trace ("translateQueryWithParams: and the resulting query is " ++ show sql) $ (sql, params sql)
    translateable trans form vars = isJust (evalStateT (pureOrExecF True trans form) (KeyState [] Nothing [] False [] False (not (null vars))) )
    translateable' trans form vars = isJust (evalStateT (pureOrExecF' trans form) (KeyState [] Nothing [] False [] False (not (null vars))))

instance DBConnection conn SQLQuery => ExtractDomainSize DBAdapterMonad conn SQLTrans where
    extractDomainSize _ trans varDomainSize (Atom (Pred name _) args) =
        if isBuiltIn
            then return maxArgDomainSize -- assume that builtins don't restrict domain size
            else return (if name `member` predtablemap
                    then fromList [(fv, Bounded 1) | fv <- freeVars args] -- just set bound to 1, we only use Bounded 1 or Unbounded for now
                    else empty) where
                argsDomainSizeMaps = map (exprDomainSizeMap varDomainSize Unbounded) args
                maxArgDomainSize = mmaxs argsDomainSizeMaps
                isBuiltIn = name `member` builtin
                (SQLTrans _ (BuiltIn builtin) predtablemap) = trans
