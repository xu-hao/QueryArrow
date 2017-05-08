{-# LANGUAGE TypeSynonymInstances, FlexibleInstances, MultiParamTypeClasses, FlexibleContexts, RankNTypes, GADTs, PatternSynonyms, TypeFamilies #-}
module QueryArrow.SQL.SQL where

import QueryArrow.FO.Data hiding (Subst, subst)
import QueryArrow.FO.Types
import QueryArrow.FO.Utils
import QueryArrow.DB.GenericDatabase
import QueryArrow.ListUtils
import QueryArrow.Utils
import QueryArrow.FO.Domain

import Prelude hiding (lookup)
import Data.List (intercalate, (\\),union, nub)
import Control.Monad.Trans.State.Strict (StateT, get, put, evalStateT, runStateT, modify)
import Control.Monad.Trans.Class (lift)
import Data.Map.Strict (empty, Map, insert, member, singleton, lookup, fromList, keys, toList, elems, size)
import Data.Monoid ((<>))
import Data.Maybe
import Debug.Trace
import qualified Data.Text as T
import Data.Set (toAscList, Set)
import qualified Data.Set as Set
import Algebra.Lattice
import Algebra.Lattice.Dropped
import Algebra.Lattice.Ordered

type Col = String
type TableName = String
data Table = OneTable TableName SQLVar deriving (Eq, Ord, Show, Read)

type SQLTableList = [Table]

-- merge two lists of sql tables
-- only support onetable
mergeTables :: SQLTableList -> SQLTableList -> SQLTableList
mergeTables = union

newtype SQLVar = SQLVar {unSQLVar :: String} deriving (Eq, Ord, Show, Read)

type SQLQualifiedCol = (SQLVar, Col)

type SQLOper = String

data SQLExpr = SQLColExpr SQLQualifiedCol
             | SQLIntConstExpr Integer
             | SQLStringConstExpr T.Text
             | SQLPatternExpr T.Text
             | SQLNullExpr
             | SQLParamExpr String
             | SQLExprText String
             | SQLCastExpr SQLExpr String
             | SQLInfixFuncExpr String SQLExpr SQLExpr
             | SQLFuncExpr String [SQLExpr]
             | SQLFuncExpr2 String SQLExpr deriving (Eq, Ord, Show)

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
             deriving (Eq, Ord, Show)

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

data SQLOrder = ASC | DESC deriving (Eq, Ord, Show)

type IntLattice = Dropped (Ordered Int)
pattern IntLattice a = Drop (Ordered a)

data SQL = SQLQuery {sqlSelect :: [ (Var, SQLExpr) ], sqlFrom :: SQLTableList, sqlWhere :: SQLCond, sqlOrderBy :: [(SQLExpr, SQLOrder)], sqlLimit :: IntLattice, sqlDistinct :: Bool, sqlGroupBy :: [SQLExpr]} deriving (Eq, Ord, Show)

data SQLStmt = SQLQueryStmt SQL
    | SQLInsertStmt TableName [(Col,SQLExpr)] [Table] SQLCond
    | SQLUpdateStmt (TableName, SQLVar) [(Col,SQLExpr)] SQLCond
    | SQLDeleteStmt (TableName, SQLVar) SQLCond deriving (Eq, Ord, Show)

instance Serialize Table where
    serialize (OneTable tablename var) = tablename ++ " " ++ serialize var

instance Serialize SQLCond where
    serialize a = show2 a []
instance Serialize SQLExpr where
    serialize a = show2 a []

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
        else serialize var ++ "." ++ col
    show2 (SQLIntConstExpr i) _ = show i
    show2 (SQLStringConstExpr s) _ = "'" ++ sqlStringEscape (T.unpack s) ++ "'"
    show2 (SQLPatternExpr s) _ = "'" ++ sqlPatternEscape (T.unpack s) ++ "'"
    show2 (SQLParamExpr _) _ = "?"
    show2 (SQLCastExpr arg ty) sqlvar =  "cast(" ++ show2 arg sqlvar ++ " as " ++ ty ++ ")"
    show2 (SQLInfixFuncExpr fn a b) sqlvar = "(" ++ show2 a sqlvar ++ fn ++ show2 b sqlvar ++ ")"
    show2 (SQLFuncExpr fn args) sqlvar = fn ++ "(" ++ intercalate "," (map (\a -> show2 a sqlvar) args) ++ ")"
    show2 (SQLFuncExpr2 fn arg) sqlvar = fn ++ " " ++ show2 arg sqlvar
    show2 (SQLExprText s) _ = s
    show2 SQLNullExpr _ = "NULL"

instance Serialize SQLVar where
    serialize (SQLVar var) = var

showWhereCond2 :: SQLCond -> [SQLVar] -> String
showWhereCond2 cond sqlvar = case cond of
    SQLTrueCond -> ""
    _ -> " WHERE " ++ show2 cond sqlvar

instance Show2 SQL where
    show2 (SQLQuery cols tables conds orderby limit distinct groupby) sqlvar = "SELECT " ++ (if distinct then "DISTINCT " else "") ++ (if null cols then "1" else intercalate "," (map (\(var, expr) -> show2 expr sqlvar ++ " AS \"" ++ serialize var ++ "\"") cols)) ++
            (if null tables
                then ""
                else " FROM " ++ intercalate "," (map serialize tables)) ++
            (showWhereCond2 conds sqlvar) ++
            (if null groupby
                then ""
                else " GROUP BY " ++ intercalate "," (map serialize groupby)) ++
            (if null orderby
                then ""
                else " ORDER BY " ++ intercalate "," (map (\(expr, ord) -> serialize expr ++ " " ++ case ord of
                                                                                                    ASC -> "ASC"
                                                                                                    DESC -> "DESC") orderby)) ++
            (case limit of
                Top -> ""
                IntLattice n -> " LIMIT " ++ show n)


instance Serialize SQLStmt where
    serialize (SQLQueryStmt sql) = show2 sql []

    serialize (SQLInsertStmt tname colsexprs tables cond) =
        let (cols, exprs) = unzip colsexprs in
            "INSERT INTO " ++ tname ++ " (" ++ intercalate "," cols ++ ")" ++
                if null tables && all isSQLConstExpr exprs
                    then " VALUES (" ++ intercalate "," (map (\a -> show2 a []) exprs)++ ")"
                    else " SELECT " ++ intercalate "," (map (\a -> show2 a []) exprs) ++ (if null tables
                        then ""
                        else " FROM " ++ intercalate "," (map serialize tables)) ++ showWhereCond2 cond []
    serialize (SQLDeleteStmt (tname, sqlvar) cond)  =
        "DELETE FROM " ++ tname ++ showWhereCond2  cond [sqlvar]

    serialize (SQLUpdateStmt (tname, sqlvar) colsexprs cond)  =
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

type SQLQuery0 = ([Var], SQLStmt) -- return vars, sql
type SQLQuery = ([Var], SQLStmt, [Var]) -- return vars, sql, param vars

instance Monoid SQL where
    (SQLQuery sselect1 sfrom1 swhere1 [] Top False []) `mappend` (SQLQuery sselect2 sfrom2 swhere2 [] Top False []) =
            SQLQuery (sselect1 ++ sselect2) (sfrom1 `mergeTables` sfrom2) (swhere1 .&&. swhere2) [] Top False []
    _ `mappend` _ =
            error "sand: incompatible order by, limit, distinct, or group by"
    mempty = SQLQuery [] [] SQLTrueCond [] top False []

sor :: SQL -> SQL -> SQL
sor (SQLQuery sselect1 sfrom1 swhere1 [] Top False []) (SQLQuery sselect2 sfrom2 swhere2 [] Top False []) = SQLQuery (sselect1 ++ sselect2) (sfrom1 `mergeTables` sfrom2) (swhere1 .||. swhere2) [] top False []
sor _ _ = error "sor: incompatible order by, limit, distinct, or group by"

snot :: SQL -> SQL
snot (SQLQuery sselect sfrom swhere _ (Drop (Ordered 0)) _ _) = mempty
snot (SQLQuery sselect sfrom swhere _ _ _ _) = SQLQuery sselect sfrom (SQLNotCond swhere) [] top False []

swhere :: SQLCond -> SQL
swhere swhere1 = SQLQuery [] [] swhere1 [] top False []

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
type PredTableMap = Map PredName (Table, [SQLQualifiedCol])
-- table -> cols, primary key
type Schema = Map TableName ([Col], [Col])
-- builtin predicate -> op, neg op
newtype BuiltIn = BuiltIn (Map PredName ([Expr] -> TransMonad SQL))

simpleBuildIn :: String -> ([SQLExpr] -> TransMonad SQL) -> [Expr] -> TransMonad SQL
simpleBuildIn n builtin  args = do
    let err m = do
                a <- m
                case a of
                    Left expr -> return expr
                    Right _ -> error ("unconstrained argument to built-in predicate " ++ n)
    sqlExprs <- mapM (err . sqlExprFromArg) args
    builtin sqlExprs

repBuildIn :: ([Either SQLExpr Var] -> [(Var, SQLExpr)]) -> [Expr] -> TransMonad SQL
repBuildIn builtin args = do
    sqlExprs <- mapM sqlExprFromArg args
    let varexprs = builtin sqlExprs
    mapM_ (uncurry addVarRep) varexprs
    return mempty


data TransState = TransState {
    builtin :: BuiltIn,
    predtablemap :: PredTableMap,
    repmap :: RepMap,
    tablemap :: TableMap, -- this is a list of free vars that appear in atoms to be deleted they must be linear
    nextid :: Maybe Pred,
    ptm :: PredTypeMap
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
        NullExpr ->
            return (Left (SQLNullExpr))
        CastExpr t v -> do
            e2 <- sqlExprFromArg v
            case e2 of
              Left e ->
                return (Left (SQLCastExpr e (case t of
                                            TextType -> "text"
                                            Int64Type -> "integer")))
              Right var ->
                error ("unrepresented var in cast expr " ++ show var ++ " " ++ show (repmap ts))

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
    params (SQLCastExpr e _) = params e
    params (SQLInfixFuncExpr _ a b) = params a ++ params b
    params (SQLFuncExpr _ es) = foldMap params es
    params (SQLFuncExpr2 _ e) = params e
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
    params (SQLQueryStmt (SQLQuery sel _ cond _ _ _ _)) = params sel ++ params cond
    params (SQLInsertStmt _ vs _ cond) = params (map snd vs) ++ params cond
    params (SQLUpdateStmt _ vs cond) = params (map snd vs) ++ params cond
    params (SQLDeleteStmt _ cond) = params cond

instance Params SQL where
    params (SQLQuery sel _ cond _ _ _ _) = params sel ++ params cond

instance Params (Var, SQLExpr) where
    params (_, expr) = params expr

instance Params SQLQuery where
    params (_, _, params) = params

instance Serialize SQLQuery where
    serialize (_, stmt, _) = show stmt

translateQueryToSQL :: [Var] -> Formula -> TransMonad SQLQuery
translateQueryToSQL vars formula = do
    ts <- get
    let nextid1 = nextid ts
    let repmap1 = repmap ts
    case formula of
        FAtomic (Atom p [VarExpr v]) | (predName <$> nextid1) == Just p ->
            if v `member` repmap1
                then error (show "translateQueryToSQL: nextid " ++ show v ++ " is already bound")
                else return ([v], SQLQueryStmt (SQLQuery {sqlSelect = [(v, SQLFuncExpr "nextval" [SQLStringConstExpr (T.pack "R_ObjectId")])], sqlFrom = [], sqlWhere = SQLTrueCond, sqlDistinct = False, sqlOrderBy = [], sqlLimit = top, sqlGroupBy = []}), [])
        _ -> do
            (vars, sql) <- if pureF formula
                then do
                    (SQLQuery sels tablelist cond1 orderby limit distinct groupby) <-  translateFormulaToSQL formula
                    ts <- get
                    let map2 = fromList sels <> repmap ts
                    let extractCol var = case lookup var map2 of
                                            Just col -> col
                                            _ -> error ("translateQueryToSQL: " ++ show var ++ " doesn't correspond to a column while translating query " ++  show formula ++ " to SQL, available " ++ show (repmap ts))
                    let cols = map extractCol vars
                        sql = SQLQueryStmt (SQLQuery (zip vars cols) tablelist cond1 orderby limit distinct groupby)
                    return (vars, sql)
                else translateInsertToSQL formula
            let sql2 = simplifySQLCond sql
            return (vars, sql2, params sql2)

simplifySQLCond :: SQLStmt -> SQLStmt
simplifySQLCond (SQLQueryStmt (SQLQuery s f cond orderby limit distinct groupby)) = SQLQueryStmt (SQLQuery s f (simplifySQLCond' cond) orderby limit distinct groupby)
simplifySQLCond (SQLInsertStmt t s f cond) = SQLInsertStmt t s f (simplifySQLCond' cond)
simplifySQLCond (SQLUpdateStmt t cs cond) = SQLUpdateStmt t cs (simplifySQLCond' cond)
simplifySQLCond (SQLDeleteStmt t cond) = SQLDeleteStmt t (simplifySQLCond' cond)

simplifySQLCond2 :: SQL -> SQL
simplifySQLCond2 (SQLQuery s f cond orderby limit distinct groupby) = SQLQuery s f (simplifySQLCond' cond) orderby limit distinct groupby

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
sqlexists (SQLQuery _ _ _ _ (Drop (Ordered 0)) _ _) = sqlfalse
sqlexists (SQLQuery cols tablelist cond _ _ _ _) = SQLQuery [] [] (SQLExistsCond (SQLQuery cols tablelist cond [] Top False [])) [] top False []

sqlfalse :: SQL
sqlfalse = SQLQuery [] [] (SQLFalseCond) [] top False []

sqlsummarize :: [(Var, SQLExpr)] -> [SQLExpr] -> SQL -> SQL
sqlsummarize funcs groupby (SQLQuery _ from whe [] Top False []) =
    SQLQuery funcs from whe [] top False groupby
sqlsummarize funcs groupby (SQLQuery _ from whe (_ : _) _ _ _) =
    error "cannot summarize orderby selection"
sqlsummarize funcs groupby (SQLQuery _ from whe _ (Drop _) _ _) =
    error "cannot summarize limit selection"
sqlsummarize funcs groupby (SQLQuery _ from whe _ _ True _) =
    error "cannot summarize distinct selection"
sqlsummarize funcs groupby (SQLQuery _ from whe _ _ _ (_ : _)) =
    error "cannot summarize groupby selection"

sqlorderby :: SQLOrder -> SQLExpr -> SQL -> SQL
sqlorderby ord expr (SQLQuery sel from whe orderby limit distinct groupby) =
    SQLQuery sel from whe ((expr, ord) : orderby) limit distinct groupby

sqllimit :: IntLattice -> SQL -> SQL
sqllimit n (SQLQuery sel from whe orderby limit distinct groupby) =
    SQLQuery sel from whe orderby (n /\ limit) distinct groupby

findRep :: Var -> TransMonad SQLExpr
findRep v =  do
  ts <- get
  case lookup v (repmap ts) of
        Nothing -> error ("cannot find representative for variable " ++ show v)
        Just expr -> return expr

translateFormulaToSQL :: Formula -> TransMonad SQL
translateFormulaToSQL (FAtomic a) = translateAtomToSQL a
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

translateFormulaToSQL (Aggregate Not form) =
    snot <$> translateFormulaToSQL form

translateFormulaToSQL (Aggregate (Summarize funcs groupby) conj) = do
    sql <- translateFormulaToSQL conj
    funcs' <- mapM (\(v, s) ->
        case s of
            Max v2 -> do
                rep <- findRep v2
                return (v, SQLFuncExpr "max" [rep])
            Min v2 -> do
                rep <- findRep v2
                return (v, SQLFuncExpr "min" [rep])
            Sum v2 -> do
                rep <- findRep v2
                return (v, SQLFuncExpr "sum" [rep])
            Average v2 -> do
                rep <- findRep v2
                return (v, SQLFuncExpr "average" [rep])
            Count -> return (v, SQLFuncExpr "count" [SQLExprText "*"])
            CountDistinct v2 -> do
                rep <- findRep v2
                return (v, SQLFuncExpr "count" [SQLFuncExpr2 "distinct" rep])) funcs
    groupbyreps <- mapM findRep groupby
    return (sqlsummarize funcs' groupbyreps sql)

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

translateFormulaToSQL (Aggregate Distinct form) = do
    sql <- translateFormulaToSQL form
    return sql {sqlDistinct = True}


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

translateAtomToSQL :: Atom -> TransMonad SQL
translateAtomToSQL (Atom name args) = do
    ts <- get
    let (BuiltIn builtints) = builtin ts
    --try builtin first
    case lookup name builtints of
        Just builtinpred ->
            builtinpred args
        Nothing -> case lookup name (predtablemap ts) of
            Just (table, cols) -> do
                (tables, varmap, cols2, args2) <- case table of
                    OneTable tablename sqlvar ->
                        case lookup name (ptm ts) of
                            Nothing ->
                                error ("translateAtomToSQL: cannot find predicate " ++ show name)
                            Just pt -> do
                                let prikeyargs = keyComponents pt args
                                let prikeyargcols = keyComponents pt cols
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
                return (SQLQuery [] tables2 cond3 [] top False []
                            )
            Nothing -> error (show name ++ " is not defined")



-- formula must be pure
translateInsertToSQL :: Formula -> TransMonad SQLQuery0
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

translateInsertToSQL' :: [Lit] -> Formula -> TransMonad SQLQuery0
translateInsertToSQL' lits conj = do
    (SQLQuery _ tablelist cond orderby limit distinct groupby) <- translateFormulaToSQL conj
    if distinct
        then error "cannot insert from distinct selection"
        else do
            ts <- get
            let keymap = sortByKey (ptm ts) lits
            if size keymap > 1
                then error ("translateInsertToSQL: more than one key " ++ show keymap)
                else do
                    insertparts <- sortParts <$> (concat <$> mapM combineLitsSQL (elems keymap))
                    case insertparts of
                        [insertpart] -> do
                            ts <- get
                            let sql = toInsert tablelist cond insertpart
                            return ([], sql)
                        _ -> error ("translateInsertToSQL: more than one actions " ++ show insertparts ++ show lits)

-- each SQLStmt must be an Insert statement
sortParts :: [SQLStmt] -> [SQLStmt]
sortParts [] = []
sortParts (p : ps) = b++[a] where
    (a, b) = foldl (\(active, done) part ->
        case (active, part) of
            (SQLInsertStmt tname colexprs tablelist cond, SQLInsertStmt tname2 colexprs2 tablelist2 cond2)
                | tname == tname2 && compatible colexprs colexprs2 && cond == cond2 ->
                    (SQLInsertStmt tname ( colexprs `union` colexprs2) (tablelist ++ tablelist2) cond, done)
            (SQLUpdateStmt tnamevar colexprs cond, SQLUpdateStmt tnamevar2 colexprs2 cond2)
                | tnamevar == tnamevar2 && compatible colexprs colexprs2 && cond == cond2 ->
                    (SQLUpdateStmt tnamevar ( colexprs `union` colexprs2) cond, done)
            (SQLDeleteStmt tnamevar cond, SQLDeleteStmt tnamevar2 cond2)
                | tnamevar == tnamevar2 && cond == cond2 ->
                    (SQLDeleteStmt tnamevar cond, done)
            _ -> (part, active : done)) (p,[]) ps where
            compatible colexpr = all (\(col, expr) -> all (\(col2, expr2) ->col2 /= col || expr2 == expr) colexpr)

toInsert :: [Table] -> SQLCond -> SQLStmt -> SQLStmt
toInsert tablelist cond (SQLInsertStmt tname colexprs tablelist2 cond2) = SQLInsertStmt tname colexprs (tablelist ++ tablelist2) (cond .&&. cond2)
toInsert tablelist cond (SQLDeleteStmt tname cond2) = SQLDeleteStmt tname (cond .&&. cond2)
toInsert tablelist cond (SQLUpdateStmt tname colexprs cond2) = SQLUpdateStmt tname colexprs (cond .&&. cond2)


combineLitsSQL :: [Lit] -> TransMonad [SQLStmt]
combineLitsSQL lits = do
    ts <- get
    combineLits (ptm ts) lits generateUpdateSQL generateInsertSQL generateDeleteSQL

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

generateInsertSQLForPred :: (PredName, [Atom]) -> TransMonad SQLStmt
generateInsertSQLForPred (pred1, [posatom]) = translatePosInsertAtomToSQL posatom -- set property
generateInsertSQLForPred (pred1, _) = error "unsupported number of pos and neg literals" -- set property

generateUpdateSQLForPred :: (PredName, [Atom], [Atom]) -> TransMonad SQLStmt
generateUpdateSQLForPred (pred1, [posatom], _) = translatePosUpdateAtomToSQL posatom -- set property
generateUpdateSQLForPred (pred1, [], [negatom]) = translateNegUpdateAtomToSQL negatom -- set property
generateUpdateSQLForPred (pred1, _, _) = error "unsupported number of pos and neg literals" -- set property

translateDeleteAtomToSQL :: Atom -> TransMonad SQLStmt
translateDeleteAtomToSQL (Atom pred1 args) = do
    ts <- get
    case lookup pred1 (predtablemap ts) of
        Just (OneTable tname sqlvar, qcols) ->
            case lookup pred1 (ptm ts) of
                Nothing ->
                    error ("translateDeleteAtomToSQL: cannot find predicate " ++ show pred1)
                Just pt -> do
                    (qcol_args, sqlvar2) <- preproc0 tname sqlvar qcols pt args
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
        Just (OneTable tname sqlvar, qcols) ->
            case lookup pred1 (ptm ts) of
                Nothing ->
                    error ("translatePosUpdateAtomToSQL: cannot find predicate " ++ show pred1)
                Just pt -> do
                    (keyqcol_args, propqcol_args, sqlvar2) <- preproc tname sqlvar qcols pt args
                    cond <- foldl (.&&.) SQLTrueCond <$> mapM qcolArgToUpdateCond keyqcol_args
                    set <- mapM qcolArgToSet propqcol_args
                    return (SQLUpdateStmt (tname, sqlvar2) set cond)
        Nothing -> error "not an updatable predicate"


translateNegUpdateAtomToSQL :: Atom -> TransMonad SQLStmt
translateNegUpdateAtomToSQL (Atom pred1 args) = do
    ts <- get
    case lookup pred1 (predtablemap ts) of
        Just (OneTable tname sqlvar, qcols) ->
            case lookup pred1 (ptm ts) of
                Nothing ->
                    error ("translatePosUpdateAtomToSQL: cannot find predicate " ++ show pred1)
                Just pt -> do
                    (keyqcol_args, propqcol_args, sqlvar2) <- preproc tname sqlvar qcols pt args
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


data SQLTrans = SQLTrans  BuiltIn PredTableMap (Maybe Pred) PredTypeMap

data SQLState = SQLState {
    queryKeys:: [(String, [Expr])],
    updateKey:: Maybe (String, [Expr]),
    ksDeleteProp :: [PredName],
    ksDeleteObj :: Bool,
    ksInsertProp :: [PredName],
    ksInsertObj :: Bool,
    ksQuery :: Bool,
    env :: Set Var,
    deleteConditional :: Bool
}

pureOrExecF :: SQLTrans -> Set Var -> FormulaT -> StateT SQLState Maybe ()
pureOrExecF  (SQLTrans  (BuiltIn builtin) predtablemap nextid ptm) dvars (FAtomic2 _ (Atom n@(PredName _ pn) args)) = do
    ks <- get
    if Just n == (predName <$> nextid)
        then lift Nothing
        else if isJust (updateKey ks)
            then lift Nothing
            else if n `member` builtin
                then return ()
                else case lookup n predtablemap of
                    Nothing ->
                        trace ("pureOrExecF: cannot find table for predicate " ++ show n ++ " ignored, the predicate nextid is " ++ show nextid) $ return ()
                    Just (OneTable tablename _, _) ->
                        case lookup n ptm of
                            Nothing ->
                                error ("pureOrExecF: cannot find predicate " ++ show n ++ " available predicates: " ++ show ptm)
                            Just pt -> do
                                let key = keyComponents pt args
                                put ks{queryKeys = queryKeys ks `union` [(tablename, key)]}

pureOrExecF  trans@(SQLTrans _ _ _ ptm) dvars form@(FSequencing2 _ form1 form2) = do
    pureOrExecF  trans dvars form1
    let dvars2 = determinedVars (toDSP ptm) dvars form1
    pureOrExecF  trans dvars2 form2

pureOrExecF  trans dvars form@(FPar2 _ form1 form2) =
    -- only works if all vars are determined
    if freeVars form1 `Set.isSubsetOf` dvars && freeVars form2 `Set.isSubsetOf` dvars
        then do
            ks <- get
            if isJust (updateKey ks)
                then lift Nothing
                else do
                    trace ("#############b" ++ serialize form) $ put ks {ksQuery = True}
                    pureOrExecF  trans dvars form1
                    pureOrExecF  trans dvars form2
        else
            lift Nothing
pureOrExecF  trans dvars form@(FChoice2 _ form1 form2) =
    -- only works if all vars are determined
    if freeVars form1 `Set.isSubsetOf` dvars && freeVars form2 `Set.isSubsetOf` dvars
        then do
            ks <- get
            if isJust (updateKey ks)
                then lift Nothing
                else do
                    trace ("#############a" ++ serialize form) $ put ks {ksQuery = True}
                    pureOrExecF  trans dvars form1
                    pureOrExecF  trans dvars form2
        else
            lift Nothing
pureOrExecF  (SQLTrans  builtin predtablemap _ ptm) _ form@(FInsert2 _ (Lit sign0 (Atom pred0 args))) = do
            ks <- get
            trace ("#############1" ++ show (ksQuery ks) ++ "," ++ serialize form) $ if ksQuery ks || deleteConditional ks
                then lift Nothing
                else
                  trace "#############2" $ case lookup pred0 ptm of
                    Nothing -> error ("pureOrExecF: cannot find predicate " ++ show pred0 ++ " available predicates: " ++ show ptm)
                    Just pt -> do
                      let key = keyComponents pt args
                          tablename = case lookup pred0 predtablemap of
                              Just (OneTable tn _, _) -> tn
                              Nothing -> error ("pureOrExecF: cannot find table for predicate " ++ show pred0)
                      let isObject = isObjectPred ptm pred0
                      let isDelete = case sign0 of
                              Pos -> False
                              Neg -> True
                      ks' <- case updateKey ks of
                          Nothing ->
                              trace "#############3" $ if isObject
                                  then if isDelete
                                      then if not (superset [(tablename, key)] (queryKeys ks))
                                          then lift Nothing
                                          else return ks{updateKey = Just (tablename, key), ksDeleteObj = True}
                                      else if not (superset [(tablename, key)] (queryKeys ks))
                                          then lift Nothing
                                          else return ks{updateKey = Just (tablename, key), ksInsertObj = True}
                                  else if isDelete
                                      then if not (superset [(tablename, key)] (queryKeys ks))
                                          then lift Nothing
                                          else return ks{updateKey = Just (tablename, key), ksDeleteProp = [pred0]}
                                      else if not (superset [(tablename, key)] (queryKeys ks))
                                          then lift Nothing
                                          else return ks{updateKey = Just (tablename, key), ksInsertProp = [pred0]}
                          Just key' ->
                              trace "#############4" $ if isObject
                                  then trace "#############6" $ if isDelete
                                      then if not (null (ksInsertProp ks)) || ksInsertObj ks || ksDeleteObj ks || (tablename, key) /= key' || not (superset [(tablename, key)] (queryKeys ks))
                                          then lift Nothing
                                          else return ks{ksDeleteObj = True}
                                      else lift Nothing
                                  else trace "#############5" $ if isDelete
                                      then if not (null (ksInsertProp ks)) || ksInsertObj ks || ksDeleteObj ks || pred0 `elem` (ksDeleteProp ks) || (tablename, key) /= key' || not (superset [(tablename, key)] (queryKeys ks))
                                          then lift Nothing
                                          else return ks{ksDeleteProp = ksDeleteProp ks ++ [pred0]}
                                      else trace "#############" $ if not (null (ksDeleteProp ks)) || ksDeleteObj ks || pred0 `elem` (ksInsertProp ks) || (tablename, key) /= key'
                                          then lift Nothing
                                          else return ks{ksInsertProp = ksInsertProp ks ++ [pred0]}
                      let isDeleteConditional = isDelete && not (all isVar (propComponents pt args)) -- || (not isDelete && not (all isVar (keyComponents pt args)))
                      put ks'{deleteConditional = isDeleteConditional}

pureOrExecF  _ _ (FOne2 _) = return ()
pureOrExecF  _ _ (FZero2 _) = return ()
pureOrExecF trans dvars for@(Aggregate2 _ Not form) = do
    ks <- get
    if isJust (updateKey ks)
        then lift Nothing
        else do
            trace ("#############d" ++ serialize for) $ put ks {ksQuery = True}
            pureOrExecF trans dvars form
pureOrExecF trans dvars for@(Aggregate2 _ Exists form) = do
  ks <- get
  if isJust (updateKey ks)
      then lift Nothing
      else do
          trace ("#############c" ++ serialize for) $ put ks {ksQuery = True}
          pureOrExecF trans dvars form
pureOrExecF _ _ (Aggregate2 _ _ _) =
  lift Nothing

sequenceF :: SQLTrans -> FormulaT -> StateT SQLState Maybe ()
sequenceF (SQLTrans _ _ (Just nextid1) _) (FAtomic2 _ (Atom p [_])) | predName nextid1 == p =
                        return ()
sequenceF _ _ = lift Nothing

limitF :: SQLTrans -> Set Var -> FormulaT -> StateT SQLState Maybe ()
limitF trans dvars (Aggregate2 _ (Limit _) form) = do
  ks <- get
  trace ("#############" ++ serialize form) $ put ks {ksQuery = True}
  limitF trans dvars form
limitF trans dvars form = orderByF trans dvars form

orderByF :: SQLTrans -> Set Var -> FormulaT -> StateT SQLState Maybe ()
orderByF trans dvars (Aggregate2 _ (OrderByAsc _) form) = do
  ks <- get
  trace ("#############ord" ++ serialize form) $ put ks {ksQuery = True}
  orderByF trans dvars form
orderByF trans dvars form = summarizeF trans dvars form

summarizeF :: SQLTrans -> Set Var -> FormulaT -> StateT SQLState Maybe ()
summarizeF trans dvars (Aggregate2 _ (Summarize _ _) form) = do
  ks <- get
  trace ("#############sum" ++ serialize form) $ put ks {ksQuery = True}
  pureOrExecF trans dvars form
summarizeF trans dvars form = pureOrExecF trans dvars form



instance IGenericDatabase01 SQLTrans where
    type GDBQueryType SQLTrans = (Bool, [Var], [CastType], String, [Var])
    type GDBFormulaType SQLTrans = FormulaT
    gTranslateQuery trans ret query@(Annotated vtm _) env =
        let (SQLTrans builtin predtablemap nextid ptm) = trans
            env2 = foldl (\map2 key@(Var w)  -> insert key (SQLParamExpr w) map2) empty env
            (sql@(retvars, sqlquery, _), ts') = runNew (runStateT (translateQueryToSQL (toAscList ret) (stripAnnotations query)) (TransState {builtin = builtin, predtablemap = predtablemap, repmap = env2, tablemap = empty, nextid = nextid, ptm = ptm}))
            retvartypes = map (\var0 -> case lookup var0 vtm of
                                                Nothing -> error "var type not found"
                                                Just (ParamType _ _ _ p) -> p) retvars in
            trace ("gTranslateQuery of SQLTrans: " ++ serialize sqlquery) $ return (case sqlquery of
                SQLQueryStmt _ -> True
                _ -> False, retvars, retvartypes, serialize sqlquery, params sql)

    gSupported trans ret form env =
        let
            initstate = SQLState [] Nothing [] False [] False (not (null ret)) env False
        in
            layeredF form && (isJust (evalStateT (limitF  trans env form) initstate )
                || isJust (evalStateT (sequenceF trans form) initstate))
