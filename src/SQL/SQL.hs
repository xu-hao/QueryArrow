{-# LANGUAGE TypeSynonymInstances, FlexibleInstances, MultiParamTypeClasses, FlexibleContexts, RankNTypes, GADTs, UndecidableInstances #-}
module SQL.SQL where

import FO
import DBQuery

import Prelude hiding (lookup)
import Data.List (intercalate, (\\),union)
import Control.Monad.Trans.State.Strict (State, get, put, evalState, runState)
import Control.Monad (foldM)
import Data.Map.Strict (empty, Map, insert, (!), member, singleton, adjust, foldlWithKey, lookup, fromList, unionWith, keys, alter, toList)
import Data.Monoid
import Control.Applicative ((<$>))

type Col = String
type TableName = String
data Table = OneTable TableName SQLVar
           | JoinTables TableName SQLVar [(TableName, SQLVar, SQLCond)] deriving (Eq, Ord)

type SQLTableList = [Table]

-- merge two lists of sql tables
-- only support onetable
mergeTables :: SQLTableList -> SQLTableList -> SQLTableList
mergeTables = union

type SQLVar = String

type SQLQualifiedCol = (SQLVar, Col)

type SQLOper = String

data SQLExpr = SQLColNameExpr Col
             | SQLColExpr SQLQualifiedCol
             | SQLIntConstExpr Int
             | SQLStringConstExpr String
             | SQLPatternExpr String
             | SQLNullExpr
             | SQLParamExpr deriving (Eq, Ord)

isSQLConstExpr :: SQLExpr -> Bool
isSQLConstExpr (SQLIntConstExpr _ ) = True
isSQLConstExpr (SQLStringConstExpr _ ) = True
isSQLConstExpr _ = False

data SQLCond = SQLCompCond SQLOper SQLExpr SQLExpr
             | SQLAndCond [SQLCond]
             | SQLOrCond [SQLCond]
             | SQLExistsCond SQL
             | SQLNotExistsCond SQL
             | SQLNotCond SQLCond deriving (Eq, Ord)

getSQLConjuncts :: SQLCond -> [SQLCond]
getSQLConjuncts (SQLAndCond conjs) = conjs
getSQLConjuncts cond = [cond]

getSQLDisjuncts :: SQLCond -> [SQLCond]
getSQLDisjuncts (SQLOrCond conjs) = conjs
getSQLDisjuncts cond = [cond]

(.&&.) :: SQLCond -> SQLCond -> SQLCond
a .&&. b =
    let conjs = getSQLConjuncts a `union` getSQLConjuncts b in
        case conjs of
            [cond] -> cond
            _ -> SQLAndCond conjs

(.||.) :: SQLCond -> SQLCond -> SQLCond
a .||. b =
    let conjs = getSQLDisjuncts a `union` getSQLDisjuncts b in
        case conjs of
            [cond] -> cond
            _ -> SQLOrCond conjs

strue = SQLAndCond []
sfalse = SQLOrCond []

(.=.) :: SQLExpr -> SQLExpr -> SQLCond
a .=. b = SQLCompCond "=" a b

(.<>.) :: SQLExpr -> SQLExpr -> SQLCond
a .<>. b = SQLCompCond "<>" a b


data SQL = SQL {sqlSelect :: [ SQLQualifiedCol ], sqlFrom :: SQLTableList, sqlWhere :: SQLCond} deriving (Eq, Ord)

class ConvertQualified a where
    convertQualified :: a -> a

instance ConvertQualified SQLCond where
    convertQualified (SQLCompCond op e1 e2) = SQLCompCond op (convertQualified e1) (convertQualified e2)
    convertQualified (SQLAndCond c1) = SQLAndCond (map convertQualified c1)
    convertQualified (SQLOrCond c1) = SQLOrCond (map convertQualified c1)
    convertQualified (SQLExistsCond sql) = SQLExistsCond (convertQualified sql)
    convertQualified (SQLNotExistsCond sql) = SQLNotExistsCond (convertQualified sql)
    convertQualified (SQLNotCond cond) = SQLNotCond (convertQualified cond)

instance ConvertQualified SQLExpr where
    convertQualified (SQLColExpr (var, col)) = SQLColNameExpr col
    convertQualified a = a

instance ConvertQualified SQL where
    convertQualified _ = error "unsupported"

instance Show SQL where
    show (SQL cols tables conds) = "SELECT " ++ intercalate "," (map (\(var,col)->var ++"."++col) cols) ++ " FROM " ++
            intercalate "," (map show tables) ++
            (case conds of
                SQLAndCond [] -> ""
                _ -> " WHERE " ++ show conds)

instance Show Table where
    show (OneTable tablename var) = tablename ++ " " ++ var
    show (JoinTables tablename var tables) =
        tablename ++ " " ++ var ++ showOtherTables tables where
            showOtherTables [] = ""
            showOtherTables ((othertablename, othervar, cond1):tables2) = " JOIN " ++ othertablename ++ " " ++ othervar ++ " ON " ++ show cond1 ++ showOtherTables tables2

instance Show SQLCond where
    show (SQLCompCond op lhs rhs) = show lhs ++ " " ++ op ++ " " ++ show rhs
    show (SQLAndCond as) = "(" ++ intercalate " AND " (map show as) ++ ")"
    show (SQLOrCond as) = "(" ++ intercalate " OR " (map show as) ++ ")"
    show (SQLExistsCond sql) = "(EXISTS (" ++ show sql ++ "))"
    show (SQLNotExistsCond sql) = "(NOT EXISTS (" ++ show sql ++ "))"

instance Show SQLExpr where
    show (SQLColNameExpr col) = col
    show (SQLColExpr (var, col)) = var ++ "." ++ col
    show (SQLIntConstExpr i) = show i
    show (SQLStringConstExpr s) = "'" ++ sqlStringEscape s ++ "'"
    show (SQLPatternExpr s) = "'" ++ sqlPatternEscape s ++ "'"
    show SQLParamExpr = "?"
    show SQLNullExpr = "NULL"

showWhereCond :: SQLCond -> String
showWhereCond cond = case cond of
    SQLAndCond [] -> ""
    _ -> " WHERE " ++ show cond

instance Show SQLInsert where
    show (SQLInsert tname colsexprs tables cond) =
        let (cols, exprs) = unzip colsexprs in
            "INSERT INTO " ++ tname ++ " (" ++ intercalate "," cols ++ ")" ++
                if all isSQLConstExpr exprs
                    then " VALUES ("++ intercalate "," (map show exprs)++ ")"
                    else " SELECT " ++ intercalate "," (map show exprs) ++ " FROM " ++ intercalate "," (map show tables) ++ showWhereCond cond


    show (SQLDelete tname cond) =
        let unqcond = convertQualified cond in
            "DELETE FROM " ++ tname ++ showWhereCond unqcond

    show (SQLUpdate tname colsexprs cond) =
        let unqcond = convertQualified cond in
            "UPDATE " ++ tname ++ " SET " ++ intercalate "," (map (\(col, expr)-> col ++ " = " ++ show expr) colsexprs) ++ showWhereCond unqcond

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
    subst varmap (JoinTables tablename var tables) = JoinTables tablename (subst varmap var) (substs varmap tables) where
        substs varmap2 = map (\(tablename2, var2, cond2) -> (tablename2, subst varmap2 var2, subst varmap2 cond2))

instance SQLFreeVars Table where
    fv (OneTable tablename var) = [(tablename, var)]
    fv (JoinTables tablename var tables) = (tablename, var) : vars tables where
        vars = map (\(tablename2, var2, _)-> (tablename2, var2))

instance Subst SQLCond where
    subst varmap (SQLCompCond op a b) = SQLCompCond op (subst varmap a) (subst varmap b)
    subst varmap (SQLAndCond as) = SQLAndCond (map (subst varmap) as)
    subst _ _ = error "unsupported SQLCond"

instance Subst SQLExpr where
    subst varmap (SQLColExpr qcol) = SQLColExpr (subst varmap qcol)
    subst _ a = a

instance Subst SQLQualifiedCol where
    subst varmap (var, col) = (subst varmap var, col)

type SQLQuery = ([Var], SQL)

instance Monoid SQL where
    (SQL sselect1 sfrom1 swhere1) `mappend` (SQL sselect2 sfrom2 swhere2) =
        SQL (sselect1 ++ sselect2) (sfrom1 `mergeTables` sfrom2) (swhere1 .&&. swhere2)
    mempty = SQL [] [] strue

sor (SQL sselect1 sfrom1 swhere1) (SQL sselect2 sfrom2 swhere2) = SQL (sselect1 ++ sselect2) (sfrom1 `mergeTables` sfrom2) (swhere1 .||. swhere2)
false = SQL [] [] sfalse

negCond :: SQLCond -> SQLCond
negCond cond = SQLNotCond cond

snot :: SQL -> SQL
snot (SQL sselect sfrom swhere) = SQL sselect sfrom (negCond swhere)

swhere swhere1 = SQL [] [] swhere1
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
newtype BuiltIn = BuiltIn (Map String (Sign -> [SQLExpr] -> TransMonad SQL))
-- int is for generating fresh var
type TableVarMap = Map TableName Int
type TransMonad a = State (Schema, BuiltIn, PredTableMap, RepMap, TableMap, TableVarMap, [Var]) a

freshSQLVar :: TableName -> TransMonad SQLVar
freshSQLVar tablename = do
    (schema, builtin, predtablemap, repmap, pkmap, tvmap, params) <- get
    let (tvmapnew, vid) = case lookup tablename tvmap of
            Just n -> (adjust (+1) tablename tvmap, show n)
            Nothing -> (insert tablename 0 tvmap, "")
    put (schema, builtin, predtablemap, repmap, pkmap, tvmapnew, params)
    return (tablename ++ vid)

sqlExprFromArg :: Expr -> TransMonad (Either SQLExpr Var)
sqlExprFromArg arg = do
    (schema, builtin, predtablemap, repmap, pkmap, vid, params) <- get
    case arg of
        VarExpr var2 ->
            if var2 `member` repmap
                then
                    let expr = repmap ! var2 in
                        case expr of
                            SQLParamExpr -> do
                                put (schema, builtin, predtablemap, repmap, pkmap, vid, params ++ [var2])
                                return (Left expr)
                            _ ->
                                return (Left expr)
                else return (Right var2)
        IntExpr i ->
            return (Left (SQLIntConstExpr i))
        StringExpr s ->
            return (Left (SQLStringConstExpr s))
        PatternExpr s ->
            return (Left (SQLPatternExpr s))

addVarRep :: Var -> SQLExpr -> TransMonad ()
addVarRep var expr = do
    (schema, builtin, a, repmap, c, vid, params) <- get
    let repmapnew = insert var expr repmap
    put (schema, builtin, a, repmapnew, c, vid, params)

condFromArg :: (SQLExpr -> SQLExpr -> SQLCond) -> (Expr, SQLQualifiedCol) -> TransMonad SQLCond
condFromArg op (arg, col) = do
    v <- sqlExprFromArg arg
    case v of
        Left expr -> return (op (SQLColExpr col) expr)
        Right var2 -> do
            addVarRep var2 (SQLColExpr col)
            return strue

update :: (a -> a) -> State a ()
update f = do
    a <- get
    put (f a)

-- add a sql representing the row identified by the keys
addTable :: TableName -> [Expr] -> SQLVar -> TransMonad ()
addTable tablename prikeyargs sqlvar2 =
    update (\(schema, builtin, predtablemap, repmap, pkmap, vid, params) -> (schema, builtin, predtablemap, repmap, insert (tablename, prikeyargs) sqlvar2 pkmap, vid, params))

translateQueryToSQL :: Query -> TransMonad SQLQuery
translateQueryToSQL (Query vars formula) = do
    (SQL _ tablelist cond1) <- translateFormulaToSQL formula
    (_, _, _, repmap, _, _, _) <- get
    let extractCol var = case repmap ! var of
                            SQLColExpr col -> col
                            _ -> error (var ++ " doesn't correspond to a column")
    let cols = map extractCol vars
    return (vars, SQL cols tablelist cond1)

translateFormulaToSQL :: Formula -> TransMonad SQL
translateFormulaToSQL (Conjunction formulas) =
    mconcat <$> mapM translateFormulaToSQL formulas

translateFormulaToSQL (Disjunction formulas) =
    foldM (\sql1 disj -> do
        sql2 <- translateFormulaToSQL disj
        return (sql1 `sor` sql2)) false formulas

translateFormulaToSQL (Atomic a) = translateAtomToSQL Pos a

-- assume that all atoms in conj involves var
-- should use subquery in from clause
translateFormulaToSQL (Exists var conj) = do
    (_, sql) <- translateQueryToSQL (Query [var] conj)
    return (snot sql)

translateFormulaToSQL (Not (Atomic a)) =
    translateAtomToSQL Neg a

-- assume that all atoms in conj involves var
translateFormulaToSQL (Not (Exists var conj)) = do
    (_, sql) <- translateQueryToSQL (Query [var] conj)
    return (snot sql)

translateForumlaToSQL (Not _) = error "not not pushed"

translateAtomToSQL :: Sign -> Atom -> TransMonad SQL
translateAtomToSQL thesign (Atom (Pred name _) args) = do
    (schema, BuiltIn builtin, predtablemap, repmap, pkmap, vid, params) <- get
    let err m = do
        a <- m
        case a of
            Left expr -> return expr
            Right _ -> error "unconstrained argument to built-in predicate"
    --try builtin first
    case lookup name builtin of
        Just builtin -> do
            sqlExprs <- mapM (err . sqlExprFromArg) args
            builtin thesign sqlExprs
        Nothing -> case lookup name predtablemap of
            Just (table, cols) -> do
                let getnewvars table2 = do
                    let fvs = fv table2
                    varmap <- foldM (\ varmap (tablename, fv2) -> do
                        sqlvar <- freshSQLVar tablename
                        return (insert fv2 sqlvar varmap)) empty fvs
                    return ([table2], varmap, cols, args)

                (tables, varmap, cols2, args2) <- case table of
                    OneTable tablename sqlvar -> do
                        let (_, prikeycols) = schema ! tablename
                        let (prikeyargs, prikeyargcols) = unzip [(arg, (sqlvar2, col)) | prikeycol <- prikeycols, (arg, (sqlvar2, col)) <- zip args cols, prikeycol == col ]
                        if length prikeyargs == length prikeycols -- if primary key columns correspond to args
                            then if (tablename, prikeyargs) `member` pkmap -- check if there already is a table with same primary key
                                then do
                                    let cols2 = cols \\ prikeyargcols
                                    let args2 = args \\ prikeyargs
                                    return ([], singleton sqlvar (pkmap ! (tablename, prikeyargs)), cols2 , args2)
                                else do
                                    sqlvar2 <- freshSQLVar tablename
                                    addTable tablename prikeyargs sqlvar2
                                    return ([table], singleton sqlvar sqlvar2, cols, args)
                            else getnewvars table
                    _ -> getnewvars table

                let tables2 = map (subst varmap) tables
                let cols3 = map (subst varmap) cols2
                condsFromArgs <- mapM (condFromArg (.=.)) (zip args2 cols3)
                let cond3 = foldl (.&&.) strue condsFromArgs
                return (SQL [] tables2 cond3)
            Nothing -> error (name ++ " is not defined")


data SQLInsert = SQLInsert TableName [(Col,SQLExpr)] [Table] SQLCond
               | SQLUpdate TableName [(Col,SQLExpr)] SQLCond
               | SQLDelete TableName SQLCond

type SQLInserts = [SQLInsert]

translateInsertToSQL :: Insert -> TransMonad SQLInserts
translateInsertToSQL (Insert lits conj) = do
    (SQL _ tablelist cond) <- translateFormulaToSQL conj
    insertparts <- sortParts <$> combineLits lits
    return (map (toInsert tablelist cond) insertparts)

sortParts :: [SQLInsert] -> [SQLInsert]
sortParts [] = []
sortParts (p : ps) = b++[a] where
    (a, b) = foldl (\(active, done) part ->
        case (active, part) of
            (SQLInsert tname colexprs tablelist cond, SQLInsert tname2 colexprs2 tablelist2 cond2)
                | tname == tname2 && compatible colexprs colexprs2 ->
                    (SQLInsert tname ( colexprs `union` colexprs2) (tablelist ++ tablelist2) (cond .&&. cond2), done)
            _ -> (part, active : done)) (p,[]) ps where
            compatible colexpr = allTrue (\(col, expr) -> allTrue (\(col2, expr2) ->col2 /= col || expr2 == expr) colexpr)

toInsert :: [Table] -> SQLCond -> SQLInsert -> SQLInsert
toInsert tablelist cond (SQLInsert tname colexprs tablelist2 cond2) = SQLInsert tname colexprs (tablelist ++ tablelist2) (cond .&&. cond2)
toInsert tablelist cond (SQLDelete tname cond2) = SQLDelete tname (cond .&&. cond2)
toInsert tablelist cond (SQLUpdate tname colexprs cond2) = SQLUpdate tname colexprs (cond .&&. cond2)


keyComponents :: PredType -> [a] -> [a]
keyComponents (PredType _ paramtypes) = map snd . filter (\(type1, arg) -> case type1 of
    Key _ -> True
    _ -> False) . zip paramtypes

propComponents :: PredType -> [a] -> [a]
propComponents (PredType _ paramtypes) = map snd . filter (\(type1, arg) -> case type1 of
    Property _ -> True
    _ -> False) . zip paramtypes


sortByKey :: [Lit] -> Map [Expr] [Lit]
sortByKey = foldl insertByKey empty where
    insertByKey map1 lit@(Lit _ (Atom (Pred _ predtype) args)) =
        let keyargs = keyComponents predtype args in
            alter (\l -> case l of
                Nothing -> Just [lit]
                Just lits -> Just (lits ++ [lit])) keyargs map1

isObjectPredAtom :: Atom -> Bool
isObjectPredAtom (Atom (Pred _ (PredType ObjectPred _)) _) = True
isObjectPredAtom _ = False

isObjectPredLit :: Lit -> Bool
isObjectPredLit (Lit _ a) = isObjectPredAtom a

sortAtomByPred :: [Atom] -> Map String [Atom]
sortAtomByPred = foldl insertAtomByPred empty where
    insertAtomByPred map1 atom@(Atom (Pred name _) _) =
        alter (\asmaybe -> case asmaybe of
            Nothing -> Just [atom]
            Just as -> Just (as ++ [atom])) name map1

combineLits :: [Lit] -> TransMonad SQLInserts
combineLits lits = do
    let objpredlits = filter isObjectPredLit lits
    let proppredlits = lits \\ objpredlits
    let (posobjpredatoms, negobjpredatoms) = splitPosNegLits objpredlits
    let (pospropredatoms, negproppredatoms) = splitPosNegLits proppredlits
    case (posobjpredatoms, negobjpredatoms) of
        ([], []) ->  generateUpdateSQL pospropredatoms negproppredatoms   -- update property
        ([posobjatom], []) ->  case negproppredatoms of
            [] -> generateInsertSQL (posobjatom:pospropredatoms)      -- insert
            _ -> error "trying to delete properties of an object to be created"
        ([], [negobjatom]) ->   case (pospropredatoms, negproppredatoms) of
            ([], []) -> generateDeleteSQL negobjatom
            _ -> error "tyring to modify propertiese of an object to be deleted" -- delete
        ([posobjatom], [negobjatom]) -> generateUpdateSQL (posobjatom:pospropredatoms) (negobjatom:negproppredatoms) -- update property

generateDeleteSQL :: Atom -> TransMonad SQLInserts
generateDeleteSQL atom = do
    sql <- translateDeleteAtomToSQL atom
    return [sql]

generateInsertSQL :: [Atom] -> TransMonad SQLInserts
generateInsertSQL atoms = do
    let map1 = sortAtomByPred atoms
    mapM generateInsertSQLForPred (toList map1)

generateUpdateSQL :: [Atom] -> [Atom] -> TransMonad SQLInserts
generateUpdateSQL pospropatoms negpropatoms = do
    let posprednamemap = sortAtomByPred pospropatoms
    let negprednamemap = sortAtomByPred negpropatoms
    let allkeys = keys posprednamemap `union` keys negprednamemap
    let poslist = [l | key <- allkeys, let l = case lookup key posprednamemap of Nothing -> []; Just l -> l]
    let neglist = [l | key <- allkeys, let l = case lookup key negprednamemap of Nothing -> []; Just l -> l]
    mapM generateUpdateSQLForPred (zip3 allkeys poslist neglist)

generateInsertSQLForPred :: (String, [Atom]) -> TransMonad SQLInsert
generateInsertSQLForPred (pred, [posatom]) = translatePosInsertAtomToSQL posatom -- set property
generateInsertSQLForPred (pred, _) = error "unsupported number of pos and neg literals" -- set property

generateUpdateSQLForPred :: (String, [Atom], [Atom]) -> TransMonad SQLInsert
generateUpdateSQLForPred (pred, [posatom], _) = translatePosUpdateAtomToSQL posatom -- set property
generateUpdateSQLForPred (pred, [], [negatom]) = translateNegUpdateAtomToSQL negatom -- set property
generateUpdateSQLForPred (pred, _, _) = error "unsupported number of pos and neg literals" -- set property

translateDeleteAtomToSQL :: Atom -> TransMonad SQLInsert
translateDeleteAtomToSQL (Atom (Pred pred predtype) args) = do
    (_, _, predtablemap, _, _, _, _) <- get
    case lookup pred predtablemap of
        Just (OneTable tname _, qcols) -> do
            let qcol_args = zip qcols args
            cond <- foldl (.&&.) strue <$> mapM qcolArgToDelete qcol_args
            return (SQLDelete tname cond)
        Just _ -> error "joined table not supported for insert"
        Nothing -> error "not an updatable predicate"

translatePosInsertAtomToSQL :: Atom -> TransMonad SQLInsert
translatePosInsertAtomToSQL (Atom (Pred pred predtype) args) = do
    (_, _, predtablemap, _, _, _, _) <- get
    case lookup pred predtablemap of
        Just (OneTable tname _, qcols) -> do
            let qcol_args = zip qcols args
            colexprs <- mapM qcolArgToValue qcol_args
            return (SQLInsert tname colexprs [] strue)
        Just _ -> error "joined table not supported for insert"
        Nothing -> error "not an updatable predicate"


translatePosUpdateAtomToSQL :: Atom -> TransMonad SQLInsert
translatePosUpdateAtomToSQL (Atom (Pred pred predtype) args) = do
    (_, _, predtablemap, _, _, _, _) <- get
    case lookup pred predtablemap of
        Just (OneTable tname _, qcols) -> do
            let qcol_args = zip qcols args
            let keyqcol_args = keyComponents predtype qcol_args
            let propqcol_args = propComponents predtype qcol_args
            cond <- foldl (.&&.) strue <$> mapM qcolArgToUpdateCond keyqcol_args
            set <- mapM qcolArgToSet propqcol_args
            return (SQLUpdate tname set cond)
        Just _ -> error "joined table not supported for insert"
        Nothing -> error "not an updatable predicate"


translateNegUpdateAtomToSQL :: Atom -> TransMonad SQLInsert
translateNegUpdateAtomToSQL (Atom (Pred pred predtype) args) = do
    (_, _, predtablemap, _, _, _, _) <- get
    case lookup pred predtablemap of
        Just (OneTable tname _, qcols) -> do
            let qcol_args = zip qcols args
            let keyqcol_args = keyComponents predtype qcol_args
            let propqcol_args = propComponents predtype qcol_args
            cond <- foldl (.&&.) strue <$> mapM qcolArgToUpdateCond keyqcol_args
            (set, conds) <- unzip <$> mapM qcolArgToSetNull propqcol_args
            return (SQLUpdate tname set (foldl (.&&.) cond conds))
        Just _ -> error "joined table not supported for insert"
        Nothing -> error "not an updatable predicate"

qcolArgToDelete :: (SQLQualifiedCol, Expr) -> TransMonad SQLCond
qcolArgToDelete (qcol, arg) = do
    sqlexpr <- sqlExprFromArg arg
    case sqlexpr of
        Left sqlexpr -> return (SQLColExpr qcol .=. sqlexpr)
        Right var -> do
            addVarRep var (SQLColExpr qcol)
            return strue -- unbounded var

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
        Left sqlexpr -> return strue
        Right var -> do
            addVarRep var (SQLColExpr qcol)
            return strue -- unbounded var

qcolArgToCond :: (SQLQualifiedCol, Expr) -> TransMonad SQLCond
qcolArgToCond (qcol, arg) = do
    sqlexpr <- sqlExprFromArg arg
    case sqlexpr of
        Left sqlexpr -> return (SQLColExpr qcol .=. sqlexpr)
        Right var -> do
            addVarRep var (SQLColExpr qcol)
            return strue -- unbounded var

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
            return ((col, SQLNullExpr), strue)


data SQLTrans = SQLTrans Schema BuiltIn PredTableMap

instance Translate SQLTrans MapResultRow SQLQuery SQLInserts where
    translateQuery trans query =
        let (SQLTrans schema builtin predtablemap) = trans in
            evalState (translateQueryToSQL query) (schema, builtin, predtablemap, empty, empty, empty, [])
    translateQueryWithParams trans query env =
        let (SQLTrans schema builtin predtablemap) = trans
            env2 = foldlWithKey (\map2 key _  -> insert key SQLParamExpr map2) empty env
            (sql, (_,_,_,_,_,_,vars)) = runState (translateQueryToSQL query) (schema, builtin, predtablemap, env2, empty, empty, []) in
            (sql, vars)
    translateInsert trans query =
        let (SQLTrans schema builtin predtablemap) = trans in
            evalState (translateInsertToSQL query) (schema, builtin, predtablemap, empty, empty, empty, [])
    translateInsertWithParams trans query env =
        let (SQLTrans schema builtin predtablemap) = trans
            env2 = foldlWithKey (\map2 key _  -> insert key SQLParamExpr map2) empty env in
            evalState (translateInsertToSQL query) (schema, builtin, predtablemap, env2, empty, empty, [])

instance DBConnection conn SQLQuery SQLInserts => ConnectionDB DBAdapterMonad conn SQLTrans where
    extractDomainSize _ trans varDomainSize thesign (Atom (Pred name _) args) =
        if isBuiltIn
            then return maxArgDomainSize -- assume that builtins don't restrict domain size
            else (case thesign of
                Neg -> return maxArgDomainSize
                Pos -> return (if name `member` predtablemap
                    then fromList [(fv, Bounded 1) | fv <- freeVars args] -- just set bound to 1, we only use Bounded 1 or Unbounded for now
                    else empty)) where
                argsDomainSizeMaps = map (exprDomainSizeMap varDomainSize Unbounded) args
                maxArgDomainSize = mmaxs argsDomainSizeMaps
                isBuiltIn = name `member` builtin
                (SQLTrans _ (BuiltIn builtin) predtablemap) = trans
