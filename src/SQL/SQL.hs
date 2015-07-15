{-# LANGUAGE TypeSynonymInstances, FlexibleInstances, MultiParamTypeClasses, FlexibleContexts, RankNTypes, GADTs #-}
module SQL.SQL where

import FO
import DBQuery

import Prelude hiding (lookup)
import Control.Monad.IO.Class (liftIO)
import Data.List (intercalate, (\\))
import Control.Monad.Trans.State.Strict (State, get, put, evalState, runState)
import Control.Monad (foldM)
import Data.Map.Strict (empty, Map, insert, (!), member, singleton, adjust, foldlWithKey, lookup)

type Col = String
type TableName = String
data Table = OneTable TableName SQLVar
           | JoinTables TableName SQLVar [(TableName, SQLVar, SQLCond)]
           
type SQLVar = String

type SQLQualifiedCol = (SQLVar, Col)
type SQLOper = String

data SQLExpr = SQLColExpr SQLQualifiedCol
             | SQLIntConstExpr Int
             | SQLStringConstExpr String
             | SQLParamExpr
             
data SQLCond = SQLCompCond SQLOper SQLExpr SQLExpr
             | SQLAndCond SQLCond SQLCond
             | SQLOrCond SQLCond SQLCond  
             | SQLTrueCond
             | SQLFalseCond
             | SQLExistsCond SQL
             | SQLNotExistsCond SQL
             
(.&&.) :: SQLCond -> SQLCond -> SQLCond
SQLTrueCond .&&. b = b
SQLFalseCond .&&. _ = SQLFalseCond
a .&&. SQLTrueCond = a
_ .&&. SQLFalseCond = SQLFalseCond
a .&&. b = SQLAndCond a b

(.||.) :: SQLCond -> SQLCond -> SQLCond
SQLTrueCond .||. _ = SQLTrueCond
SQLFalseCond .||. b = b
_ .||. SQLTrueCond = SQLTrueCond
a .||. SQLFalseCond = a
a .||. b = SQLOrCond a b

(.=.) :: SQLExpr -> SQLExpr -> SQLCond
a .=. b = SQLCompCond "=" a b

(.<>.) :: SQLExpr -> SQLExpr -> SQLCond
a .<>. b = SQLCompCond "<>" a b

type SQLTableList = [Table]
data SQL = SQL {sqlSelect :: [ SQLQualifiedCol ], sqlFrom :: SQLTableList, sqlWhere :: SQLCond} 

instance Show SQL where
    show (SQL cols tables conds) = "SELECT " ++ intercalate "," (map (\(var,col)->var ++"."++col) cols) ++ " FROM " ++
        intercalate "," (map show tables) ++
        (case conds of 
            SQLTrueCond -> ""
            _ -> " WHERE " ++ show conds)

instance Show Table where
    show (OneTable tablename var) = tablename ++ " " ++ var
    show (JoinTables tablename var tables) = 
        tablename ++ " " ++ var ++ showOtherTables tables where
            showOtherTables [] = ""
            showOtherTables ((othertablename, othervar, cond1):tables2) = " JOIN " ++ othertablename ++ " " ++ othervar ++ " ON " ++ show cond1 ++ showOtherTables tables2 

instance Show SQLCond where
    show (SQLCompCond op lhs rhs) = show lhs ++ op ++ show rhs
    show (SQLAndCond a b) = "(" ++ show a ++ " AND " ++ show b ++ ")"
    show (SQLOrCond a b) = "(" ++ show a ++ " OR " ++ show b ++ ")"
    show SQLTrueCond = "@TRUE"
    show SQLFalseCond = "@FALSE"
    show (SQLExistsCond sql) = "(EXISTS (" ++ show sql ++ "))"
    show (SQLNotExistsCond sql) = "(NOT EXISTS (" ++ show sql ++ "))" 
        
instance Show SQLExpr where
    show (SQLColExpr (var, col)) = var ++ "." ++ col
    show (SQLIntConstExpr i) = show i
    show (SQLStringConstExpr s) = "'" ++ s ++ "'"
    show (SQLParamExpr) = "?"

class Subst a where
    subst :: Map SQLVar SQLVar -> a -> a
    fv :: a -> [(TableName, SQLVar)]

instance Subst SQLVar where
    subst varmap var = case lookup  var varmap of
        Nothing -> var
        Just var2 -> var2
    fv _ = error "unsupported SQLVar"
        
instance Subst Table where
    subst varmap (OneTable tablename var) = OneTable tablename (subst varmap var)
    subst varmap (JoinTables tablename var tables) = JoinTables tablename (subst varmap var) (substs varmap tables) where
        substs varmap2 = map (\(tablename2, var2, cond2) -> (tablename2, subst varmap2 var2, subst varmap2 cond2))
    fv (OneTable tablename var) = [(tablename, var)]
    fv (JoinTables tablename var tables) = (tablename, var) : vars tables where
        vars = map (\(tablename2, var2, _)-> (tablename2, var2))

instance Subst SQLCond where
    subst varmap (SQLCompCond op a b) = SQLCompCond op (subst varmap a) (subst varmap b)
    subst varmap (SQLAndCond a b) = SQLAndCond (subst varmap a) (subst varmap b)
    subst _ _ = error "unsupported SQLCond"
    fv _ = error "unsupported SQLCond"

instance Subst SQLExpr where
    subst varmap (SQLColExpr qcol) = SQLColExpr (subst varmap qcol)
    subst _ a = a 
    fv _ = error "unsupported SQLExpr"

instance Subst SQLQualifiedCol where
    subst varmap (var, col) = (subst varmap var, col)
    fv _ = error "unsupported SQLQualifiedCol"

type SQLQuery = ([Var], SQL)

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
data BuiltIn = BuiltIn {
    builtInMap :: Sign -> String -> [SQLExpr] -> TransMonad (SQLTableList, SQLCond),
    builtInList :: [String]
    }
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
    
translateQueryToSQL :: Query -> TransMonad SQL
translateQueryToSQL (Query vars conj) = do
    (tablelist, cond1) <- translateConjToSQL conj
    (_, _, _, repmap, _, _, _) <- get 
    let extractCol var = case repmap ! var of 
                            SQLColExpr col -> col
                            _ -> error (var ++ " doesn't correspond to a column")
    let cols = map extractCol vars
    return (SQL cols tablelist cond1)

translateConjToSQL :: [Disjunction] -> TransMonad (SQLTableList, SQLCond)
translateConjToSQL [] = error "empty conjuction"
translateConjToSQL [disj] = translateDisjToSQL disj
translateConjToSQL (disj : disjs) = do
    (tablelist, cond1) <- translateDisjToSQL disj
    (tablelist2, cond2) <- translateConjToSQL disjs
    return (tablelist ++ tablelist2, cond1 .&&. cond2)
    
translateDisjToSQL :: Disjunction -> TransMonad (SQLTableList, SQLCond)
translateDisjToSQL [] = error "empty disjuction"
translateDisjToSQL [lit] = translateLitToSQL lit
translateDisjToSQL (lit : lits) = do
    (tl, cl) <- translateLitToSQL lit
    (tl2, cl2) <- translateDisjToSQL lits
    return (tl ++ tl2, cl .||. cl2)

sqlExprFromArg :: RepMap -> Expr -> TransMonad (Either SQLExpr Var)
sqlExprFromArg repmap arg = do 
    (schema, builtin, predtablemap, repmap2, pkmap, vid, params) <- get
    case arg of     
        VarExpr var2 ->
            if var2 `member` repmap
                then 
                    let expr = repmap ! var2 in
                        case expr of
                            SQLParamExpr -> do
                                put (schema, builtin, predtablemap, repmap2, pkmap, vid, params ++ [var2])
                                return (Left expr)
                            _ ->
                                return (Left expr)
                else return (Right var2)
        IntExpr i ->
            return (Left (SQLIntConstExpr i))
        StringExpr s ->
            return (Left (SQLStringConstExpr s))

update :: (a -> a) -> State a ()
update f = do
    a <- get
    put (f a)
    
translateLitToSQL :: Lit -> TransMonad (SQLTableList, SQLCond)
translateLitToSQL (Lit thesign (Atom (Pred name _) args)) = do
    (schema, builtin, predtablemap, repmap, pkmap, vid, params) <- get
    let err m = do 
        a <- m
        case a of 
            Left expr -> return expr
            Right _ -> error "unconstrained argument to built-in predicate"
    --try builtin first
    if name `elem` builtInList builtin
        then do
            sqlExprs <- mapM (err . sqlExprFromArg repmap) args
            builtInMap builtin thesign name sqlExprs 
        else if name `member` predtablemap then do
            let (table, cols) = predtablemap ! name
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
                                update (\(schema, builtin, predtablemap, repmap, pkmap, vid, params) -> (schema, builtin, predtablemap, repmap, insert (tablename, prikeyargs) sqlvar2 pkmap, vid, params))
                                return ([table], singleton sqlvar sqlvar2, cols, args)
                        else getnewvars table 
                _ -> getnewvars table
                
            let tables2 = map (subst varmap) tables
            let cols3 = map (subst varmap) cols2
            case thesign of
                Pos -> do
                    condsFromArgs <- mapM (condFromArg (.=.)) (zip args2 cols3)
                    return (tables2, foldl1 (.&&.) condsFromArgs)
                Neg -> do
                    condsFromArgs <- mapM (condFromArg (.<>.)) (zip args2 cols3)
                    return (tables2, foldl1 (.||.) condsFromArgs)
            else error (name ++ " is not defined")
             
-- assume that all atoms in conj involves var
-- should use subquery in from clause
translateLitToSQL (Lit Pos (Exists var conj)) = do
    sql <- translateQueryToSQL (Query [var] conj)
    return ([], SQLExistsCond sql)

-- assume that all atoms in conj involves var
translateLitToSQL (Lit Neg (Exists var conj)) = do
    sql <- translateQueryToSQL (Query [var] conj)
    return ([], SQLNotExistsCond sql)

condFromArg :: (SQLExpr -> SQLExpr -> SQLCond) -> (Expr, SQLQualifiedCol) -> TransMonad SQLCond
condFromArg op (arg, col) = do
    (schema, builtin, a, repmap, c, vid, params) <- get
    v <- sqlExprFromArg repmap arg
    case v of
        Left expr -> return (op (SQLColExpr col) expr)  
        Right var2 -> do
            let repmapnew = insert var2 (SQLColExpr col) repmap
            put (schema, builtin, a, repmapnew, c, vid, params)
            return SQLTrueCond
    
data SQLTrans = SQLTrans Schema BuiltIn PredTableMap
translate :: SQLTrans -> Query -> SQL
translate (SQLTrans schema builtin predtablemap) query = 
    evalState (translateQueryToSQL query) (schema, builtin, predtablemap, empty, empty, empty, [])
    
translateWithParams :: SQLTrans -> Query -> MapResultRow -> (SQL, [Var])
translateWithParams (SQLTrans schema builtin predtablemap) query env = 
    let env2 = foldlWithKey (\map2 key _  -> insert key SQLParamExpr map2) empty env
        (sql, (_,_,_,_,_,_,vars)) = runState (translateQueryToSQL query) (schema, builtin, predtablemap, env2, empty, empty, []) in
        (sql, vars)
    
resultValueToSQLExpr :: ResultValue -> SQLExpr
resultValueToSQLExpr resval = 
    case resval of 
        IntValue i -> SQLIntConstExpr i
        StringValue s -> SQLStringConstExpr s


data SQLDBAdapter connInfo conn stmt where
    SQLDBAdapter :: DBConnection conn stmt SQLQuery SQLExpr => {   
        sqlDBConn :: conn,
        sqlDBName :: String,
        sqlDBPreds :: [Pred],
        sqlTrans :: SQLTrans
    } -> SQLDBAdapter connInfo conn stmt

instance (QueryDB connInfo conn stmt ([Var], SQL) SQLExpr) => Database_ (SQLDBAdapter connInfo conn stmt) (DBAdapterMonad stmt SQLExpr) MapResultRow where
    dbStartSession _ = put (DBAdapterState empty)
    dbStopSession _ = closeAllCachedPreparedStatements
    getName = sqlDBName
    getPreds = sqlDBPreds
    domainSize db varDomainSize thesign (Pred name _) args =
        if isBuiltIn 
            then maxArgDomainSize
            else (case thesign of 
                Neg -> maxArgDomainSize
                Pos -> if name `member` predtablemap then Just 1 else Nothing) where
            argsDomainSize = map (exprDomainSize varDomainSize Nothing) args
            maxArgDomainSize = dmaxList argsDomainSize
            isBuiltIn = name `elem` builtInList builtin 
            (SQLTrans _ builtin predtablemap) = sqlTrans db
    doQuery db query@(Query queryvars _) = do
        let sql = translate (sqlTrans db) query
        let conn = sqlDBConn db
        execStatement conn (queryvars, sql)
        
    doFilter db query@(Query queryvars _) stream = do
        row <- stream
        let conn = sqlDBConn db
        let (sql, params) = translateWithParams (sqlTrans db) query row
        let vars = foldlWithKey (\vars2 key _ -> vars2 ++ [key]) [] row
        (DBAdapterState preparedstmtcache) <- liftDBAdapter get
        preparedstmt <- case lookup vars preparedstmtcache  of
            Just preparedstmt -> return preparedstmt
            Nothing -> do
                preparedstmt <- liftIO $ prepareStatement conn (queryvars, sql)
                liftDBAdapter $ put (DBAdapterState (insert vars preparedstmt preparedstmtcache))
                return preparedstmt
        execWithParams preparedstmt (map (\param -> resultValueToSQLExpr (row ! param)) params)
