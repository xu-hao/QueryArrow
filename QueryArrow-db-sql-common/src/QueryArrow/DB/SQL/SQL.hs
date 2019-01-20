{-# LANGUAGE TypeSynonymInstances, FlexibleInstances, MultiParamTypeClasses, FlexibleContexts, RankNTypes, GADTs, PatternSynonyms, TypeFamilies, DeriveGeneric, GeneralizedNewtypeDeriving #-}
module QueryArrow.DB.SQL.SQL where

import QueryArrow.Syntax.Term hiding (Subst, subst)
import QueryArrow.Syntax.Type
import QueryArrow.Semantics.TypeChecker
import QueryArrow.Syntax.Serialize
import QueryArrow.Syntax.Utils
import QueryArrow.DB.GenericDatabase
import QueryArrow.ListUtils
import QueryArrow.Utils
import QueryArrow.Semantics.Domain
import QueryArrow.SQL.SQL

import Prelude hiding (lookup)
import Data.List (intercalate, (\\),union, nub)
import Data.Either (rights, lefts, isRight)
import Control.Monad.Trans.State.Strict (StateT, get, put, evalStateT, runStateT, modify)
import Control.Monad.Trans.Class (lift)
import Data.Map.Strict (empty, Map, insert, member, singleton, lookup, fromList, keys, toList, elems, size)
import Data.Monoid ((<>))
import Data.Maybe
import Control.Monad
import Debug.Trace
import qualified Data.Text as T
import Data.Set (toAscList, Set)
import qualified Data.Set as Set
import Algebra.Lattice
import Algebra.Lattice.Dropped
import Algebra.Lattice.Ordered
import System.Log.Logger
import GHC.Generics
import Data.Yaml
import Control.Comonad.Cofree


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
type PredTableMap = Map PredName (TableName, [Col])
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

subState :: TransMonad a -> TransMonad a
subState a = do
  state <- get
  r <- a
  put state
  return r


sqlExprListFromArg :: Expr -> TransMonad [SQLExpr]
sqlExprListFromArg e = do
    let l = exprListFromExpr e
    l2 <- mapM sqlExprFromArg l
    let l3 = filter isRight l2
    if null l3
        then return (lefts l2)
        else error ("sqlExprListFromArg: unrepresented var(s) in cast expr " ++ show (rights l3))


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
        ListConsExpr a b -> do
            l <- sqlExprListFromArg arg
            return (Left (SQLListExpr l))
        ListNilExpr -> do
            l <- sqlExprListFromArg arg
            return (Left (SQLListExpr l))
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
    sql <- subState (translateFormulaToSQL conj)
    return (sqlexists sql)

translateFormulaToSQL (Aggregate Not form) =
    snot <$> subState (translateFormulaToSQL form)

translateFormulaToSQL (Aggregate (Summarize funcs groupby) conj) = do
    sql@(SQLQuery sel fro whe ord lim dis gro) <- translateFormulaToSQL conj
    funcs' <- mapM (\(Bind v@(Var vn) s) -> do
                r <- case s of
                    Max v2 -> do
                        r <- findRep v2
                        return (v, SQLFuncExpr "coalesce" [SQLFuncExpr "max" [r], SQLIntConstExpr 0])
                    Min v2 -> do
                        r <- findRep v2
                        return (v, SQLFuncExpr "coalesce" [SQLFuncExpr "min" [r], SQLIntConstExpr 0])
                    Sum v2 -> do
                        r <- findRep v2
                        return (v, SQLFuncExpr "coalesce" [SQLFuncExpr "sum" [r], SQLIntConstExpr 0])
                    Average v2 -> do
                        r <- findRep v2
                        return (v, SQLFuncExpr "coalesce" [SQLFuncExpr "average" [r], SQLIntConstExpr 0])
                    Count ->
                        return (v, SQLFuncExpr "count" [SQLExprText "*"])
                    CountDistinct v2 -> do
                        r <- findRep v2
                        return (v, SQLFuncExpr "count" [SQLUnaryOpExpr "distinct" r])
                    Random v2 -> do
                        r <- findRep v2
                        return (v, SQLArrayExpr (SQLFuncExpr "array_agg" [r]) (SQLIntConstExpr 1))
                addVarRep v (SQLVarExpr vn)
                return r) funcs
    groupbyreps <- mapM findRep groupby

    if null sel
        then
            return (sqlsummarize funcs' groupbyreps sql)
        else do
            qv <- freshSQLVar "qu"
            return (sqlsummarize2 funcs' groupbyreps sql qv)


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
            Just (tablename, cols) -> do
                (tables, varmap, cols2, args2) <- 
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
                                        return ([SimpleTable tablename v], v, cols, args)
                                    else do
                                        let cols2 = cols \\ prikeyargcols
                                        let args2 = args \\ prikeyargs
                                        return ([], v, cols2 , args2)

                let cols3 = map (SQLQualifiedCol ( varmap)) cols2
                condsFromArgs <- mapM condFromArg (zip args2 cols3)
                let cond3 = foldl (.&&.) SQLTrueCond condsFromArgs
                return (SQLQuery [] tables cond3 [] top False []
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

toInsert :: [FromTable] -> SQLCond -> SQLStmt -> SQLStmt
toInsert tablelist cond (SQLInsertStmt tname colexprs tablelist2 cond2) = SQLInsertStmt tname colexprs (tablelist ++ tablelist2) (cond .&&. cond2)
toInsert tablelist cond (SQLDeleteStmt tname cond2) = SQLDeleteStmt tname (cond .&&. cond2)
toInsert tablelist cond (SQLUpdateStmt tname colexprs cond2) = SQLUpdateStmt tname colexprs (cond .&&. cond2)


combineLitsSQL :: [Lit] -> TransMonad [SQLStmt]
combineLitsSQL lits = do
    ts <- get
    combineLits (ptm ts) lits generateUpdateSQL generateInsertSQL generateDeleteSQL

preproc0 tname cols pred1 args = do
        let key = keyComponents pred1 args
        (_, sqlvar2) <- lookupTableVar tname key
        let qcol_args = zip (map (SQLQualifiedCol ( sqlvar2)) cols) args
        return (qcol_args, sqlvar2)

preproc tname cols pred1 args = do
        (qcol_args , sqlvar2) <- preproc0 tname cols pred1 args
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
        Just (tname, cols) ->
            case lookup pred1 (ptm ts) of
                Nothing ->
                    error ("translateDeleteAtomToSQL: cannot find predicate " ++ show pred1)
                Just pt -> do
                    (qcol_args, sqlvar2) <- preproc0 tname cols pt args
                    cond <- foldl (.&&.) SQLTrueCond <$> mapM qcolArgToDelete qcol_args
                    return (SQLDeleteStmt (tname, sqlvar2) cond)
        Nothing -> error "not an updatable predicate"

translatePosInsertAtomToSQL :: Atom -> TransMonad SQLStmt
translatePosInsertAtomToSQL (Atom pred1 args) = do
    ts <- get
    case lookup pred1 (predtablemap ts) of
        Just (tname, cols) -> do
            let col_args = zip cols args
            colexprs <- mapM colArgToValue col_args
            return (SQLInsertStmt tname colexprs [] SQLTrueCond)
        Nothing -> error "not an updatable predicate"

translatePosUpdateAtomToSQL :: Atom -> TransMonad SQLStmt
translatePosUpdateAtomToSQL (Atom pred1 args) = do
    ts <- get
    case lookup pred1 (predtablemap ts) of
        Just (tname, cols) ->
            case lookup pred1 (ptm ts) of
                Nothing ->
                    error ("translatePosUpdateAtomToSQL: cannot find predicate " ++ show pred1)
                Just pt -> do
                    (keyqcol_args, propqcol_args, sqlvar2) <- preproc tname cols pt args
                    cond <- foldl (.&&.) SQLTrueCond <$> mapM qcolArgToUpdateCond keyqcol_args
                    set <- mapM qcolArgToSet propqcol_args
                    return (SQLUpdateStmt (tname, sqlvar2) set cond)
        Nothing -> error "not an updatable predicate"


translateNegUpdateAtomToSQL :: Atom -> TransMonad SQLStmt
translateNegUpdateAtomToSQL (Atom pred1 args) = do
    ts <- get
    case lookup pred1 (predtablemap ts) of
        Just (tname, cols) ->
            case lookup pred1 (ptm ts) of
                Nothing ->
                    error ("translatePosUpdateAtomToSQL: cannot find predicate " ++ show pred1)
                Just pt -> do
                    (keyqcol_args, propqcol_args, sqlvar2) <- preproc tname cols pt args
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

colArgToValue :: (Col, Expr) -> TransMonad (Col, SQLExpr)
colArgToValue (col, arg) = do
    sqlexpr <- sqlExprFromArg arg
    case sqlexpr of
        Left sqlexpr -> return (col, sqlexpr)
        Right _ -> do
            ts <- get
            error ("qcolArgToValue: set value to unbounded var" ++ show col ++ " " ++ serialize arg ++ " " ++ show (repmap ts))

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
qcolArgToSet (SQLQualifiedCol ( var) col, arg) = do
    sqlexpr <- sqlExprFromArg arg
    case sqlexpr of
        Left sqlexpr -> return (col, sqlexpr)
        Right _ -> do
            ts <- get
            error ("qcolArgToSet: set value to unbounded var" ++ show (var, col) ++ " " ++ serialize arg ++ " " ++ show (repmap ts))

qcolArgToSetNull :: (SQLQualifiedCol, Expr) -> TransMonad ((Col, SQLExpr), SQLCond)
qcolArgToSetNull (qcol@(SQLQualifiedCol ( var) col), arg) = do
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
pureOrExecF  (SQLTrans  (BuiltIn builtin) predtablemap nextid ptm) dvars (FAtomicA _ (Atom n@(PredName _ pn) args)) = do
    ks <- get
    if Just n == (predName <$> nextid)
        then lift Nothing
        else if isJust (updateKey ks)
            then lift Nothing
            else if n `member` builtin
                then return ()
                else case lookup n predtablemap of
                    Nothing ->
                        -- trace ("pureOrExecF: cannot find table for predicate " ++ show n ++ " ignored, the predicate nextid is " ++ show nextid) $
                        return ()
                    Just (tablename, _) ->
                        case lookup n ptm of
                            Nothing ->
                                error ("pureOrExecF: cannot find predicate " ++ show n ++ " available predicates: " ++ show ptm)
                            Just pt -> do
                                let key = keyComponents pt args
                                put ks{queryKeys = queryKeys ks `union` [(tablename, key)]}

pureOrExecF  trans@(SQLTrans _ _ _ ptm) dvars form@(FSequencingA _ form1 form2) = do
    pureOrExecF  trans dvars form1
    let dvars2 = determinedVars (toDSP ptm) dvars form1
    pureOrExecF  trans dvars2 form2

pureOrExecF  trans dvars form@(FParA _ form1 form2) =
    -- only works if all vars are determined
    if freeVars form1 `Set.isSubsetOf` dvars && freeVars form2 `Set.isSubsetOf` dvars
        then do
            ks <- get
            if isJust (updateKey ks)
                then lift Nothing
                else do
                    put ks {ksQuery = True}
                    pureOrExecF  trans dvars form1
                    pureOrExecF  trans dvars form2
        else
            lift Nothing
pureOrExecF  trans dvars form@(FChoiceA _ form1 form2) =
    -- only works if all vars are determined
    if freeVars form1 `Set.isSubsetOf` dvars && freeVars form2 `Set.isSubsetOf` dvars
        then do
            ks <- get
            if isJust (updateKey ks)
                then lift Nothing
                else do
                    put ks {ksQuery = True}
                    pureOrExecF  trans dvars form1
                    pureOrExecF  trans dvars form2
        else
            lift Nothing
pureOrExecF  (SQLTrans  builtin predtablemap _ ptm) _ form@(FInsertA _ (Lit sign0 (Atom pred0 args))) = do
            ks <- get
            if ksQuery ks || deleteConditional ks
                then lift Nothing
                else
                  case lookup pred0 ptm of
                    Nothing -> error ("pureOrExecF: cannot find predicate " ++ show pred0 ++ " available predicates: " ++ show ptm)
                    Just pt -> do
                      let key = keyComponents pt args
                          tablename = case lookup pred0 predtablemap of
                              Just (tn, _) -> tn
                              Nothing -> error ("pureOrExecF: cannot find table for predicate " ++ show pred0)
                      let isObject = isObjectPred ptm pred0
                      let isDelete = case sign0 of
                              Pos -> False
                              Neg -> True
                      ks' <- case updateKey ks of
                          Nothing ->
                              if isObject
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
                              if isObject
                                  then if isDelete
                                      then if not (null (ksInsertProp ks)) || ksInsertObj ks || ksDeleteObj ks || (tablename, key) /= key' || not (superset [(tablename, key)] (queryKeys ks))
                                          then lift Nothing
                                          else return ks{ksDeleteObj = True}
                                      else lift Nothing
                                  else if isDelete
                                      then if not (null (ksInsertProp ks)) || ksInsertObj ks || ksDeleteObj ks || pred0 `elem` (ksDeleteProp ks) || (tablename, key) /= key' || not (superset [(tablename, key)] (queryKeys ks))
                                          then lift Nothing
                                          else return ks{ksDeleteProp = ksDeleteProp ks ++ [pred0]}
                                      else if not (null (ksDeleteProp ks)) || ksDeleteObj ks || pred0 `elem` (ksInsertProp ks) || (tablename, key) /= key'
                                          then lift Nothing
                                          else return ks{ksInsertProp = ksInsertProp ks ++ [pred0]}
                      let isDeleteConditional = isDelete && not (all isVar (propComponents pt args)) -- prevent haddock to error || (not isDelete && not (all isVar (keyComponents pt args)))
                      put ks'{deleteConditional = isDeleteConditional}

pureOrExecF  _ _ (FOneA _) = return ()
pureOrExecF  _ _ (FZeroA _) = return ()
pureOrExecF trans dvars for@(AggregateA _ Not form) = do
    ks <- get
    if isJust (updateKey ks)
        then lift Nothing
        else do
            put ks {ksQuery = True}
            pureOrExecF trans dvars form
pureOrExecF trans dvars for@(AggregateA _ Exists form) = do
  ks <- get
  if isJust (updateKey ks)
      then lift Nothing
      else do
          put ks {ksQuery = True}
          pureOrExecF trans dvars form
pureOrExecF _ _ (AggregateA _ _ _) =
  lift Nothing

sequenceF :: SQLTrans -> FormulaT -> StateT SQLState Maybe ()
sequenceF (SQLTrans _ _ (Just nextid1) _) (FAtomicA _ (Atom p [_])) | predName nextid1 == p =
                        return ()
sequenceF _ _ = lift Nothing

limitF :: SQLTrans -> Set Var -> FormulaT -> StateT SQLState Maybe ()
limitF trans dvars (AggregateA _ (Limit _) form) = do
  ks <- get
  put ks {ksQuery = True}
  limitF trans dvars form
limitF trans dvars form = orderByF trans dvars form

orderByF :: SQLTrans -> Set Var -> FormulaT -> StateT SQLState Maybe ()
orderByF trans dvars (AggregateA _ (OrderByAsc _) form) = do
  ks <- get
  put ks {ksQuery = True}
  orderByF trans dvars form
orderByF trans dvars (AggregateA _ (OrderByDesc _) form) = do
  ks <- get
  put ks {ksQuery = True}
  orderByF trans dvars form
orderByF trans dvars form = distinctF trans dvars form

distinctF :: SQLTrans -> Set Var -> FormulaT -> StateT SQLState Maybe ()
distinctF trans dvars (AggregateA _ Distinct form) = do
  ks <- get
  put ks {ksQuery = True}
  distinctF trans dvars form
distinctF trans dvars form = summarizeF trans dvars form

summarizeF :: SQLTrans -> Set Var -> FormulaT -> StateT SQLState Maybe ()
summarizeF trans dvars (AggregateA _ (Summarize _ _) form) = do
  ks <- get
  put ks {ksQuery = True}
  summarizeF trans dvars form
summarizeF trans dvars form = pureOrExecF trans dvars form

instance IGenericDatabase01 SQLTrans where
    type GDBQueryType SQLTrans = (Bool, [Var], [CastType], String, [Var])
    type GDBFormulaType SQLTrans = FormulaT
    gTranslateQuery trans ret query@(vtm :< _) env = do
        let (SQLTrans builtin predtablemap nextid ptm) = trans
            env2 = foldl (\map2 key@(Var w)  -> insert key (SQLParamExpr w) map2) empty env
            (sql@(retvars, sqlquery, _), ts') = runNew (runStateT (translateQueryToSQL (toAscList ret) (stripAnnotations query)) (TransState {builtin = builtin, predtablemap = predtablemap, repmap = env2, tablemap = empty, nextid = nextid, ptm = ptm}))
            retvartypes = map (\var0 -> case lookup var0 vtm of
                                                Nothing -> error ("var type not found: " ++ show var0 ++ " from " ++ show query)
                                                Just (ParamType _ _ _ _ p) -> p) retvars
        debugM "SQL" ("gTranslateQuery of SQLTrans: " ++ show env ++ "\n-----------------\n" ++ serialize query ++ "\n---------------->\n" ++ serialize sqlquery ++ "\n----------------")
        return (case sqlquery of
                SQLQueryStmt _ -> True
                _ -> False, retvars, retvartypes, serialize sqlquery, params sql)

    gSupported trans ret form env =
        let
            initstate = SQLState [] Nothing [] False [] False (not (null ret)) env False
        in
            layeredF form && (isJust (evalStateT (limitF  trans env form) initstate )
                || isJust (evalStateT (sequenceF trans form) initstate))
