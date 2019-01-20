{-# LANGUAGE TypeSynonymInstances, FlexibleInstances, MultiParamTypeClasses, FlexibleContexts, RankNTypes, GADTs, PatternSynonyms, TypeFamilies, DeriveGeneric, GeneralizedNewtypeDeriving #-}
module QueryArrow.SQL.SQL where

import QueryArrow.Syntax.Term hiding (Subst, subst)
import QueryArrow.Syntax.Type
import QueryArrow.Semantics.TypeChecker
import QueryArrow.Syntax.Serialize
import QueryArrow.Syntax.Utils
import QueryArrow.DB.GenericDatabase
import QueryArrow.ListUtils
import QueryArrow.Utils
import QueryArrow.Semantics.Domain

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

type Col = String
type TableName = String
data Table = OneTable {tableName::TableName, sqlVar:: SQLVar} deriving (Eq, Ord, Show, Generic)
data FromTable = SimpleTable TableName SQLVar | QueryTable SQL SQLVar deriving (Eq, Ord, Show)

type SQLTableList = [FromTable]

data SQLMapping = SQLMapping {
  sqlMappingPredName :: String,
  sqlMappingTable :: TableName,
  sqlMappingCols :: [Col]} deriving (Eq, Ord, Show, Generic)

instance FromJSON SQLMapping
instance ToJSON SQLMapping

-- merge two lists of sql tables
-- only support onetable
mergeTables :: SQLTableList -> SQLTableList -> SQLTableList
mergeTables = union

newtype SQLVar = SQLVar {unSQLVar :: String} deriving (Eq, Ord, Show, FromJSON, ToJSON)

instance FromJSON Table
instance ToJSON Table

instance FromJSON SQLQualifiedCol
instance ToJSON SQLQualifiedCol
instance FromJSON SQLExpr
instance ToJSON SQLExpr

data SQLQualifiedCol = SQLQualifiedCol {
  tableVar :: SQLVar,
  colName :: Col} deriving (Eq, Ord, Show, Generic)

type SQLOper = String

data SQLExpr = SQLVarExpr String
             | SQLTableAliasExpr SQLVar
             | SQLColExpr SQLQualifiedCol
             | SQLIntConstExpr Integer
             | SQLStringConstExpr T.Text
             | SQLNullExpr
             | SQLParamExpr String
             | SQLExprText String
             | SQLListExpr [SQLExpr]
             | SQLCastExpr SQLExpr String
             | SQLArrayExpr SQLExpr SQLExpr
             | SQLInfixFuncExpr String SQLExpr SQLExpr
             | SQLFuncExpr String [SQLExpr]
             | SQLUnaryOpExpr String SQLExpr deriving (Eq, Ord, Show, Generic)

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
    | SQLInsertStmt TableName [(Col,SQLExpr)] [FromTable] SQLCond
    | SQLUpdateStmt (TableName, SQLVar) [(Col,SQLExpr)] SQLCond
    | SQLDeleteStmt (TableName, SQLVar) SQLCond deriving (Eq, Ord, Show)

instance Serialize FromTable where
    serialize (SimpleTable tablename var) = tablename ++ " " ++ serialize var
    serialize (QueryTable qu var) = "(" ++ serialize qu ++ ") " ++ serialize var

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
    show2 (SQLVarExpr col) sqlvar = col
    show2 (SQLTableAliasExpr col) sqlvar = serialize col
    show2 (SQLColExpr (SQLQualifiedCol var col)) sqlvar = if var `elem` sqlvar
        then col
        else serialize var ++ "." ++ col
    show2 (SQLIntConstExpr i) _ = show i
    show2 (SQLStringConstExpr s) _ = "'" ++ sqlStringEscape (T.unpack s) ++ "'"
    show2 (SQLParamExpr _) _ = "?"
    show2 (SQLCastExpr arg ty) sqlvar =  "cast(" ++ show2 arg sqlvar ++ " as " ++ ty ++ ")"
    show2 (SQLArrayExpr arr inx) sqlvar = "(" ++ show2 arr sqlvar ++ ")[" ++ show2 inx sqlvar ++ "]"
    show2 (SQLInfixFuncExpr fn a b) sqlvar = "(" ++ show2 a sqlvar ++ fn ++ show2 b sqlvar ++ ")"
    show2 (SQLListExpr args) sqlvar = "ARRAY[" ++ intercalate "," (map (\a -> show2 a sqlvar) args) ++ "]"
    show2 (SQLFuncExpr fn args) sqlvar = fn ++ "(" ++ intercalate "," (map (\a -> show2 a sqlvar) args) ++ ")"
    show2 (SQLUnaryOpExpr fn arg) sqlvar = fn ++ " " ++ show2 arg sqlvar
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

instance Serialize SQL where
    serialize sql = show2 sql []
instance Serialize SQLStmt where
    serialize (SQLQueryStmt sql) = serialize sql

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


instance Subst FromTable where
    subst varmap (SimpleTable tablename var) = SimpleTable tablename (subst varmap var)
    subst varmap (QueryTable qu var) = QueryTable (subst varmap qu) (subst varmap var)

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
    subst varmap (SQLQualifiedCol ( var) col) = SQLQualifiedCol ( (subst varmap var)) col

instance Subst a => Subst [a] where
    subst varmap = map (subst varmap)

instance Subst (Var, SQLExpr) where
    subst varmap (a,b) = (a, subst varmap b)

instance Subst (SQLExpr, SQLOrder) where
    subst varmap (a,b) = (subst varmap a, b)

instance Subst SQL where
    subst varmap (SQLQuery sel fro whe orderby limit distinct groupby) = SQLQuery (subst varmap sel) (subst varmap fro) (subst varmap whe) (subst varmap orderby) limit distinct (subst varmap groupby)

type SQLQuery0 = ([Var], SQLStmt) -- return vars, sql
type SQLQuery = ([Var], SQLStmt, [Var]) -- return vars, sql, param vars

instance Semigroup SQL where
    (SQLQuery sselect1 sfrom1 swhere1 [] Top False []) <> (SQLQuery sselect2 sfrom2 swhere2 [] Top False []) =
        SQLQuery (sselect1 ++ sselect2) (sfrom1 `mergeTables` sfrom2) (swhere1 .&&. swhere2) [] Top False []
    _ <> _ =
        error "sand: incompatible order by, limit, distinct, or group by"

instance Monoid SQL where
    mempty = SQLQuery [] [] SQLTrueCond [] top False []

sor :: SQL -> SQL -> SQL
sor (SQLQuery sselect1 sfrom1 swhere1 [] Top False []) (SQLQuery sselect2 sfrom2 swhere2 [] Top False []) = SQLQuery (sselect1 ++ sselect2) (sfrom1 `mergeTables` sfrom2) (swhere1 .||. swhere2) [] top False []
sor _ _ = error "sor: incompatible order by, limit, distinct, or group by"

snot :: SQL -> SQL
snot (SQLQuery sselect sfrom swhere _ (Drop (Ordered 0)) _ _) = mempty
snot (SQLQuery sselect sfrom swhere _ _ _ _) = SQLQuery sselect sfrom (SQLNotCond swhere) [] top False []

swhere :: SQLCond -> SQL
swhere swhere1 = SQLQuery [] [] swhere1 [] top False []

class Params a where
    params :: a -> [Var]

instance Params a => Params [a] where
    params = foldMap params

instance Params SQLExpr where
    params (SQLParamExpr p) = [Var p]
    params (SQLCastExpr e _) = params e
    params (SQLArrayExpr a b) = params a ++ params b
    params (SQLInfixFuncExpr _ a b) = params a ++ params b
    params (SQLFuncExpr _ es) = foldMap params es
    params (SQLUnaryOpExpr _ e) = params e
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
    params (SQLQueryStmt sql) = params sql
    params (SQLInsertStmt _ vs _ cond) = params (map snd vs) ++ params cond
    params (SQLUpdateStmt _ vs cond) = params (map snd vs) ++ params cond
    params (SQLDeleteStmt _ cond) = params cond

instance Params FromTable where
    params (SimpleTable _ _) = []
    params (QueryTable sql _) = params sql

instance Params SQL where
    params (SQLQuery sel from cond _ _ _ _) = params sel ++ params from ++ params cond

instance Params (Var, SQLExpr) where
    params (_, expr) = params expr

instance Params SQLQuery where
    params (_, _, params) = params

instance Serialize SQLQuery where
    serialize (_, stmt, _) = show stmt

class SimplifySQLCond a where
    simplifySQLCond :: a -> a

instance SimplifySQLCond SQLStmt where
    simplifySQLCond (SQLQueryStmt sql) = SQLQueryStmt (simplifySQLCond sql)
    simplifySQLCond (SQLInsertStmt t s f cond) = SQLInsertStmt t s f (simplifySQLCond cond)
    simplifySQLCond (SQLUpdateStmt t cs cond) = SQLUpdateStmt t cs (simplifySQLCond cond)
    simplifySQLCond (SQLDeleteStmt t cond) = SQLDeleteStmt t (simplifySQLCond cond)

instance SimplifySQLCond SQL where
    simplifySQLCond (SQLQuery s f cond orderby limit distinct groupby) = SQLQuery s (simplifySQLCond f) (simplifySQLCond cond) orderby limit distinct groupby

instance SimplifySQLCond a => SimplifySQLCond [a] where
    simplifySQLCond = map simplifySQLCond

instance SimplifySQLCond FromTable where
    simplifySQLCond table@(SimpleTable _ _) = table
    simplifySQLCond (QueryTable sql v) = QueryTable (simplifySQLCond sql) v

instance SimplifySQLCond SQLCond where
    simplifySQLCond c@(SQLCompCond "=" a b) | a == b = SQLTrueCond
    simplifySQLCond c@(SQLCompCond _ _ _) = c
    simplifySQLCond (SQLTrueCond) = SQLTrueCond
    simplifySQLCond (SQLFalseCond) = SQLFalseCond
    simplifySQLCond c@(SQLAndCond _ _) =
        let conj = getSQLConjuncts c
            conj2 = concatMap (getSQLConjuncts . simplifySQLCond) conj
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
    simplifySQLCond (SQLOrCond a b) = case simplifySQLCond a of
        SQLFalseCond -> simplifySQLCond b
        SQLTrueCond -> SQLTrueCond
        a' -> case simplifySQLCond b of
            SQLFalseCond -> a'
            SQLTrueCond -> SQLTrueCond
            b' -> SQLOrCond a' b'
    simplifySQLCond (SQLNotCond a) = case simplifySQLCond a of
        SQLTrueCond -> SQLFalseCond
        SQLFalseCond -> SQLTrueCond
        a' -> SQLNotCond a'
    simplifySQLCond (SQLExistsCond sql) = SQLExistsCond (simplifySQLCond sql)


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

sqlsummarize2 :: [(Var, SQLExpr)] -> [SQLExpr] -> SQL -> SQLVar -> SQL
sqlsummarize2 funcs groupby sql v =
    SQLQuery funcs [QueryTable sql v] SQLTrueCond [] top False []

sqlorderby :: SQLOrder -> SQLExpr -> SQL -> SQL
sqlorderby ord expr (SQLQuery sel from whe orderby limit distinct groupby) =
    SQLQuery sel from whe ((expr, ord) : orderby) limit distinct groupby

sqllimit :: IntLattice -> SQL -> SQL
sqllimit n (SQLQuery sel from whe orderby limit distinct groupby) =
    SQLQuery sel from whe orderby (n /\ limit) distinct groupby


instance New SQLVar SQLVar where
    new (SQLVar svn) = SQLVar <$> new (StringWrapper svn)
    
    
