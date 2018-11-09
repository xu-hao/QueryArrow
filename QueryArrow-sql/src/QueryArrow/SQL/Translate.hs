{-# LANGUAGE FlexibleContexts, MultiParamTypeClasses, FlexibleInstances, OverloadedStrings, GADTs, ExistentialQuantification, ParallelListComp #-}

module QueryArrow.SQL.Translate where

import QueryArrow.Syntax.Term
import QueryArrow.Syntax.Type

import Prelude hiding (lookup)
import Data.Text (Text, pack)
import System.Log.Logger (errorM, infoM)
import Data.List (find, intercalate, nub)
import Data.Maybe (fromMaybe)
import Data.Char (toLower)
import qualified Data.Map.Strict as M
import Control.Monad.Reader
import Control.Monad.State.Strict
import Debug.Trace

import QueryArrow.SQL.SQL hiding (TransMonad)
import QueryArrow.SQL.Mapping
import Control.Arrow ((***))
import Data.Namespace.Path


type SQLTableColToPredIndex = M.Map TableName (M.Map Col [(PredName, Int)])
type SQLPredToColsIndex = M.Map PredName [Col]
type SQLAliasToTableIndex = M.Map SQLVar TableName
type SQLQColToVarIndex = M.Map SQLQualifiedCol Var
type SQLColToTableIndex = M.Map Col [TableName]
type SQLTableToAliasIndex = M.Map TableName [SQLVar]
type SQLVarToSQLExprIndex = M.Map String SQLExpr
type SQLSQLOperToPredIndex = M.Map SQLOper PredName

createSQLTableIndex :: NamespacePath String -> [SQLMapping] -> SQLTableColToPredIndex
createSQLTableIndex namespacepath = M.unionsWith (M.unionWith (++)) . (map (\(SQLMapping uqpredname tablename colnames) ->
  let qpredname = ObjectPath namespacepath uqpredname in
      M.singleton tablename (M.fromList [(colname, [(qpredname, i)]) | colname <- colnames | i <- [0..(length colnames - 1)]])))

createSQLPredIndex :: NamespacePath String -> [SQLMapping] -> SQLPredToColsIndex
createSQLPredIndex namespacepath = M.fromList . map (\(SQLMapping uqpredname _ colnames) -> (ObjectPath namespacepath uqpredname, colnames))

createSQLColIndex :: [SQLMapping] -> SQLColToTableIndex
createSQLColIndex = M.unionsWith (++) . map (\(SQLMapping uqpredname tablename colnames) ->
  M.fromList [(colname, [tablename]) | colname <- colnames])
  
createSQLSQLOperToPredIndex :: NamespacePath String -> SQLSQLOperToPredIndex
createSQLSQLOperToPredIndex namespacepath = 
  M.fromList [(sqloper, qpredname) | sqloper <- ["<",">","=","<>","<=",">="] | uqpredname <- ["lt","gt","eq","ne","le","ge"], let qpredname = ObjectPath namespacepath uqpredname]


data Immutable = Immutable {
  tableColToPred :: SQLTableColToPredIndex, 
  predToCols :: SQLPredToColsIndex, 
  colToTable :: SQLColToTableIndex, 
  sqlOperToPred :: SQLSQLOperToPredIndex
}

data Mutable = Mutable {
  qcolToVar :: SQLQColToVarIndex, 
  aliasToTable :: SQLAliasToTableIndex,
  tableToAlias :: SQLTableToAliasIndex,
  varToSQLExpr :: SQLVarToSQLExprIndex
}
  
type TransMonad = ReaderT Immutable (StateT Mutable NewEnv)

runTrans :: TransMonad a -> Immutable -> a
runTrans a im = runNew (evalStateT (runReaderT a im) (Mutable
                    M.empty
                    M.empty
                    M.empty
                    M.empty))


-- | return or generate a new default alias for table name
getAliasByTableName :: TableName -> TransMonad SQLVar
getAliasByTableName tablename = do
  mutable <- lift $ get
  case M.lookup tablename (tableToAlias mutable) of
    Just [alias] ->
      return alias
    Just aliases@(_:_:_) -> do
      error ("getAliasByTableName: ambiguous aliases for table \"" ++ tablename ++ "\"")
    _ ->
      error ("getAliasByTableName: cannot find any alias for table \"" ++ tablename ++ "\"")

-- | return or generate a new default alias for table name
getTableNameByAlias :: SQLVar -> TransMonad TableName
getTableNameByAlias alias@(SQLVar aliasname) = do
  mutable <- lift $ get
  case M.lookup alias (aliasToTable mutable) of
    Just tablename ->
      return tablename
    _ ->
      return aliasname

-- | return or generate a QAL var for a qualified column
getVarByQCol :: SQLQualifiedCol -> TransMonad Var
getVarByQCol qcol@(SQLQualifiedCol sqlvar colname) = do
  mutable <- lift $ get
  case M.lookup qcol (qcolToVar mutable) of
    Just v -> return v
    Nothing -> do
      nv <- lift . lift $ new (Var colname)
      let sqlqcoltovarindex' = M.insert qcol nv (qcolToVar mutable)
      lift $ put mutable {qcolToVar = sqlqcoltovarindex'}
      return nv

-- | return a qualified column from an unqualified column
getQColByCol :: Col -> TransMonad SQLQualifiedCol
getQColByCol colname = do
  immutable <- ask
  case M.lookup colname (colToTable immutable) of
    Just [tablename] -> do
      alias <- getAliasByTableName tablename
      return (SQLQualifiedCol alias colname)
    Just tablenames@(_:_:_) ->
      error ("getQColByCol: ambiguous tables for column \"" ++ colname ++ "\"")
    _ ->
      error ("getQColByCol: cannot find any table for column \"" ++ colname ++ "\"")

-- | translate qualified column to a query that assign the value of that column to a variable
translateQColToQuery :: SQLQualifiedCol -> TransMonad Formula
translateQColToQuery (SQLQualifiedCol alias colname) = do
  immutable <- ask
  tablename <- getTableNameByAlias alias
  case M.lookup tablename (tableColToPred immutable) of
    Just colindex ->
      case M.lookup colname colindex of
        Just ((predname, i) : _) ->
          case M.lookup predname (predToCols immutable) of
            Just cols -> do
              vars <- mapM getVarByQCol (map (SQLQualifiedCol alias) cols)
              return (predname @@@ map VarExpr vars)
            Nothing ->  error ("translateQColToQuery: cannot find columns for predicate \"" ++ show predname ++ "\"")
        _ -> error ("translateQColToQuery: cannot find predicate for column \"" ++ tablename ++ "." ++ colname ++ "\"")
    Nothing -> error ("translateQColToQuery: cannot find col index for table \"" ++ tablename ++ "\"")

translateSQLToQuery :: SQL -> TransMonad Formula
translateSQLToQuery qu@(SQLQuery sel from whe _ _ _ groupbys) = do
  mapM_ addFromToAliasMap from
  mapM_ addSelectToColAliasMap sel
  mapM_ generateAliasesInFrom from
  selq <- fsequencing <$> mapM translateSelectToQuery sel
  wheq <- translateWhereToQuery whe
  let form = selq .*. wheq
  if isSQLAnyAggregation qu then
    if isSQLAllAggregation qu then do
      binds <- mapM translateSelToBind sel
      groups <- mapM translateGroupByToVar groupbys
      return (Aggregate (Summarize binds groups) form)
    else
      error ("translateSQLToQuery: mixed aggregation and nonaggregation selects")
  else do
    vars <- mapM translateSelectToReturn sel
    return (Aggregate (FReturn vars) form)


-- | add aliases in from to alias to table map and table to aliases map
addFromToAliasMap :: FromTable -> TransMonad ()
addFromToAliasMap (SimpleTable tablename alias@(SQLVar aliasname)) = 
  if aliasname == "" then
    return ()
  else do
    mutable <- lift $ get
    if alias `M.member` (aliasToTable mutable) then
      error ("addFromToAliasMap: redefinition of alias \"" ++ aliasname ++ "\"")
    else do
      lift . lift $ register [aliasname]
      let sqlaliastotableindex' = M.insert alias tablename (aliasToTable mutable)
      let sqltabletoaliasindex' = M.alter (\a -> 
                Just (case a of
                  Nothing -> [alias]
                  Just as -> alias : as)) tablename (tableToAlias mutable)
      lift $ put mutable{aliasToTable = sqlaliastotableindex', tableToAlias = sqltabletoaliasindex'}
addFromToAliasMap a = error ("addFromToAliasMap: unsupported " ++ show a)

-- | generete aliases for tables without aliases in from and add them to alias to table map and table to aliases map
generateAliasesInFrom :: FromTable -> TransMonad ()
generateAliasesInFrom (SimpleTable tablename alias@(SQLVar aliasname)) = 
  if aliasname /= "" then
    return ()
  else do
    mutable <- lift $ get
    alias <- lift . lift $ new (SQLVar tablename)
    let sqlaliastotableindex' = M.insert alias tablename (aliasToTable mutable)
    let sqltabletoaliasindex' = M.alter (\a -> 
                Just (case a of
                  Nothing -> [alias]
                  Just as -> alias : as)) tablename (tableToAlias mutable)
    lift $ put mutable {aliasToTable = sqlaliastotableindex', tableToAlias = sqltabletoaliasindex'}
generateAliasesInFrom a = error ("generateAliasesInFrom: unsupported " ++ show a)

-- | add column variables to var map
addSelectToVarMap :: (Var, SQLExpr) -> TransMonad ()
addSelectToVarMap (Var "", _) =
  return ()
addSelectToVarMap (Var name, sqlexpr) = do
  lift . lift $ register [name]
  lift $ modify (\mutable -> mutable{varToSQLExpr = M.insert name sqlexpr (varToSQLExpr mutable)} )
    
-- | a query that assigns the value of columns that appear in the clause to variables
translateSelectToQuery :: (Var, SQLExpr) -> TransMonad Formula
translateSelectToQuery (Var vn, sqlexpr) =
  case sqlexpr of
    SQLColExpr qcol ->
      translateQColToQuery qcol
    SQLVarExpr name -> do
      mutable <- lift $ get
      case M.lookup name (varToSQLExpr mutable) of
        Just sqlexpr2 ->
          translateSelectToQuery (Var "", sqlexpr2)
        Nothing ->
          getQColByCol name >>= translateQColToQuery
    SQLFuncExpr fn args -> 
      fsequencing <$> mapM translateSelectToQuery (map (\arg -> (Var "",arg)) args)
    _ -> 
      error ("translateSelectToQuery: unsupported " ++ show sqlexpr)

-- | a query that assigns the value of columns that appear in the clause to variables
translateWhereToQuery :: SQLCond -> TransMonad Formula
translateWhereToQuery SQLTrueCond = 
  return FOne
translateWhereToQuery SQLFalseCond = 
  return FZero
translateWhereToQuery (SQLAndCond a b) = 
  FSequencing <$> translateWhereToQuery a <*> translateWhereToQuery b
translateWhereToQuery (SQLOrCond a b) = 
  FChoice <$> translateWhereToQuery a <*> translateWhereToQuery b
translateWhereToQuery (SQLCompCond sqloper x y) = do
  immutable <- ask
  case M.lookup sqloper (sqlOperToPred immutable) of
    Just predname -> do
      exprx <- translateSQLExprToExpr x
      expry <- translateSQLExprToExpr y
      return (predname @@@ [exprx, expry])
    Nothing ->
      error ("translateWhereToQuery: cannot find predicate for sql operator \"" ++ sqloper ++ "\"")
translateWhereToQuery a = 
  error ("translateWhereToQuery: unsupported " ++ show a)
                              
translateSQLExprToExpr :: SQLExpr -> TransMonad Expr
translateSQLExprToExpr sqlexpr =
  case sqlexpr of
    SQLColExpr qcol ->
      VarExpr <$> getVarByQCol qcol
    SQLVarExpr name -> do
      mutable <- lift $ get
      case M.lookup name (varToSQLExpr mutable) of
        Just sqlexpr2 ->
          translateSQLExprToExpr sqlexpr2
        Nothing ->
          getQColByCol name >>= getVarByQCol >>= (return . VarExpr)
    SQLIntConstExpr i -> 
      return (IntExpr i)
    SQLStringConstExpr s ->
      return (StringExpr s)
    _ -> 
      error ("translateSQLExprToExpr: unsupported " ++ show sqlexpr)
  
isSQLAnyAggregation :: SQL -> Bool
isSQLAnyAggregation (SQLQuery sel _ _ _ _ _ _) = any isSelAggregation sel

isSQLAllAggregation :: SQL -> Bool
isSQLAllAggregation (SQLQuery sel _ _ _ _ _ _) = all isSelAggregation sel

isSelAggregation :: (Var, SQLExpr) -> Bool
isSelAggregation (_, SQLFuncExpr fn _) = aggregateFunction fn
isSelAggregation (_, SQLFuncExpr fn _) = error ("isSelAggregation: unsupported " ++ fn)
isSelAggregation _ = False

aggregateFunction :: String -> Bool
aggregateFunction fn = fn `elem` ["min", "max", "count", "avg", "sum"]

-- | add aliases in from to alias to table map and table to aliases map
addSelectToColAliasMap :: (Var, SQLExpr) -> TransMonad ()
addSelectToColAliasMap (Var "", _) =
  return ()
addSelectToColAliasMap (Var aliasname, sqlexpr) = do
  mutable <- lift $ get
  if aliasname `M.member` (varToSQLExpr mutable) then
    error ("addSelectToColAliasMap: redefinition of alias \"" ++ aliasname ++ "\"")
  else do
    lift . lift $ register [aliasname]
    let sqlvartosqlexprindex' = M.insert aliasname sqlexpr (varToSQLExpr mutable)
    lift $ put mutable{varToSQLExpr = sqlvartosqlexprindex'}
addSelectToColAliasMap a = error ("addSelectToColAliasMap: unsupported " ++ show a)

-- | a query that assigns the value of columns that appear in the clause to variables
translateSelToBind :: (Var, SQLExpr) -> TransMonad Bind
translateSelToBind (Var vn, sqlexpr) =
  case sqlexpr of
    SQLFuncExpr fn [arg] -> do
      qcol <- case arg of 
        SQLColExpr qcol ->
          return qcol
        SQLVarExpr name ->
          getQColByCol name
        _ -> 
          error ("translateSelToBind: unsupported " ++ show sqlexpr)
      var <- getVarByQCol qcol
      bvar <- lift . lift $ new var
      return (Bind bvar (case fn of
        "min" -> Min var
        "max" -> Max var
        "count" -> Count
        "avg" -> Average var
        "sum" -> Sum var
        _ -> error ("translateSelToBind: unsupported " ++ show sqlexpr)))
          
    _ -> 
      error ("translateSelToBind: unsupported " ++ show sqlexpr)

-- | a query that assigns the value of columns that appear in the clause to variables
translateGroupByToVar :: SQLExpr -> TransMonad Var
translateGroupByToVar sqlexpr = do
  qcol <- case sqlexpr of
    SQLColExpr qcol ->
      return qcol
    SQLVarExpr name ->
      getQColByCol name
    _ -> 
      error ("translateGroupByToVar: unsupported " ++ show sqlexpr)
  getVarByQCol qcol

translateSelectToReturn :: (Var, SQLExpr) -> TransMonad Var
translateSelectToReturn (Var "", sqlexpr) =
  case sqlexpr of
    SQLColExpr qcol ->
      getVarByQCol qcol
    SQLVarExpr name ->
      return (Var name)
    _ ->
      error ("translateSelectToReturn: unsupported " ++ show sqlexpr)
translateSelectToReturn (v, _) = 
  return v
  
  

