{-# LANGUAGE TypeFamilies, RankNTypes, GADTs #-}

module QueryArrow.Binding.Binding where

import QueryArrow.FO.Data
import QueryArrow.Utils
import QueryArrow.DB.ResultStream
import QueryArrow.DB.DB
import QueryArrow.DB.NoConnection
import Data.Set (Set, member)
import Data.Map (fromList, singleton)
import Control.Monad.IO.Class (MonadIO(..))
import Data.List (find)
import Data.Maybe
import QueryArrow.FO.Utils

-- dbName :: a -> String
data ParamIO = I | O

class Binding a where
  bindingPred :: a -> Pred
  bindingSupport :: a -> [ParamIO] -> Bool
  bindingSupportInsert :: a -> Bool
  bindingSupportDelete :: a -> Bool
  bindingExec :: a -> [ParamIO] -> [ResultValue] -> IO [[ResultValue]]
  bindingInsert :: a -> [ResultValue] -> IO ()
  bindingDelete :: a -> [ResultValue] -> IO ()

data AbstractBinding = forall a. (Binding a) => AbstractBinding a

instance Binding AbstractBinding where
  bindingPred ab =
      case ab of
          AbstractBinding b -> bindingPred b
  bindingSupport ab =
    case ab of
        AbstractBinding b -> bindingSupport b
  bindingSupportInsert ab =
    case ab of
        AbstractBinding b -> bindingSupportInsert b
  bindingSupportDelete ab =
    case ab of
        AbstractBinding b -> bindingSupportDelete b
  bindingExec ab =
    case ab of
        AbstractBinding b -> bindingExec b
  bindingInsert ab =
    case ab of
        AbstractBinding b -> bindingInsert b
  bindingDelete ab =
    case ab of
        AbstractBinding b -> bindingDelete b

data BindingDatabase = BindingDatabase String [AbstractBinding]

getBindingByPredName :: PredName -> [AbstractBinding] -> AbstractBinding
getBindingByPredName predname = fromMaybe (error "cannot find predicate") . find (\ab -> predName (bindingPred ab) == predname)

instance IDatabase0 BindingDatabase where
    type DBFormulaType BindingDatabase = Formula
    getName (BindingDatabase n _) = n
    getPreds (BindingDatabase _ bs) = map bindingPred bs
    supported (BindingDatabase _ db) _ (FAtomic (Atom predname [VarExpr var1])) env | not (var1 `member` env) = bindingSupport (getBindingByPredName predname db) [O]
    supported (BindingDatabase _ db) _ (FAtomic (Atom predname [_])) _ = bindingSupport (getBindingByPredName predname db) [I]
    supported (BindingDatabase _ db) _ (FAtomic (Atom predname [VarExpr var1, VarExpr var2])) env | not (var1 `member` env || var2 `member` env) = bindingSupport (getBindingByPredName predname db) [O,O]
    supported (BindingDatabase _ db) _ (FAtomic (Atom predname [VarExpr var1, _])) env | not (var1 `member` env) = bindingSupport (getBindingByPredName predname db) [O,I]
    supported (BindingDatabase _ db) _ (FAtomic (Atom predname [_, VarExpr var2])) env | not (var2 `member` env) = bindingSupport (getBindingByPredName predname db) [I,O]
    supported (BindingDatabase _ db) _ (FAtomic (Atom predname [_, _])) _ = bindingSupport (getBindingByPredName predname db) [I,I]
    supported (BindingDatabase _ db) _ (FAtomic (Atom predname [VarExpr var1, VarExpr var2, VarExpr var3])) env | not (var1 `member` env || var2 `member` env || var3 `member` env) = bindingSupport (getBindingByPredName predname db) [O,O,O]
    supported (BindingDatabase _ db) _ (FAtomic (Atom predname [VarExpr var1, _, VarExpr var3])) env | not (var1 `member` env || var3 `member` env) = bindingSupport (getBindingByPredName predname db) [O,I,O]
    supported (BindingDatabase _ db) _ (FAtomic (Atom predname [_, VarExpr var2, VarExpr var3])) env | not (var2 `member` env || var3 `member` env) = bindingSupport (getBindingByPredName predname db) [I,O,O]
    supported (BindingDatabase _ db) _ (FAtomic (Atom predname [_, _, VarExpr var3])) env |  not (var3 `member` env) = bindingSupport (getBindingByPredName predname db) [I,I,O]
    supported (BindingDatabase _ db) _ (FAtomic (Atom predname [VarExpr var1, VarExpr var2, _])) env | not (var1 `member` env || var2 `member` env) = bindingSupport (getBindingByPredName predname db) [O,O,I]
    supported (BindingDatabase _ db) _ (FAtomic (Atom predname [VarExpr var1, _, _])) env | not (var1 `member` env) = bindingSupport (getBindingByPredName predname db) [O,I,I]
    supported (BindingDatabase _ db) _ (FAtomic (Atom predname [_, VarExpr var2, _])) env | not (var2 `member` env) = bindingSupport (getBindingByPredName predname db) [I,O,I]
    supported (BindingDatabase _ db) _ (FAtomic (Atom predname [_,_,_])) _ = bindingSupport (getBindingByPredName predname db) [I,I,I]
    supported (BindingDatabase _ db) _ (FInsert (Lit Pos (Atom predname _))) _ = bindingSupportInsert (getBindingByPredName predname db)
    supported (BindingDatabase _ db) _ (FInsert (Lit Neg (Atom predname _))) _ = bindingSupportDelete (getBindingByPredName predname db)
    supported _ _ _ _ = False
    checkQuery _ _ _ _ = return (Right ())

instance IDatabase1 BindingDatabase where
    type DBQueryType BindingDatabase = ( Set Var, Formula,  Set Var)
    translateQuery _ vars qu vars2 = return (vars, qu, vars2)

instance INoConnectionDatabase2 BindingDatabase where
    type NoConnectionQueryType BindingDatabase = (Set Var, Formula, Set Var)
    type NoConnectionRowType BindingDatabase = MapResultRow
    noConnectionDBStmtExec (BindingDatabase _ db) (_,  FAtomic (Atom predname [VarExpr a]), env) stream | not (a `member` env) = do
        row <- stream
        rs <- liftIO $ bindingExec (getBindingByPredName predname db) [O] []
        listResultStream (map (\[val1] -> singleton a val1) rs)
    noConnectionDBStmtExec (BindingDatabase _ db) (_, FAtomic (Atom predname [a]), _) stream = do
        row <- stream
        let aval = evalExpr row a
        rs <- liftIO $ bindingExec (getBindingByPredName predname db) [I] [aval]
        listResultStream (map (\[] -> mempty) rs)
    noConnectionDBStmtExec (BindingDatabase _ db) (_,  FAtomic (Atom predname [VarExpr a, VarExpr b]), env) stream | not (a `member` env || b `member` env) = do
        row <- stream
        rs <- liftIO $ bindingExec (getBindingByPredName predname db) [O,O] []
        listResultStream (map (\[val1,val2] -> fromList [(a, val1),(b,val2)]) rs)
    noConnectionDBStmtExec (BindingDatabase _ db) (_, FAtomic (Atom predname [a, VarExpr b]), env) stream | not (b `member` env) = do
        row <- stream
        let aval = evalExpr row a
        rs <- liftIO $ bindingExec (getBindingByPredName predname db) [I,O] [aval]
        listResultStream (map (\[val2] -> singleton b val2) rs)
    noConnectionDBStmtExec (BindingDatabase _ db) (_, FAtomic (Atom predname [VarExpr a, b]), env) stream | not (a `member` env) = do
        row <- stream
        let bval = evalExpr row b
        rs <- liftIO $ bindingExec (getBindingByPredName predname db) [O,I] [bval]
        listResultStream (map (\[val1] -> singleton a val1) rs)
    noConnectionDBStmtExec (BindingDatabase _ db) (_, FAtomic (Atom predname [a, b]), _) stream = do
        row <- stream
        let aval = evalExpr row a
        let bval = evalExpr row b
        rs <- liftIO $ bindingExec (getBindingByPredName predname db) [I,I] [aval, bval]
        listResultStream (map (\[] -> mempty) rs)
    noConnectionDBStmtExec (BindingDatabase _ db) (_,  FAtomic (Atom predname [VarExpr a, VarExpr b, VarExpr c]), env) stream | not (a `member` env || b `member` env || c `member` env) = do
        row <- stream
        rs <- liftIO $ bindingExec (getBindingByPredName predname db) [O,O,O] []
        listResultStream (map (\[val1,val2,val3] -> fromList [(a, val1),(b,val2),(c,val3)]) rs)
    noConnectionDBStmtExec (BindingDatabase _ db) (_, FAtomic (Atom predname [a, VarExpr b, VarExpr c]), env) stream | not (b `member` env || c `member` env) = do
        row <- stream
        let aval = evalExpr row a
        rs <- liftIO $ bindingExec (getBindingByPredName predname db) [I,O,O] [aval]
        listResultStream (map (\[val2,val3] -> fromList [(b,val2),(c,val3)]) rs)
    noConnectionDBStmtExec (BindingDatabase _ db) (_, FAtomic (Atom predname [VarExpr a, b, VarExpr c]), env) stream | not (a `member` env || c `member` env) = do
        row <- stream
        let bval = evalExpr row b
        rs <- liftIO $ bindingExec (getBindingByPredName predname db) [O,I,O] [bval]
        listResultStream (map (\[val1,val3] -> fromList [(a, val1),(c,val3)]) rs)
    noConnectionDBStmtExec (BindingDatabase _ db) (_, FAtomic (Atom predname [a, b, VarExpr c]), env) stream | not (c `member` env) = do
        row <- stream
        let aval = evalExpr row a
        let bval = evalExpr row b
        rs <- liftIO $ bindingExec (getBindingByPredName predname db) [I,I,O] [aval, bval]
        listResultStream (map (\[val3] -> singleton c val3) rs)
    noConnectionDBStmtExec (BindingDatabase _ db) (_,  FAtomic (Atom predname [VarExpr a, VarExpr b, c]), env) stream | not (a `member` env || b `member` env) = do
        row <- stream
        let cval = evalExpr row c
        rs <- liftIO $ bindingExec (getBindingByPredName predname db) [O,O,I] [cval]
        listResultStream (map (\[val1,val2] -> fromList [(a, val1),(b,val2)]) rs)
    noConnectionDBStmtExec (BindingDatabase _ db) (_, FAtomic (Atom predname [a, VarExpr b, c]), env) stream | not (b `member` env) = do
        row <- stream
        let aval = evalExpr row a
        let cval = evalExpr row c
        rs <- liftIO $ bindingExec (getBindingByPredName predname db) [I,O,I] [aval,cval]
        listResultStream (map (\[val2] -> singleton b val2) rs)
    noConnectionDBStmtExec (BindingDatabase _ db) (_, FAtomic (Atom predname [VarExpr a, b, c]), env) stream | not (a `member` env) = do
        row <- stream
        let bval = evalExpr row b
        let cval = evalExpr row c
        rs <- liftIO $ bindingExec (getBindingByPredName predname db) [O,I,I] [bval, cval]
        listResultStream (map (\[val1] -> singleton a val1) rs)
    noConnectionDBStmtExec (BindingDatabase _ db) (_, FAtomic (Atom predname [a, b, c]), _) stream = do
        row <- stream
        let aval = evalExpr row a
        let bval = evalExpr row b
        let cval = evalExpr row c
        rs <- liftIO $ bindingExec (getBindingByPredName predname db) [I,I,I] [aval, bval, cval]
        listResultStream (map (\[] -> mempty) rs)
    noConnectionDBStmtExec (BindingDatabase _ db) (_, FInsert (Lit Pos (Atom predname as)), _) stream = do
        row <- stream
        let avals = map (evalExpr row) as
        liftIO $ bindingInsert (getBindingByPredName predname db) avals
        return mempty
    noConnectionDBStmtExec (BindingDatabase _ db) (_, FInsert (Lit Neg (Atom predname as)), _) stream = do
        row <- stream
        let avals = map (evalExpr row) as
        liftIO $ bindingDelete (getBindingByPredName predname db) avals
        return mempty

    noConnectionDBStmtExec _ qu _ = error ("noConnectionDBStmtExec: unsupported Formula " ++ show qu)


data BinaryBoolean = BinaryBoolean String String CastType CastType (ResultValue -> ResultValue -> Bool)

instance Binding BinaryBoolean where
    bindingPred (BinaryBoolean ns n t1 t2 _) = Pred (PredName [ns] n) (PredType ObjectPred [PTKeyI t1, PTKeyI t2])
    bindingSupport  _  [I,I] = True
    bindingSupport _  _ = False
    bindingSupportInsert _ = False
    bindingSupportDelete _ = False
    bindingExec (BinaryBoolean _ _ _ _ func) _ [val1, val2] =
      return (if func val1 val2
        then [[]]
        else [])


data UnaryFunction = UnaryFunction  String String CastType CastType (ResultValue -> ResultValue)

instance Binding UnaryFunction where
    bindingPred (UnaryFunction ns n t1 t2 _) = Pred (PredName [ns] n) (PredType PropertyPred [PTKeyI t1, PTPropIO t2])
    bindingSupport _ [I,_] = True
    bindingSupport _ _ = False
    bindingSupportInsert _ = False
    bindingSupportDelete _ = False
    bindingExec (UnaryFunction _ _ _ _ func) [I,I] [val1, val2] =
      return (if func val1 == val2
        then [[]]
        else [])
    bindingExec (UnaryFunction _ _ _ _ func) [I,O] [val1] =
      return [ [func val1] ]

data UnaryIso = UnaryIso String String CastType CastType (ResultValue -> ResultValue) (ResultValue -> ResultValue)

instance Binding UnaryIso where
    bindingPred (UnaryIso ns n t1 t2 _ _) = Pred (PredName [ns] n) (PredType PropertyPred [PTKeyIO t1, PTPropIO t2])
    bindingSupport _ [I,I] = True
    bindingSupport _ [I,O]= True
    bindingSupport _ [O,I]= True
    bindingSupport _ _ = False
    bindingSupportInsert _ = False
    bindingSupportDelete _ = False
    bindingExec (UnaryIso _ _ _ _ func _) [I,I] [val1, val2] =
      return (if func val1 == val2
        then [[]]
        else [])
    bindingExec (UnaryIso _ _ _ _ func _) [I, O] [val1] =
      return [ [func val1] ]
    bindingExec (UnaryIso _ _ _ _ _ g) [O, I] [val2] =
      return [ [g val2] ]

data BinaryFunction = BinaryFunction  String String CastType CastType CastType (ResultValue -> ResultValue -> ResultValue)

instance Binding BinaryFunction where
    bindingPred (BinaryFunction ns n t1 t2 t3 _) = Pred (PredName [ns] n) (PredType PropertyPred [PTKeyI t1, PTKeyI t2, PTPropIO t3])
    bindingSupport _ [I,I,_] = True
    bindingSupport _ _ = False
    bindingSupportInsert _ = False
    bindingSupportDelete _ = False
    bindingExec (BinaryFunction _ _ _ _ _ func) [I,I,I] [val1, val2, val3] =
      return (if func val1 val2 == val3
        then [[]]
        else [])
    bindingExec (BinaryFunction _ _ _ _ _ func) [I,I,O] [val1, val2] =
      return [ [func val1 val2] ]

data UnaryProcedure = UnaryProcedure String String CastType (ResultValue -> IO ())

instance Binding UnaryProcedure where
    bindingPred (UnaryProcedure ns n t1 _) = Pred (PredName [ns] n) (PredType ObjectPred [PTKeyI t1])
    bindingSupport _ [I] = True
    bindingSupport _ _ = False
    bindingSupportInsert _ = False
    bindingSupportDelete _ = False
    bindingExec (UnaryProcedure _ _ _ func) [I] [val1] = do
      func val1
      return [[]]

data BinaryParamIso = BinaryParamIso String String CastType CastType CastType (ResultValue -> ResultValue -> ResultValue) (ResultValue -> ResultValue -> ResultValue)

instance Binding BinaryParamIso where
    bindingPred (BinaryParamIso ns n t1 t2 t3 _ _) = Pred (PredName [ns] n) (PredType PropertyPred [PTKeyIO t1, PTKeyI t2, PTPropIO t3])
    bindingSupport _ [I,I,I] = True
    bindingSupport _ [I,I,O] = True
    bindingSupport _ [O,I,I] = True
    bindingSupport _ _ = False
    bindingSupportInsert _ = False
    bindingSupportDelete _ = False
    bindingExec (BinaryParamIso _ _ _ _ _ func _) [I,I,I] [a, b, c] =
        return (if func a b == c
            then [[]]
            else [])
    bindingExec (BinaryParamIso _ _ _ _ _ func _) [I,I,O] [a, b] =
        return [[func a b]]
    bindingExec (BinaryParamIso _ _ _ _ _ _ g) [O,I,I] [b, c] =
        return [[g c b]]

data BinaryIso = BinaryIso String String CastType CastType CastType (ResultValue -> ResultValue -> ResultValue) (ResultValue -> ResultValue -> ResultValue) (ResultValue -> ResultValue -> ResultValue)

instance Binding BinaryIso where
    bindingPred (BinaryIso ns n t1 t2 t3 _ _ _) = Pred (PredName [ns] n) (PredType PropertyPred [PTKeyIO t1, PTKeyIO t2, PTPropIO t3])
    bindingSupport _ [I,I,I] = True
    bindingSupport _ [I,I,O] = True
    bindingSupport _ [I,O,I] = True
    bindingSupport _ [O,I,I] = True
    bindingSupport _ _ = False
    bindingSupportInsert _ = False
    bindingSupportDelete _ = False
    bindingExec (BinaryIso _ _ _ _ _ func _ _) [I,I,I] [a, b, c] =
        return (if func a b == c
            then [[]]
            else [])
    bindingExec (BinaryIso _ _ _ _ _ func _ _) [I,I,O] [a, b] =
        return [[func a b]]
    bindingExec (BinaryIso _ _ _ _ _ _ g _) [I,O,I] [a, c] =
        return [[g a c]]
    bindingExec (BinaryIso _ _ _ _ _ _ _ h) [O,I,I] [b, c] =
        return [[h b c]]

data BinaryMono = BinaryMono String String CastType CastType CastType (ResultValue -> ResultValue -> ResultValue) (ResultValue -> ResultValue -> Maybe ResultValue) (ResultValue -> ResultValue -> Maybe ResultValue)

instance Binding BinaryMono where
    bindingPred (BinaryMono ns n t1 t2 t3 _ _ _) = Pred (PredName [ns] n) (PredType PropertyPred [PTKeyIO t1, PTKeyIO t2, PTPropIO t3])
    bindingSupport _ [I,I,I] = True
    bindingSupport _ [I,I,O] = True
    bindingSupport _ [I,O,I] = True
    bindingSupport _ [O,I,I] = True
    bindingSupport _ _ = False
    bindingSupportInsert _ = False
    bindingSupportDelete _ = False
    bindingExec (BinaryMono _ _ _ _ _ func _ _) [I,I,I] [a, b, c] =
        return (if func a b == c
            then [[]]
            else [])
    bindingExec (BinaryMono _ _ _ _ _ func _ _) [I,I,O] [a, b] =
      return [[func a b]]
    bindingExec (BinaryMono _ _ _ _ _ _ g _) [I,O,I] [a, c] =
      return (case g a c of
        Just b -> [[b]]
        Nothing -> [])
    bindingExec (BinaryMono _ _ _ _ _ _ _ h) [O,I,I] [b, c] =
      return (case h b c of
        Just a -> [[a]]
        Nothing -> [])
