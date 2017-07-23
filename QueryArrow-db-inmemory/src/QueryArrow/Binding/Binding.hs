{-# LANGUAGE TypeFamilies, RankNTypes, GADTs #-}

module QueryArrow.Binding.Binding where

import QueryArrow.Syntax.Data
import QueryArrow.Syntax.Types
import QueryArrow.Utils
import QueryArrow.Semantics.ResultStream
import QueryArrow.DB.DB
import QueryArrow.DB.NoConnection
import Data.Set (Set, member)
import Data.Map.Strict (fromList)
import qualified Data.Map.Strict as Map
import Control.Monad.IO.Class (MonadIO(..))
import Data.List (find)
import Data.Maybe
import QueryArrow.Syntax.Utils
import QueryArrow.Semantics.ResultValue
import QueryArrow.Semantics.ResultValue.AbstractResultValue
import QueryArrow.Data.Some
import QueryArrow.Semantics.ResultRow.VectorResultRow
import QueryArrow.Semantics.ResultSet.ResultStreamResultSet
import QueryArrow.Semantics.ResultSet.VectorResultSetTransformer
import qualified Data.Vector as V
import QueryArrow.Semantics.ResultHeader.VectorResultHeader
import QueryArrow.Semantics.ResultSet
import Data.Conduit
import QueryArrow.Semantics.ResultHeader
import Data.Conduit.List (sourceList)
import QueryArrow.Data.Monoid.Action

-- dbName :: a -> String
data ParamIO = I | O

class Binding a where
  bindingPred :: a -> Pred
  bindingSupport :: a -> [ParamIO] -> Bool
  bindingSupportInsert :: a -> Bool
  bindingSupportDelete :: a -> Bool
  bindingExec :: a -> [ParamIO] -> [ConcreteResultValue] -> IO [[ConcreteResultValue]]
  bindingInsert :: a -> [ConcreteResultValue] -> IO ()
  bindingDelete :: a -> [ConcreteResultValue] -> IO ()

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

argsToIO :: Set Var -> [Expr] -> [ParamIO]
argsToIO row =
  map (\arg -> case arg of
    VarExpr a | not (a `member` row) -> O
    _ -> I)

argsToIOs :: ResultHeader -> [Expr] -> ([ParamIO], [Expr], [Var])
argsToIOs hdr =
  mconcat . map (\arg -> case arg of
    VarExpr a | not (a `V.elem` hdr) -> ([O], [], [a])
    _ -> ([I], [arg], []))

evalArgs :: ResultHeader -> VectorResultRow AbstractResultValue -> [Expr] -> [ConcreteResultValue]
evalArgs hdr row =
  map (\arg -> case evalExpr hdr row arg of Some arv -> toConcreteResultValue arv)

instance IDatabase0 BindingDatabase where
    type DBFormulaType BindingDatabase = FormulaT
    getName (BindingDatabase n _) = n
    getPreds (BindingDatabase _ bs) = map bindingPred bs
    supported (BindingDatabase _ db) _ (FAtomic2 _ (Atom predname args)) env = bindingSupport (getBindingByPredName predname db) (argsToIO env args)
    supported (BindingDatabase _ db) _ (FInsert2 _ (Lit Pos (Atom predname _))) _ = bindingSupportInsert (getBindingByPredName predname db)
    supported (BindingDatabase _ db) _ (FInsert2 _ (Lit Neg (Atom predname _))) _ = bindingSupportDelete (getBindingByPredName predname db)
    supported _ _ _ _ = False

instance IDatabase1 BindingDatabase where
    type DBQueryType BindingDatabase = ( Set Var, FormulaT,  Set Var)
    translateQuery _ vars qu vars2 = return (vars, qu, vars2)

instance INoConnectionDatabase2 BindingDatabase where
    type NoConnectionQueryType BindingDatabase = (Set Var, FormulaT, Set Var)
    type NoConnectionInputRowType BindingDatabase = VectorResultRow AbstractResultValue
    type NoConnectionResultSetType BindingDatabase = ResultStreamResultSet (ResultSetTransformer AbstractResultValue) (VectorResultRow AbstractResultValue)
    noConnectionDBStmtExec (BindingDatabase _ db) (_,  FAtomic2 _ (Atom predname args), _) rset = do
        let (ResultStream stream) = toResultStream rset
        let hdr = getHeader rset
        let (ios, iexprs, ovars) = argsToIOs hdr args
        let hdr2 = toHeader ovars
        let hdr3 = hdr V.++ hdr2
        return (ResultStreamResultSet RSId hdr3 (ResultStream (stream =$= awaitForever (\row -> do
          let ivals = evalArgs hdr row iexprs
          let rowtrans = tconvert (combineRow hdr row hdr2 :: ResultSetTransformer AbstractResultValue) :: ResultRowTransformer AbstractResultValue
          rs <- liftIO $ bindingExec (getBindingByPredName predname db) ios ivals
          sourceList (map (\ovals -> rowtrans `act` V.map Some (V.fromList ovals)) rs)))))
    noConnectionDBStmtExec (BindingDatabase _ db) (_, FInsert2 _ (Lit Pos (Atom predname as)), _) rset = do
      let (ResultStream stream) = toResultStream rset
      let hdr = getHeader rset
      return (ResultStreamResultSet RSId hdr (ResultStream (stream =$= awaitForever (\row -> do
        let avals = evalArgs hdr row as
        liftIO $ bindingInsert (getBindingByPredName predname db) avals
        yield row
        ))))
    noConnectionDBStmtExec (BindingDatabase _ db) (_, FInsert2 _ (Lit Neg (Atom predname as)), _) rset = do
      let (ResultStream stream) = toResultStream rset
      let hdr = getHeader rset
      return (ResultStreamResultSet RSId hdr (ResultStream (stream =$= awaitForever (\row -> do
        let avals = evalArgs hdr row as
        liftIO $ bindingDelete (getBindingByPredName predname db) avals
        yield row
        ))))

    noConnectionDBStmtExec _ qu _ = error ("noConnectionDBStmtExec: unsupported Formula " ++ show qu)


data UnaryFunction = UnaryFunction  String String CastType CastType (ConcreteResultValue -> ConcreteResultValue)

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

data BinaryFunction = BinaryFunction  String String CastType CastType CastType (ConcreteResultValue -> ConcreteResultValue -> ConcreteResultValue)

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

data TernaryFunction = TernaryFunction  String String CastType CastType CastType CastType (ConcreteResultValue -> ConcreteResultValue -> ConcreteResultValue -> ConcreteResultValue)

instance Binding TernaryFunction where
    bindingPred (TernaryFunction ns n t1 t2 t3 t4 _) = Pred (PredName [ns] n) (PredType PropertyPred [PTKeyI t1, PTKeyI t2, PTKeyI t3, PTPropIO t4])
    bindingSupport _ [I,I,I,_] = True
    bindingSupport _ _ = False
    bindingSupportInsert _ = False
    bindingSupportDelete _ = False
    bindingExec (TernaryFunction _ _ _ _ _ _ func) [I,I,I,I] [val1, val2, val3, val4] =
      return (if func val1 val2 val3 == val4
        then [[]]
        else [])
    bindingExec (TernaryFunction _ _ _ _ _ _ func) [I,I,I,O] [val1, val2, val3] =
      return [ [func val1 val2 val3] ]

data UnaryIso = UnaryIso String String CastType CastType (ConcreteResultValue -> ConcreteResultValue) (ConcreteResultValue -> ConcreteResultValue)

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

data BinaryIso = BinaryIso String String CastType CastType CastType (ConcreteResultValue -> ConcreteResultValue -> ConcreteResultValue) (ConcreteResultValue -> ConcreteResultValue -> ConcreteResultValue) (ConcreteResultValue -> ConcreteResultValue -> ConcreteResultValue)

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

data BinaryParamIso = BinaryParamIso String String CastType CastType CastType (ConcreteResultValue -> ConcreteResultValue -> ConcreteResultValue) (ConcreteResultValue -> ConcreteResultValue -> ConcreteResultValue)

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

data UnaryMono = UnaryMono String String CastType CastType (ConcreteResultValue -> ConcreteResultValue) (ConcreteResultValue -> Maybe ConcreteResultValue)

instance Binding UnaryMono where
    bindingPred (UnaryMono ns n t1 t2 _ _) = Pred (PredName [ns] n) (PredType PropertyPred [PTKeyIO t1, PTKeyIO t2])
    bindingSupport _ [I,I] = True
    bindingSupport _ [I,O] = True
    bindingSupport _ [O,I] = True
    bindingSupport _ _ = False
    bindingSupportInsert _ = False
    bindingSupportDelete _ = False
    bindingExec (UnaryMono _ _ _ _ func _) [I,I] [a, b] =
        return (if func a == b
            then [[]]
            else [])
    bindingExec (UnaryMono _ _ _ _ func _) [I,O] [a] =
      return [[func a]]
    bindingExec (UnaryMono _ _ _ _ _ g) [O,I] [b] =
      return (case g b of
        Just a -> [[a]]
        Nothing -> [])

data BinaryMono = BinaryMono String String CastType CastType CastType (ConcreteResultValue -> ConcreteResultValue -> ConcreteResultValue) (ConcreteResultValue -> ConcreteResultValue -> Maybe ConcreteResultValue) (ConcreteResultValue -> ConcreteResultValue -> Maybe ConcreteResultValue)

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

data UnaryBoolean = UnaryBoolean String String CastType (ConcreteResultValue -> Bool)

instance Binding UnaryBoolean where
    bindingPred (UnaryBoolean ns n t1 _) = Pred (PredName [ns] n) (PredType ObjectPred [PTKeyI t1])
    bindingSupport  _  [I] = True
    bindingSupport _  _ = False
    bindingSupportInsert _ = False
    bindingSupportDelete _ = False
    bindingExec (UnaryBoolean _ _ _ func) _ [val1] =
      return (if func val1
        then [[]]
        else [])

data BinaryBoolean = BinaryBoolean String String CastType CastType (ConcreteResultValue -> ConcreteResultValue -> Bool)

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

data TernaryBoolean = TernaryBoolean String String CastType CastType CastType (ConcreteResultValue -> ConcreteResultValue -> ConcreteResultValue -> Bool)

instance Binding TernaryBoolean where
    bindingPred (TernaryBoolean ns n t1 t2 t3 _) = Pred (PredName [ns] n) (PredType ObjectPred [PTKeyI t1, PTKeyI t2, PTKeyI t3])
    bindingSupport  _  [I,I,I] = True
    bindingSupport _  _ = False
    bindingSupportInsert _ = False
    bindingSupportDelete _ = False
    bindingExec (TernaryBoolean _ _ _ _ _ func) _ [val1, val2, val3] =
      return (if func val1 val2 val3
        then [[]]
        else [])

data UnaryProcedure = UnaryProcedure String String CastType (ConcreteResultValue -> IO ())

instance Binding UnaryProcedure where
    bindingPred (UnaryProcedure ns n t1 _) = Pred (PredName [ns] n) (PredType ObjectPred [PTKeyI t1])
    bindingSupport _ [I] = True
    bindingSupport _ _ = False
    bindingSupportInsert _ = False
    bindingSupportDelete _ = False
    bindingExec (UnaryProcedure _ _ _ func) [I] [val1] = do
      func val1
      return [[]]
