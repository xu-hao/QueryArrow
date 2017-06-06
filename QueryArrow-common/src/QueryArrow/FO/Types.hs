{-# LANGUAGE TypeSynonymInstances, FlexibleInstances, MultiParamTypeClasses, StandaloneDeriving #-}

module QueryArrow.FO.Types where

import Prelude hiding (lookup, null, filter)
import Data.Map.Strict (Map, lookup, insert, delete, fromList, keysSet, unionWith, filterWithKey, union, empty, mapKeys, toList)
import qualified Data.Map.Strict as Map
import Data.List (foldl', intercalate)
import Data.Set (Set, (\\), toAscList, member, null, filter)
import qualified Data.Set as Set
import QueryArrow.FO.Data
import QueryArrow.FO.Domain
import Control.Monad.Trans.Either
import Control.Monad.Trans.State
import Control.Monad.Trans.Reader
import Control.Monad.Except
import Control.Arrow ((***))
import Control.Monad (zipWithM_)
import Data.Maybe (fromMaybe)
import Algebra.Lattice

type VarTypeMap = Map Var ParamType
type TVarMap = Map String CastType

type TCMonad = EitherT String (StateT (VarTypeMap, TVarMap) (ReaderT PredTypeMap NewEnv))
type FormulaT = Formula1 Tie (Annotated VarTypeMap)


deriving instance Show FormulaT
deriving instance Eq FormulaT
deriving instance Ord FormulaT

instance Subst VarTypeMap where
  subst s = fromList . concatMap (\(k, v) -> 
		case lookup k s of
                    Just (VarExpr v2) -> [(v2, v)]
                    Nothing -> [(k, v)]
                    _ -> []) . toList

class Typecheck a b where
  typecheck :: a -> TCMonad b

initTCMonad :: VarTypeMap -> TCMonad ()
initTCMonad vtm = do
      lift $ modify (unionWith const vtm *** id)
      lift . lift . lift $ register (concatMap (Set.toAscList . freeTypeVars) (Map.elems vtm))

class TCUnify a where
  tcunify :: a -> a -> TCMonad ()

equal :: String -> CastType -> TCMonad ()
equal a t = do
    (_, tvm) <- lift get
    let tvm2 = insert a t tvm
    let tvm3 = tcsubst tvm2 tvm2
    lift $ modify (tcsubst tvm3 *** const tvm3)

context :: String -> TCMonad () -> TCMonad ()
context str a =
  catchError a (\e -> throwError (str++":\n" ++ e))
instance TCUnify CastType where
  tcunify s t | s == t = return ()
  tcunify (TypeVar a) t =
    equal a t
  tcunify t (TypeVar a) =
    equal a t
  tcunify s t =
    throwError ("unify: type mismatch " ++ show s ++ " and " ++ show t)

class TCSubst a where
  tcsubst :: TVarMap -> a -> a

instance TCSubst CastType where
  tcsubst _ Int64Type = Int64Type
  tcsubst _ TextType = TextType
  tcsubst _ ByteStringType = ByteStringType
  tcsubst _ ty@(RefType _) = ty
  tcsubst m tv@(TypeVar v) = fromMaybe tv (lookup v m)

instance TCSubst PredType where
  tcsubst m (PredType pk pts) = PredType pk (map (tcsubst m) pts)

instance TCSubst ParamType where
  tcsubst m (ParamType a b c t) = ParamType a b c (tcsubst m t)

instance TCSubst b => TCSubst (Map a b) where
  tcsubst m tvm = Map.map (tcsubst m) tvm

instance TCSubst FormulaT where
  tcsubst m (Annotated a f1) = Annotated (tcsubst m a) (fmap (tcsubst m) f1)

class FreeTypeVars a where
  freeTypeVars :: a -> Set String

instance FreeTypeVars CastType where
  freeTypeVars Int64Type = mempty
  freeTypeVars TextType = mempty
  freeTypeVars (RefType _) = mempty
  freeTypeVars ByteStringType = mempty
  freeTypeVars (TypeVar v) = Set.singleton v

instance FreeTypeVars PredType where
  freeTypeVars (PredType _ pts) = Set.unions (map freeTypeVars pts)

instance FreeTypeVars ParamType where
  freeTypeVars (ParamType _ _ _ t) = freeTypeVars t

checkVarType :: Var -> ParamType -> TCMonad ()
checkVarType v (ParamType pk inp out t) = do
  (vartypemap, _) <- lift get
  case lookup v vartypemap of
    Nothing -> do
        unless out $ throwError ("checkVarType: unbounded var: " ++ show v)
        lift $ modify (insert v (ParamType pk True False t) *** id)
    Just (ParamType _ inp2 out2 t2) -> do
        when (not inp && inp2) $ throwError ("checkVarType: bounded var: " ++ show v)
        when (not out && out2) $ throwError ("checkVarType: unbounded var: " ++ show v)
        context ("checkVarType: " ++ serialize v ++ " " ++ show vartypemap ++ " expected and encountered") $ tcunify t t2
        (_,tvm) <- lift get
        let t' = tcsubst tvm t
        lift $ modify (insert v (ParamType pk True False t') *** id)

unbounded :: VarTypeMap -> Set Var -> Set Var
unbounded vtm  = Set.filter (\var0 -> case lookup var0 vtm of
                                        Nothing -> True
                                        Just (ParamType _ False _ _) -> True
                                        _ -> False)

instantiatePredType :: PredType -> TCMonad PredType
instantiatePredType pt = do
    let freetvs = Set.toAscList (freeTypeVars pt)
    newtvs <- lift . lift . lift $ new (map StringWrapper freetvs)
    let s = fromList (zip freetvs (map TypeVar newtvs))
    return (tcsubst s pt)

instance Typecheck Atom () where
  typecheck a@(Atom pn args) = do
      (vtm, _) <- lift get
      predtypemap <- lift . lift $ ask
      case lookup pn predtypemap of
          Nothing -> throwError ("typecheck: cannot find predicate " ++ serialize pn)
          Just pt -> do
              (PredType _ pts) <- instantiatePredType pt
              if length pts /= length args
                  then throwError ("typecheck: argument count mismatch " ++ serialize a)
                  else
                      zipWithM_ (\paramtype arg -> do
                          let free = freeVars arg
                          let ub = unbounded vtm free
                          when (not (isVar arg) && not (null ub)) $ throwError ("typecheck: unbounded nested vars: " ++ show ub ++ ", the formula is " ++ serialize a)
                          (_, tvm) <- lift get
                          let pt'@(ParamType _ _ _ t') = tcsubst tvm paramtype
                          case arg of
                              VarExpr v ->
                                  context ("typecheck: " ++ serialize a ++ " arg " ++ serialize arg) $ checkVarType v pt'
                              IntExpr _ ->
                                  context ("typecheck: " ++ serialize a ++ " arg " ++ serialize arg) $ tcunify t' Int64Type
                              StringExpr _ ->
                                  context ("typecheck: " ++ serialize a ++ " arg " ++ serialize arg) $ tcunify t' TextType
                              PatternExpr _ ->
                                  context ("typecheck: " ++ serialize a ++ " arg " ++ serialize arg) $ tcunify t' TextType
                              CastExpr t2 _ ->
                                  context ("typecheck: " ++ serialize a ++ " arg " ++ serialize arg ++ " paramtype " ++ serialize paramtype) $ tcunify t' t2
                              NullExpr ->
                                  return ()) pts args

instance Typecheck Lit () where
    typecheck  (Lit s a) = typecheck  a

instance Typecheck Aggregator () where
  typecheck  (FReturn vars) = do
    (vtm, _) <- lift get
    let rvars = Set.fromList vars
    let ub = unbounded vtm rvars
    unless (null ub) $ throwError ("typecheck: return unbounded vars " ++ intercalate ", " (map serialize (Set.toAscList ub)) ++ " dvars = " ++ show vtm )
    let vtm' = foldl' (\vtm0 var0 -> delete var0 vtm0) vtm vars
    lift $ modify (const vtm' *** id)
  typecheck  (Summarize cols _) = do
    vtml <- mapM (\(v, s) ->
      case s of
        Max v2 -> do
            checkVarType v2 (ParamType False True False Int64Type)
            return (v, ParamType False True False Int64Type)
        Min v2 -> do
            checkVarType v2 (ParamType False True False Int64Type)
            return (v, ParamType False True False Int64Type)
        Average v2 -> do
            checkVarType v2 (ParamType False True False Int64Type)
            return (v, ParamType False True False Int64Type)
        Sum v2 -> do
            checkVarType v2 (ParamType False True False Int64Type)
            return (v, ParamType False True False Int64Type)
        Count ->
            return (v, ParamType False True False Int64Type)
        CountDistinct _ ->
            return (v, ParamType False True False Int64Type)
        Random v2 -> do
            ntv <- lift . lift . lift $ new (StringWrapper "random")
            checkVarType v2 (ParamType False True False (TypeVar ntv))
            (vtm, _) <- lift get
            return (v, fromMaybe (error ("typecheck: cannot find var type " ++ show v2)) (lookup v2 vtm))
      ) cols
    let vtm = fromList vtml
    lift $ modify (const vtm *** id)
  typecheck  Exists =
      lift $ modify (const mempty *** id)
  typecheck  Not =
      lift $ modify (const mempty *** id)
  typecheck  (OrderByDesc _) = 
      return ()
  typecheck  (OrderByAsc _) = 
      return ()
  typecheck  Distinct = 
      return ()
  typecheck  (Limit n) = 
      return ()

combineAggVtm :: Aggregator -> VarTypeMap -> TCMonad ()
combineAggVtm (FReturn _) vtm = do
    (vtm2, _) <- lift get
    lift $ modify (const (unionWith (const id) vtm vtm2) *** id)
combineAggVtm (Summarize _ _) vtm = do
        (vtm2, _) <- lift get
        let keysset = keysSet vtm
            keysset2 = keysSet vtm2
            overlap = filter (\ key -> not (isOutputType (fromMaybe (error "key not found") (lookup key vtm))) || not (isInputType (fromMaybe (error "key not found") (lookup key vtm2))) ) (keysset /\ keysset2)
        when (not (null overlap)) $ throwError ("typecheck: reassignment of variable(s) " ++ show (map (\key -> (key, lookup key vtm, lookup key vtm2)) (toAscList overlap)))
        lift $ modify (const (unionWith (const id) vtm vtm2) *** id)
combineAggVtm  Exists vtm =
      lift $ modify (const vtm *** id)
combineAggVtm  Not vtm =
      lift $ modify (const vtm *** id)
combineAggVtm  (OrderByDesc _) _ = 
      return ()
combineAggVtm  (OrderByAsc _) _ = 
      return ()
combineAggVtm  Distinct _ =
      return ()
combineAggVtm  (Limit n) _ = 
      return ()

instance Typecheck Formula FormulaT where
    typecheck  (FAtomic a) = do
      () <- typecheck  a
      (vtm, _) <- lift get
      let vars = freeVars a
      let vtm' = filterWithKey (const . (`member` vars)) vtm
      return (Annotated vtm' (FAtomic0 a))
    typecheck  (FInsert l) = do
      (vtm, _) <- lift get
      let vars = freeVars l
      let ub = unbounded vtm vars
      unless (null ub) $ throwError ("typecheck: insert unbounded vars " ++ intercalate ", " (map serialize (Set.toAscList ub)) ++ " dvars = " ++ show vtm )
      () <- typecheck  l
      (vtm, _) <- lift get
      let vtm' = filterWithKey (const . (`member` vars)) vtm
      return (Annotated vtm' (FInsert0 l))
    typecheck  (FSequencing a b) = do
        a'@(Annotated vtma _) <- typecheck  a
        b'@(Annotated vtmb _) <- typecheck  b
        return (Annotated (vtma `union` vtmb) (FSequencing0 a' b'))
    typecheck  (FChoice a b) = do
        a'@(Annotated vtma _) <- typecheck  a
        b'@(Annotated vtmb _) <- typecheck  b
        return (Annotated (vtma `union` vtmb) (FChoice0 a' b'))
    typecheck  (FPar a b) = do
        a'@(Annotated vtma _) <- typecheck  a
        b'@(Annotated vtmb _) <- typecheck  b
        return (Annotated (vtma `union` vtmb) (FPar0 a' b'))
    typecheck  FZero = do
        (vtm, _) <- lift get
        return (Annotated empty FZero0)
    typecheck  FOne = do
        (vtm, _) <- lift get
        return (Annotated empty FOne0)
    typecheck  (Aggregate agg form) = do
        (vtm, _) <- lift get
        form' <- typecheck form
        () <- typecheck agg
        () <- combineAggVtm agg vtm
        (vtm', _) <- lift get
        -- let vars = freeVars agg
        -- let vtm' = filterWithKey (const . (`member` vars)) vtm
        return (Annotated vtm' (Aggregate0 agg form'))

isOutputType (ParamType _ _ t _) = t
isInputType (ParamType _ t _ _) = t

setToMap :: Set Var -> Set Var -> (VarTypeMap, VarTypeMap)
setToMap vars vars2 =
  let
    varsi = vars \\ vars2
    vars2i = vars2 /\ vars
    vars2o = vars2 \\ vars
    tvsi = map (ParamType False True False . TypeVar . show) [1..length varsi]
    tvs2i = map (ParamType False True False . TypeVar. show) [length varsi + 1..length varsi + length vars2i]
    tvs2o = map (ParamType False False True . TypeVar. show) [length vars + 1..length vars + length vars2o]
    varsti = Map.fromList (zip (toAscList varsi) tvsi)
    varst2i = Map.fromList (zip (toAscList vars2i) tvs2i)
    varst2o = Map.fromList (zip (toAscList vars2o) tvs2o)
    varstinp = Map.union varsti varst2i
    varstout = Map.union varst2i varst2o
  in
    (varstinp, varstout)

setToMap2 :: Map Var CastType -> Set Var -> (VarTypeMap, VarTypeMap)
setToMap2 vars vars2 =
  let
    (vars2i, varsi) = Map.partitionWithKey (\k _ -> k `elem` vars2) vars
    vars2o = vars2 \\ keysSet vars
    tvs2o = map (ParamType False False True . TypeVar. show) [1..length vars2o]
    varsti = Map.map (ParamType False True False) varsi
    varst2i = Map.map (ParamType False True False) vars2i
    varst2o = Map.fromList (zip (toAscList vars2o) tvs2o)
    varstinp = Map.union varsti varst2i
    varstout = Map.union varst2i varst2o
  in
    (varstinp, varstout)

typeCheckFormula :: PredTypeMap -> VarTypeMap -> Formula -> Either String (Formula1 Tie (Annotated VarTypeMap))
typeCheckFormula ptm vtm form = runNew (runReaderT (evalStateT (runEitherT (do
                                      initTCMonad vtm
                                      qu' <- typecheck form
                                      (_, tvm) <- lift $ get
                                      return (tcsubst tvm qu')
                                      )) (mempty, mempty)) ptm)
