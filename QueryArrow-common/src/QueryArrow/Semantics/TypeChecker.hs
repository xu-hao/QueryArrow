{-# LANGUAGE TypeSynonymInstances, FlexibleInstances, MultiParamTypeClasses, StandaloneDeriving #-}

module QueryArrow.Semantics.TypeChecker where

import Prelude hiding (lookup, null, filter)
import Data.Map.Strict (Map, lookup, insert, delete, fromList, keysSet, unionWith, filterWithKey, union, empty, mapKeys, toList, singleton)
import qualified Data.Map.Strict as Map
import Data.List (foldl', intercalate)
import Data.Set (Set, (\\), toAscList, member, null, filter)
import qualified Data.Set as Set
import QueryArrow.Syntax.Term
import QueryArrow.Semantics.Domain
import Control.Monad.Trans.Except
import Control.Monad.Trans.State
import Control.Monad.Trans.Reader
import Control.Monad.Except
import Control.Arrow ((***))
import Control.Monad (zipWithM_)
import Control.Comonad.Cofree
import Data.Maybe (fromMaybe)
import Algebra.Lattice
import QueryArrow.Syntax.Type
import QueryArrow.Syntax.Serialize

type VarTypeMap = Map Var ParamType
type TVarMap = Map String CastType

type TCMonad = ExceptT String (StateT (VarTypeMap, TVarMap) (ReaderT PredTypeMap NewEnv))
type FormulaT = Formula2 () VarTypeMap

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
  tcunify (TypeApp a b) (TypeApp a2 b2) = do
    tcunify a a2
    tcunify b b2
  tcunify s@(TypeUniv _ _) t@(TypeUniv _ _) =
    throwError ("unify: unsupported higher-order unification " ++ show s ++ " and " ++ show t)
  tcunify s t =
    throwError ("unify: type mismatch " ++ show s ++ " and " ++ show t)

class TCSubst a where
  tcsubst :: TVarMap -> a -> a

instance TCSubst CastType where
  tcsubst _ ty@(TypeCons _) = ty
  tcsubst m (TypeApp ty1 ty2) = TypeApp (tcsubst m ty1) (tcsubst m ty2)
  tcsubst m (TypeUniv tv ty) = TypeUniv tv (tcsubst (delete tv m) ty)
  tcsubst m tv@(TypeVar v) = fromMaybe tv (lookup v m)

instance TCSubst PredType where
  tcsubst m (PredType pk pts) = PredType pk (map (tcsubst m) pts)

instance TCSubst ParamType where
  tcsubst m (ParamType a b c d t) = ParamType a b c d (tcsubst m t)

instance TCSubst b => TCSubst (Map a b) where
  tcsubst m tvm = Map.map (tcsubst m) tvm

instance TCSubst FormulaT where
  tcsubst m (a :< f1) = (tcsubst m a) :< (fmap (tcsubst m) f1)

class FreeTypeVars a where
  freeTypeVars :: a -> Set String

instance FreeTypeVars CastType where
  freeTypeVars (TypeCons _) = mempty
  freeTypeVars (TypeApp ty1 ty2)  = freeTypeVars ty1 \/ freeTypeVars ty2
  freeTypeVars (TypeUniv tv ty)  = Set.delete tv (freeTypeVars ty)
  freeTypeVars (TypeVar v) = Set.singleton v

instance FreeTypeVars PredType where
  freeTypeVars (PredType _ pts) = Set.unions (map freeTypeVars pts)

instance FreeTypeVars ParamType where
  freeTypeVars (ParamType _ _ _ _ t) = freeTypeVars t

checkVarType :: Var -> ParamType -> TCMonad ()
checkVarType v (ParamType pk inp out _ t) = do
  (vartypemap, _) <- lift get
  case lookup v vartypemap of
    Nothing -> do
        unless out $ throwError ("checkVarType: unbounded var: " ++ show v)
        lift $ modify (insert v (ParamType pk True False False t) *** id)
    Just (ParamType _ inp2 out2 _ t2) -> do
        when (not inp && inp2) $ throwError ("checkVarType: bounded var: " ++ show v)
        when (not out && out2) $ throwError ("checkVarType: unbounded var: " ++ show v)
        t2' <- instantiate t2
        context ("checkVarType: " ++ serialize v ++ " " ++ show vartypemap ++ " expected and encountered") $ tcunify t t2'
        (_,tvm) <- lift get
        let t' = tcsubst tvm t
        lift $ modify (insert v (ParamType pk True False False t') *** id)

consTypeMap :: Map String CastType
consTypeMap = fromList [("(:)", TypeUniv "a" (TypeVar "a" `FuncType` (ListType (TypeVar "a") `FuncType` (ListType (TypeVar "a"))))),
                        ("[]", TypeUniv "a" (ListType (TypeVar "a")))]

checkConsType :: String -> ParamType -> TCMonad ()
checkConsType v (ParamType _ False True _ _) =
  throwError ("checkConsType: a constructor cannot be used as an output parameter: " ++ show v)
checkConsType v (ParamType _ _ _ _ t) = do
  case lookup v consTypeMap of
    Nothing ->
        throwError ("checkConsType: undefined constructor: " ++ show v)
    Just t2 -> do
        t2' <- instantiate t2
        context ("checkConsType: " ++ v ++ " " ++ show consTypeMap ++ " expected and encountered") $ tcunify t t2'

unbounded :: VarTypeMap -> Set Var -> Set Var
unbounded vtm  = Set.filter (\var0 -> case lookup var0 vtm of
                                        Nothing -> True
                                        Just (ParamType _ False _ _ _) -> True
                                        _ -> False)

newTVar :: String -> TCMonad String
newTVar s = lift . lift . lift $ new (StringWrapper s)

newTVars :: [String] -> TCMonad [String]
newTVars as = mapM newTVar as

instantiate :: CastType -> TCMonad CastType
instantiate (TypeUniv tv ty) = do
    tv' <- newTVar tv
    ty' <- instantiate ty
    return (tcsubst (singleton tv (TypeVar tv')) ty')
instantiate ty = return ty

instantiatePredType :: PredType -> TCMonad PredType
instantiatePredType pt = do
    let freetvs = Set.toAscList (freeTypeVars pt)
    newtvs <- newTVars freetvs
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
                          context ("typecheck: " ++ serialize a) $ typecheck (arg, paramtype)) pts args

instance Typecheck (Expr, ParamType) () where
  typecheck (arg, paramtype) = do
                  (_, tvm) <- lift get
                  let pt'@(ParamType _ _ _ _ t') = tcsubst tvm paramtype
                  case arg of
                              ConsExpr v ->
                                  context ("typecheck: arg " ++ serialize arg) $ checkConsType v pt'
                              VarExpr v ->
                                  context ("typecheck: arg " ++ serialize arg) $ checkVarType v pt'
                              IntExpr _ ->
                                  context ("typecheck: arg " ++ serialize arg) $ tcunify t' Int64Type
                              StringExpr _ ->
                                  context ("typecheck: arg " ++ serialize arg) $ tcunify t' TextType
                              AppExpr e1 e2 -> do
                                  paramType <- newTVar "paramType"
                                  let paramtv = TypeVar paramType
                                  context ("typecheck: arg " ++ serialize arg) $ typecheck (e1, FuncType paramtv t')
                                  context ("typecheck: arg " ++ serialize arg) $ typecheck (e2, paramtv)
                              CastExpr t2 _ ->
                                  context ("typecheck: arg " ++ serialize arg ++ " paramtype " ++ serialize paramtype) $ tcunify t' t2
                              NullExpr ->
                                  return ()

instance Typecheck (Expr, CastType) () where
  typecheck (arg, ty) = typecheck (arg, ParamType False True False False ty)

instance Typecheck Lit () where
    typecheck  (Lit s a) = typecheck  a

instance Typecheck Aggregator () where
  typecheck  (FReturn vars) = do
    (vtm, _) <- lift get
    let rvars = Set.fromList vars
    let ub = unbounded vtm rvars
    unless (null ub) $ throwError ("typecheck: return unbounded vars " ++ intercalate ", " (map serialize (Set.toAscList ub)) ++ " dvars = " ++ show vtm )
    let vtm' = foldl' (\vtm0 var0 -> insert var0 (fromMaybe (error "typecheck: cannot find bounded var") (lookup var0 vtm)) vtm0) mempty vars
    lift $ modify (const vtm' *** id)
  typecheck  (Summarize cols _) = do
    vtml <- mapM (\(Bind v s) ->
      case s of
        Max v2 -> do
            checkVarType v2 (ParamType False True False False Int64Type)
            return (v, ParamType False True False False Int64Type)
        Min v2 -> do
            checkVarType v2 (ParamType False True False False Int64Type)
            return (v, ParamType False True False False Int64Type)
        Average v2 -> do
            checkVarType v2 (ParamType False True False False Int64Type)
            return (v, ParamType False True False False Int64Type)
        Sum v2 -> do
            checkVarType v2 (ParamType False True False False Int64Type)
            return (v, ParamType False True False False Int64Type)
        Count ->
            return (v, ParamType False True False False Int64Type)
        CountDistinct _ ->
            return (v, ParamType False True False False Int64Type)
        Random v2 -> do
            ntv <- newTVar "random"
            checkVarType v2 (ParamType False True False False (TypeVar ntv))
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
      return (vtm' :< (FAtomicF a))
    typecheck  (FInsert l) = do
      (vtm, _) <- lift get
      let vars = freeVars l
      let ub = unbounded vtm vars
      unless (null ub) $ throwError ("typecheck: insert unbounded vars " ++ intercalate ", " (map serialize (Set.toAscList ub)) ++ " dvars = " ++ show vtm )
      () <- typecheck  l
      (vtm, _) <- lift get
      let vtm' = filterWithKey (const . (`member` vars)) vtm
      return (vtm' :< (FInsertF l))
    typecheck  (FSequencing a b) = do
        a'@(vtma :< _) <- typecheck  a
        b'@(vtmb :< _) <- typecheck  b
        return ((vtma `union` vtmb) :< (FSequencingF a' b'))
    typecheck  (FChoice a b) = do
        a'@(vtma :< _) <- typecheck  a
        b'@(vtmb :< _) <- typecheck  b
        return ((vtma `union` vtmb) :< (FChoiceF a' b'))
    typecheck  (FPar a b) = do
        a'@(vtma :< _) <- typecheck  a
        b'@(vtmb :< _) <- typecheck  b
        return ((vtma `union` vtmb) :< (FParF a' b'))
    typecheck  FZero = do
        (vtm, _) <- lift get
        return (empty :< FZeroF)
    typecheck  FOne = do
        (vtm, _) <- lift get
        return (empty :< FOneF)
    typecheck  (Aggregate agg form) = do
        (vtm, _) <- lift get
        form' <- typecheck form
        () <- typecheck agg
        () <- combineAggVtm agg vtm
        (vtm', _) <- lift get
        -- let vars = freeVars agg
        -- let vtm' = filterWithKey (const . (`member` vars)) vtm
        return (vtm' :< (AggregateF agg form'))

isOutputType (ParamType _ _ t _ _) = t
isInputType (ParamType _ t _ _ _) = t

setToMap :: Set Var -> Set Var -> (VarTypeMap, VarTypeMap)
setToMap vars vars2 =
  let
    varsi = vars \\ vars2
    vars2i = vars2 /\ vars
    vars2o = vars2 \\ vars
    tvsi = map (ParamType False True False False . TypeVar . show) [1..length varsi]
    tvs2i = map (ParamType False True False False . TypeVar. show) [length varsi + 1..length varsi + length vars2i]
    tvs2o = map (ParamType False False True False . TypeVar. show) [length vars + 1..length vars + length vars2o]
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
    tvs2o = map (ParamType False False True False . TypeVar. show) [1..length vars2o]
    varsti = Map.map (ParamType False True False False) varsi
    varst2i = Map.map (ParamType False True False False) vars2i
    varst2o = Map.fromList (zip (toAscList vars2o) tvs2o)
    varstinp = Map.union varsti varst2i
    varstout = Map.union varst2i varst2o
  in
    (varstinp, varstout)

typeCheckFormula :: PredTypeMap -> VarTypeMap -> Formula -> Either String FormulaT
typeCheckFormula ptm vtm form = runNew (runReaderT (evalStateT (runExceptT (do
                                      initTCMonad vtm
                                      qu' <- typecheck form
                                      (_, tvm) <- lift $ get
                                      return (tcsubst tvm qu')
                                      )) (mempty, mempty)) ptm)
