{-# LANGUAGE TypeSynonymInstances, FlexibleInstances #-}

module QueryArrow.FO.Types where

import Prelude hiding (lookup)
import Data.Map.Strict (Map, lookup, insert, delete, fromList, keysSet, unionWith)
import qualified Data.Map.Strict as Map
import Data.List (foldl', intercalate)
import Data.Set (Set, (\\), isSubsetOf)
import qualified Data.Set as Set
import QueryArrow.FO.Data
import QueryArrow.FO.Domain
import Control.Monad.Trans.Either
import Control.Monad.Trans.State
import Control.Monad.Trans.Reader
import Control.Monad.Except
import Control.Arrow ((***))
import Control.Monad (zipWithM_)
import Algebra.Lattice ((\/))
import Data.Maybe (fromMaybe)

type VarTypeMap = Map Var ParamType
type TVarMap = Map String CastType

type TCMonad = EitherT String (StateT (VarTypeMap, TVarMap) (ReaderT PredTypeMap NewEnv))
class Typecheck a where
  typecheck :: a -> TCMonad ()

initTCMonad :: [ParamType] -> [Var] -> TCMonad ()
initTCMonad = zipWithM_ (\pt var -> do
      lift $ modify (insert var pt *** id))

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
  tcsubst _ NumberType = NumberType
  tcsubst _ TextType = TextType
  tcsubst m tv@(TypeVar v) = fromMaybe tv (lookup v m)

instance TCSubst PredType where
  tcsubst m (PredType pk pts) = PredType pk (map (tcsubst m) pts)

instance TCSubst ParamType where
  tcsubst m (ParamType a b c t) = ParamType a b c (tcsubst m t)

instance TCSubst b => TCSubst (Map a b) where
  tcsubst  m tvm = Map.map (tcsubst m) tvm

class FreeTypeVars a where
  freeTypeVars :: a -> Set String

instance FreeTypeVars CastType where
  freeTypeVars NumberType = mempty
  freeTypeVars TextType = mempty
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
        context ("checkVarType: " ++ serialize v ++ " expected and encountered") $ tcunify t t2
        (_,tvm) <- lift get
        let t' = tcsubst tvm t
        lift $ modify (insert v (ParamType pk True False t') *** id)

unbounded :: VarTypeMap -> Set Var -> Set Var
unbounded vtm  = Set.filter (\var -> case lookup var vtm of
                                        Nothing -> True
                                        Just (ParamType _ False _ _) -> True
                                        _ -> False)

instantiatePredType :: PredType -> TCMonad PredType
instantiatePredType pt = do
    let freetvs = Set.toAscList (freeTypeVars pt)
    newtvs <- lift . lift . lift $ new (map StringWrapper freetvs)
    let s = fromList (zip freetvs (map TypeVar newtvs))
    return (tcsubst s pt)

instance Typecheck Atom where
  typecheck a@(Atom pn args) = do
      (vtm, _) <- lift get
      predtypemap <- lift . lift $ ask
      case lookup pn predtypemap of
          Nothing -> throwError ("typecheck: cannot find predicate " ++ show pn)
          Just pt -> do
              (PredType _ pts) <- instantiatePredType pt
              if length pts /= length args
                  then throwError ("typecheck: argument count mismatch " ++ show a)
                  else
                      zipWithM_ (\paramtype arg -> do
                          let free = freeVars arg
                          let ub = unbounded vtm free
                          when (not (isVar arg) && not (null ub)) $ throwError ("typecheck: unbounded nested vars: " ++ show ub ++ ", the formula is " ++ show a)
                          (_, tvm) <- lift get
                          let pt'@(ParamType _ _ _ t') = tcsubst tvm paramtype
                          case arg of
                              VarExpr v ->
                                  context ("typecheck: " ++ serialize a ++ " arg " ++ serialize arg) $ checkVarType v pt'
                              IntExpr _ ->
                                  context ("typecheck: " ++ serialize a ++ " arg " ++ serialize arg) $ tcunify t' NumberType
                              StringExpr _ ->
                                  context ("typecheck: " ++ serialize a ++ " arg " ++ serialize arg) $ tcunify t' TextType
                              PatternExpr _ ->
                                  context ("typecheck: " ++ serialize a ++ " arg " ++ serialize arg) $ tcunify t' TextType
                              CastExpr t2 _ ->
                                  context ("typecheck: " ++ serialize a ++ " arg " ++ serialize arg ++ " paramtype " ++ show paramtype) $ tcunify t' t2
                              NullExpr ->
                                  return ()) pts args

instance Typecheck Lit where
    typecheck  (Lit _ a) = typecheck  a

instance Typecheck Aggregator where
  typecheck  (Summarize cols _) = do
    vtml <- mapM (\(v, s) ->
      case s of
        Max v2 -> do
            checkVarType v2 (ParamType False True False NumberType)
            return (v, ParamType False True False NumberType)
        Min v2 -> do
            checkVarType v2 (ParamType False True False NumberType)
            return (v, ParamType False True False NumberType)
        Average v2 -> do
            checkVarType v2 (ParamType False True False NumberType)
            return (v, ParamType False True False NumberType)
        Sum v2 -> do
            checkVarType v2 (ParamType False True False NumberType)
            return (v, ParamType False True False NumberType)
        Count ->
            return (v, ParamType False True False NumberType)
        CountDistinct _ ->
            return (v, ParamType False True False NumberType)
      ) cols
    let vtm = fromList vtml
    lift $ modify (const vtm *** id)
  typecheck  Exists =
      lift $ modify (const mempty *** id)
  typecheck  Not =
      lift $ modify (const mempty *** id)
  typecheck  (OrderByDesc _) = return ()
  typecheck  (OrderByAsc _) = return ()
  typecheck  Distinct = return ()
  typecheck  (Limit _) = return ()

instance Typecheck Formula where
    typecheck  (FAtomic a) = typecheck  a
    typecheck  (FInsert l) = do
      (vtm, _) <- lift get
      let vars = freeVars l
      let ub = unbounded vtm vars
      unless (null ub) $ throwError ("typecheck: insert unbounded vars " ++ intercalate ", " (map serialize (Set.toAscList ub)) ++ " dvars = " ++ show vtm )
      typecheck  l
    typecheck  (FReturn vars) = do
      (vtm, _) <- lift get
      let rvars = Set.fromList vars
      let ub = unbounded vtm rvars
      unless (null ub) $ throwError ("typecheck: return unbounded vars " ++ intercalate ", " (map serialize (Set.toAscList ub)) ++ " dvars = " ++ show vtm )
      let vtm' = foldl' (\vtm' var -> delete var vtm') vtm vars
      lift $ modify (const vtm' *** id)
    typecheck  (FSequencing a b) = do
        typecheck  a
        typecheck  b
    typecheck  (FChoice a b) = typecheck  a >> typecheck  b
    typecheck  (FPar a b) = typecheck  a >> typecheck  b
    typecheck  FZero = return ()
    typecheck  FOne = return ()
    typecheck  FTransaction = return ()
    typecheck  (Aggregate agg form) = do
        (vtm, _) <- lift get
        typecheck form
        typecheck agg
        (vtm2, _) <- lift get
        lift $ modify (const (unionWith (const id) vtm vtm2) *** id)
