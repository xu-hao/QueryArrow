{-# LANGUAGE TypeFamilies, MultiParamTypeClasses, FlexibleContexts, ExistentialQuantification, FlexibleInstances, OverloadedStrings #-}
module Prover.Parser where

import FO.Data

import Data.Map.Strict (Map, (!), member, insert)
import Control.Applicative ((<$>), (<*>))
import qualified Codec.TPTP.Base as TPTPB
import qualified Codec.TPTP as TPTP
import Data.Ratio
import Data.Convertible
import Data.Functor.Identity
import Control.Monad.State (get, State, evalState)
import Codec.TPTP.Pretty
import Codec.TPTP.Export

type ConvEnv a = State PredMap a
type Input = (String , String, PureFormula)

instance Convertible TPTP.TPTP_Input (ConvEnv Input) where
    safeConvert (TPTP.AFormula (TPTP.AtomicWord name) (TPTP.Role role) formula _) = Right ((\ x -> (name, role, x)) <$> convert formula)

instance Convertible (TPTP.F Identity) (ConvEnv PureFormula) where
    safeConvert f = safeConvert (runIdentity (TPTP.runF f))

instance Convertible  (TPTP.T Identity) (ConvEnv Expr) where
    safeConvert t = safeConvert (runIdentity (TPTP.runT t))

instance (Convertible t (ConvEnv Expr), Convertible f (ConvEnv PureFormula)) => Convertible (TPTPB.Formula0 t f) (ConvEnv PureFormula) where
    safeConvert (TPTP.BinOp f1 (TPTP.:<=>:) f2) =
        Right ((<-->) <$> convert f1 <*> convert f2)
    safeConvert (TPTP.BinOp f1 (TPTP.:=>:) f2) =
        Right ((-->) <$> convert f1 <*> convert f2)
    safeConvert (TPTP.BinOp f1 (TPTP.:&:) f2) =
        Right ((&) <$> convert f1 <*> convert f2)
    safeConvert (TPTP.BinOp f1 (TPTP.:|:) f2) =
        Right ((|||) <$> convert f1 <*> convert f2)
    safeConvert (TPTP.Quant TPTP.All vs f) =
        Right (foldr (\(TPTP.V v) f -> Forall (Var v) f) <$> convert f <*> return vs)
    safeConvert (TPTP.Quant TPTP.Exists vs f) =
        Right (foldr (\(TPTP.V v) f -> Exists (Var v) f) <$> convert f <*> return vs)
    safeConvert (TPTP.InfixPred t1 (TPTP.:=:) t2) =
        Right ((eqPred @@) <$> sequence [convert t1, convert t2])
    safeConvert (TPTP.InfixPred t1 (TPTP.:!=:) t2) =
        Right ((Not . (eqPred @@)) <$> sequence [convert t1, convert t2])
    safeConvert (TPTP.PredApp (TPTP.AtomicWord pred) terms) = Right (do
        pmap <- get
        if pred `member` pmap
            then do
                let p = pmap ! pred
                (p @@) <$> mapM convert terms
            else
                error "undefined predicate")
    safeConvert ((TPTP.:~:) f) = Right (Not <$> convert f)

instance Convertible t (ConvEnv Expr) => Convertible (TPTP.Term0 t) (ConvEnv Expr) where
    safeConvert (TPTP.Var (TPTP.V v)) = Right (return (VarExpr (Var v)))
    safeConvert (TPTP.NumberLitTerm ratio) =
        if denominator ratio /= 1
            then error "fraction not supported"
            else Right (return (IntExpr (fromIntegral (numerator ratio))))
    safeConvert (TPTP.DistinctObjectTerm s) = Right (return (StringExpr s))

parseTPTP :: PredMap -> String -> [Input]
parseTPTP pm s = evalState (mapM convert (TPTP.parse s)) pm

toTPTP3' :: [Input ]->String
toTPTP3' = toTPTP' . (convert :: [Input] -> [TPTP.TPTP_Input])

toTPTP3 :: [PureFormula ] -> PureFormula -> String
toTPTP3 rules formula =
    toTPTP3' (map (\(i, formula) ->
        ("rule" ++ show i, "axiom", formula)) (zip [1..length rules] rules) ++ [("goal", "conjecture", formula)])


prettyPrint :: [Input]->String
prettyPrint = prettySimple . (convert :: [Input] -> [TPTP.TPTP_Input])

instance Convertible Input TPTP.TPTP_Input where
    safeConvert (name, role, formula) =
        Right ( TPTP.AFormula (TPTP.AtomicWord name) (TPTP.Role role) (convert (foldr Forall formula (freeVars formula))) TPTP.NoAnnotations)

instance Convertible PureFormula TPTP.Formula where
    safeConvert (Atomic a) = Right (convert a)
    safeConvert (Disjunction (Not a) b) =
        Right (convert a TPTP..=>. convert b)
    safeConvert (Conjunction (Disjunction (Not a) b) (Disjunction (Not c) d)) | a == d && b == c =
        Right (convert a TPTP..<=>. convert b)
    safeConvert (Conjunction a b) = do
        a' <- safeConvert a
        b' <- safeConvert b
        Right (a' TPTP..&. b')
    safeConvert (CTrue) =
        Right (TPTP.numberLitTerm 1 TPTP..=. TPTP.numberLitTerm 1)
    safeConvert (Disjunction a b) = do
        a' <- safeConvert a
        b' <- safeConvert b
        Right (a' TPTP..|. b')
    safeConvert (CFalse) =
        Right (TPTP.numberLitTerm 1 TPTP..!=. TPTP.numberLitTerm 1)
    safeConvert (Not a) = do
        a' <- safeConvert a
        return ((TPTP..~.) (a'))
    safeConvert (Exists v a) = Right (TPTP.exists (map convert [ v]) (convert a))
    safeConvert (Forall v a) = Right (TPTP.for_all (map convert [ v]) (convert a))

instance Convertible Atom TPTP.Formula where
    safeConvert (Atom p@(Pred pred _) args) =
       Right( if p == eqPred
          then convert (head args) TPTP..=. convert (args !! 1)
          else TPTP.pApp (TPTP.AtomicWord pred) (map convert args))

instance Convertible Var TPTP.V where
    safeConvert (Var v) = Right (TPTP.V ("V"++v))

instance Convertible Expr TPTP.Term where
    safeConvert (VarExpr v) = Right (TPTP.var (convert v))
    safeConvert (IntExpr i) = Right (TPTP.numberLitTerm (fromIntegral i))
    safeConvert (StringExpr s) = Right (TPTP.distinctObjectTerm s)

instance Convertible a b => Convertible [a] [b] where
    safeConvert = mapM safeConvert
