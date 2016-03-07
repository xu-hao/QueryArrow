{-# LANGUAGE MultiParamTypeClasses, FlexibleInstances #-}
module FO.FunSat where

import FO.Data

-- import qualified Funsat.Types as FT
-- import qualified Funsat.Solver as FS
import qualified Data.Array.IArray as IArray
import qualified Data.Set as Set
import Data.Convertible.Base
import qualified Data.Map.Strict as Map
import Data.Map.Strict (Map, empty, foldlWithKey, insert, lookup)
import Control.Monad.Trans.State.Strict (get, put, State, runState   )
import Prelude  hiding (lookup)
import Control.Applicative ((<$>))
import Control.Monad.Logic
import Control.Monad.Plus

-- funsat doesn't compile consider using a first-order theorem prover

type ConvEnv a = State (Int, Map Atom Int) a
instance Convertible Atom (ConvEnv Int) where
    safeConvert a = Right (do
        (i, m) <- get
        case lookup a m of
            Nothing -> do
                put (i+1, insert a i m)
                return i
            Just j ->
                return j) -- assume that the atom is a ground atom

--instance Convertible Lit (ConvEnv FT.Lit) where
--    safeConvert (Lit Pos a) = Right (FT.L <$> convert a)
--    safeConvert (Lit Neg a) = Right (FT.L <$> negate <$> convert a)

--instance Convertible Rule (ConvEnv FT.Clause) where
--    safeConvert rule = Right (mapM convert (Set.toList rule))

--convertToCounterExample :: FT.IAssignment -> Map Atom Int -> CounterExample
--convertToCounterExample  iassignment = foldlWithKey (\ce atom i -> let assignment = iassignment IArray.! FT.V i in
--    insert atom (if assignment == 0 then Nothing else Just (assignment == i)) ce) empty

type CounterExample = Map Atom (Maybe Bool)
type Example = CounterExample

-- the funsat functions
valid :: Formula -> Maybe CounterExample
valid = \x -> Nothing -- do not call funsat for now
-- falsifiable

--falsifiable :: Formula -> Maybe Example
--falsifiable formula = satisfiable (Not formula)

--satisfiable :: Formula -> Maybe Example
--satisfiable formula =
--    let clauses = toCNF (pushNegations Pos formula)
--        consts0 = gatherConstants clauses
--        consts = if Set.null consts0 then Set.singleton (IntExpr 1) else consts0
--        instances = observeAll (msum (map (instantiate (mfromList (Set.toList consts))) (Set.toList clauses)))
--        (solverclauses0, (_, atomintmap)) = runState (mapM convert instances) (1 :: Int, empty :: Map Atom Int)
--        solverclauses = Set.fromList solverclauses0
--        cnf = FT.CNF (Map.size atomintmap) (Set.size solverclauses) solverclauses
--        (solution, stats, resolutiontrace) = FS.solve1 cnf in
--        case solution of
--            FT.Sat assignment -> Just (convertToCounterExample  assignment atomintmap)
--            FT.Unsat assignment -> Nothing
