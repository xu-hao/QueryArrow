{-# LANGUAGE MultiParamTypeClasses, TypeFamilies,
  FlexibleInstances, TypeSynonymInstances, FlexibleContexts #-}

-- iRODS genquery library
-- http://www.irods.org
-- github: https://github.com/irods/irods

module QueryArrowList where

import Control.Arrow
import Control.Category
import Control.Monad
import Data.Foldable
import qualified Data.Map as Map
import qualified Data.List as List
import Control.Monad.State hiding (lift)
import Prelude hiding (id,(.), concat, foldr)
import QueryArrow

-- A concrete example

-- Graph
type CGLabel = String
type CGVertex = String
type CGEdge = (CGVertex, CGLabel, CGVertex)
type CGEdgeMap = Map.Map CGLabel [CGEdge]
type CGraph = Map.Map CGVertex CGEdgeMap

type GraphQueryMonad = State CGraph

type CQuery = Kleisli GraphQueryMonad

getEdges :: VPath CGraph -> (CGEdgeMap -> [CGEdge]) -> (CGVertex -> CGEdge -> Bool) -> GraphQueryMonad [EPath CGraph]
getEdges vp getEdgeList filterEdge = do
    g <- get
    let v = (case vp of VLeaf v0 -> v0
                        VCons v0 _ -> v0)
        em = g Map.! v
        es = getEdgeList em
        eins = filter (filterEdge v) es
        eps = map (`ECons` vp) eins in
        return eps

instance Graph CGraph CQuery [] where
    type Vertex CGraph = CGVertex
    type Edge CGraph = CGEdge
    type Label CGraph = CGLabel
    data VPath CGraph = VLeaf (Vertex CGraph) | VCons (Vertex CGraph) (EPath CGraph) deriving Show
    data EPath CGraph = ECons (Edge CGraph) (VPath CGraph) deriving Show
    inE =  Kleisli (\ vp -> getEdges vp concat (\ v (_, _, vin) -> vin == v)) -- fmap is for Identity
    linE l = Kleisli (\ vp -> getEdges vp ( \ em -> if Map.member l em then em Map.! l else [] ) (\ v (_, _, vin) -> vin == v))
    outE = Kleisli (\ vp -> getEdges vp concat (\ v (vout, _, _) -> vout == v))
    loutE l = Kleisli (\ vp -> getEdges vp (\ em -> if Map.member l em then em Map.! l else [] ) (\ v (vout, _, _) -> vout == v))
    inV = Kleisli (\ p -> case p of (ECons (vin, _, _) _) -> return (VCons vin p))
    outV = Kleisli (\ p -> case p of (ECons (_, _, vout) _) -> return (VCons vout p))
    vpath = start . VLeaf


instance Query CQuery where
    start a = arr (const a)

instance Injectable CQuery [] where
    inj = Kleisli (\ a -> return [a])
     
instance Monoidal CQuery [EPath CGraph] [EPath CGraph] [EPath CGraph] where
    aplus (Kleisli a) (Kleisli b) = Kleisli (\ x -> do
        as <- a x
        bs <- b x
        return (as ++ bs))

instance Growable CQuery [] where
    grow (Kleisli f) = Kleisli (mapM f) 

instance Multiplicable CQuery [] [a] [a] where
    mtimes = arr concat
--    (CQuery f) &++ (CQuery g) = CQuery (\ as -> (++) <$> f as <*> g as)

data Count l a = Count Int (l a) deriving Show
instance Countable CQuery [] Count where
    qcount = arr (\ as -> Count (length as) as)
    qlimit n = arr (take n)

instance Sortable CQuery [] where
    qsort (Kleisli f) = Kleisli (\ as -> do
        s <- get
        return (List.sortBy (\ x y -> fst (runState (f (x,y)) s)) as))

data Group (l :: * -> * ) key d = Group key d deriving Show
instance Groupable CQuery [] (VPath CGraph) Group CGLabel where
    qgroup (Kleisli extractKey) = Kleisli grp >>> arr Map.toList >>> arr (fmap (uncurry Group)) where
        grp = foldrM f Map.empty where
            f a m = do
                v <- extractKey a
                return (if Map.member v m then Map.adjust (++[a]) v m else Map.insert v [a] m)
                
    gmap (Kleisli f)  = Kleisli (\ (Group l as) -> do
        c <- f as
        return (Group l c))

instance Filterable CQuery [] where
    qfilter (Kleisli f) = Kleisli (filterM f) 

runCQuery :: CQuery a b -> CGraph -> a -> (b, CGraph)
runCQuery (Kleisli f) g a = runState (f a) g