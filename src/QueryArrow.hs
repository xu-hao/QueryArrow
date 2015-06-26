{-# LANGUAGE MultiParamTypeClasses, FunctionalDependencies, TypeFamilies, FlexibleInstances, TypeSynonymInstances #-}

-- iRODS 4.2 genquery library
-- http://www.irods.org
-- github: https://github.com/irods/irods

module QueryArrow where

import Control.Arrow
import Control.Category
import Control.Monad
import Control.Applicative
import Data.Foldable
import qualified Data.Map as Map
import qualified Data.List as List
import Control.Monad.State hiding (lift)
import Prelude hiding (id,(.), concat, foldr)
import qualified Data.Function

-- abstract framework
-- Query
infix 3 &++

class (Arrow q, Monad n) => Query q n m | q -> n m where
    grow ::(a -> n (m b)) -> q a b
    transform :: (m a -> m b) -> q a b
    (&++) :: q a b -> q a b -> q a b

class Monad n => Graph g n m | g -> n m where
    type Vertex g
    type Edge g
    type Label g
    data VPath g -- injectivity
    data EPath g
    inE :: VPath g -> n (m (EPath g))
    outE :: VPath g -> n (m (EPath g))
    inV :: EPath g -> n (m (VPath g))
    outV :: EPath g -> n (m (VPath g))
    linE :: Label g -> VPath g -> n (m (EPath g))
    loutE :: Label g -> VPath g -> n (m (EPath g))
    vpath :: Vertex g -> m (VPath g)

class Filterable m where
    qfilter :: (a-> Bool) -> m a -> m a

class Groupable m where
    type Key m a
    data Group m a -- make sure this is injective
    qgroup :: (a -> Key m a) -> m a -> m (Group m a)
    gmap :: (m a -> m b) -> m (Group m a) -> m (Group m b)

class Countable m where
    type Count m a
    qcount :: m a -> m (Count m a)
    qlimit :: Int -> m a -> m a

class Sortable m where
    qsort :: (Ord k)=> (a -> k) -> m a -> m a

start :: (Query q n m, Graph g n m) => Vertex g -> q a (VPath g)
start a = transform (\ _ -> vpath a)

add :: (Query q g m, Functor m) => a -> q a a
add a = arr id &++ transform (a <$)

trim :: (Query q g m, Filterable m) => (a -> Bool) -> q a a
trim f = transform (qfilter f)

group :: (Query q g m, Groupable m) => (a -> Key m a) -> q a (Group m a)
group f = transform (qgroup f)

count :: (Query q g m, Countable m) => q a (Count m a)
count = transform qcount

limit :: (Query q g m, Countable m) => Int -> q a a
limit n = transform ( qlimit n )

groupCount :: (Query q g m, Countable m, Groupable m, Functor m) => (a -> Key m a) -> q a (Group m (Count m a))
groupCount f = group f >>> transform ( gmap qcount )

sort :: (Query q g m, Sortable m, Ord k) => ( a -> k) -> q a a
sort f = transform (qsort f)

selectInE :: (Query q n m, Graph g n m) => Label g -> q (VPath g) (EPath g)
selectInE l = grow (linE l)

selectOutE :: (Query q n m, Graph g n m) => Label g -> q (VPath g) (EPath g)
selectOutE l = grow (loutE l)

selectInV :: (Query q n m, Graph g n m) => q (EPath g) (VPath g)
selectInV = grow inV

selectOutV :: (Query q n m, Graph g n m) => q (EPath g) (VPath g)
selectOutV = grow outV

selectE :: (Query q n m, Graph g n m) => Label g -> q (VPath g) (EPath g)
selectE l = selectInE l &++ selectOutE l

selectEV :: (Query q n m, Graph g n m) => Label g -> q (VPath g) (VPath g)
selectEV l = (selectInE l >>> selectOutV) &++ (selectOutE l >>> selectInV)

startV :: (Query q n m, Graph g n m) => Vertex g -> q a (VPath g)
startV v = transform (\ _ -> vpath v)


-- A concrete example

-- Graph
type CGLabel = String
type CGVertex = String
type CGEdge = (CGVertex, CGLabel, CGVertex)
type CGraph = Map.Map CGVertex (Map.Map CGLabel [CGEdge])

type MGraph = State CGraph

getEdges :: VPath CGraph -> (Map.Map CGLabel [CGEdge] -> [CGEdge]) -> (CGVertex -> CGEdge -> Bool) -> MGraph [EPath CGraph]
getEdges vp getEdgeList filterEdge = do
    g <- get
    let v = (case vp of VLeaf v0 -> v0
                        VCons v0 _ -> v0)
        em = g Map.! v
        es = getEdgeList em
        eins = filter (filterEdge v) es
        eps = map (`ECons` vp) eins in
        return eps

instance Graph CGraph MGraph [] where
    type Vertex CGraph = CGVertex
    type Edge CGraph = CGEdge
    type Label CGraph = CGLabel
    data VPath CGraph = VLeaf (Vertex CGraph) | VCons (Vertex CGraph) (EPath CGraph) deriving Show
    data EPath CGraph = ECons (Edge CGraph) (VPath CGraph) deriving Show
    inE vp = getEdges vp concat (\ v (_, _, vin) -> vin == v)
    linE l vp = getEdges vp ( \ em -> if Map.member l em then em Map.! l else [] ) (\ v (_, _, vin) -> vin == v)
    outE vp = getEdges vp concat (\ v (vout, _, _) -> vout == v)
    loutE l vp = getEdges vp (\ em -> if Map.member l em then em Map.! l else [] ) (\ v (vout, _, _) -> vout == v)
    inV p = case p of (ECons (vin, _, _) _) -> return [VCons vin p]
    outV p = case p of (ECons (_, _, vout) _) -> return [VCons vout p]
    vpath v = return (VLeaf v)

data CQuery a b = CQuery {unCQuery :: [a] -> MGraph [b]}

instance Category CQuery where
    id = CQuery return
    CQuery f . CQuery g = CQuery (g >=> f)

instance Arrow CQuery where
    arr f = CQuery (return . fmap f)
    first (CQuery f) = CQuery (\ acs ->
        let (as, cs) = unzip acs in do
            bs <- f as
            return (zip bs cs))

instance Query CQuery MGraph [] where
    grow f = CQuery (fmap concat . Prelude.mapM f)
    transform f = CQuery (return . f)
    (CQuery f) &++ (CQuery g) = CQuery (\ as -> (++) <$> f as <*> g as)

instance Countable [] where
    type Count [] a = (Int, [a])
    qcount as = [(length as, as)]
    qlimit = take

instance Sortable [] where
    qsort f = List.sortBy (compare `Data.Function.on` f)

instance Groupable [] where
    type Key [] a = CGVertex
    data Group [] a = Group { unGroup :: (CGLabel, [a]) } deriving Show
    qgroup extractKey as = fmap Group (Map.toList grp) where
        grp = foldr f Map.empty as where
            f a m = if Map.member v m then Map.adjust (++[a]) v m else Map.insert v [a] m where
                v = extractKey a
    gmap f = fmap (\ (Group (l, as)) -> Group (l, f as))

instance Filterable [] where
    qfilter = filter
