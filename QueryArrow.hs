{-# LANGUAGE MultiParamTypeClasses #-}
-- | Main entry point to the application.
module QueryArrow where

import Control.Arrow
import Control.Category
import Control.Monad
import Control.Applicative
import Data.Traversable
import Data.Foldable
import qualified Data.Map
import qualified Data.List
import Control.Monad.State hiding (lift)

import Prelude hiding (id,(.),foldr, concat)

-- generic arrows

-- TraversableArrow
-- Foldable, Functor => Traversable
data (Traversable f, Alternative f, Traversable g) => TraversableArrow f g a b c = TraversableArrow {unTraversableArrow :: a (f b) (g c)}

instance (Traversable f, Alternative f, Category a) => Category (TraversableArrow f f a) where
    id = TraversableArrow id
    TraversableArrow f . TraversableArrow g = TraversableArrow (f . g)

instance (Traversable f, Alternative f, Arrow a) => Arrow (TraversableArrow f f a) where
    arr f = TraversableArrow (arr (fmap f))
    first (TraversableArrow f) = TraversableArrow (arr tunzip >>> first f >>> arr (uncurry tzip)) where
        tunzip = foldr (\ (x, y) (xs, ys) -> (pure x <|> xs, pure y <|> ys)) (empty, empty)
        tzip :: (Traversable f, Alternative f) => f a -> f b -> f (a, b)
        tzip a b = foldr (<|>) empty (map pure (zip (toList a) (toList b)))

-- *** has to be commutative for genmap to be a functor
genmap :: (ArrowChoice a) => a b c -> a [b] [c]
genmap f = arr (\ xs ->
                case xs of [] -> Left []
                           (hd : tl) -> Right (hd, tl)) >>> (arr id ||| (f *** genmap f >>> arr (uncurry (:))))

genmapT :: (Traversable f, Alternative f, ArrowChoice a) => a b c -> a (f b) (f c)
genmapT f = arr toList >>> genmap f >>> arr (foldr (\ x xs -> pure x <|> xs) empty)

class (Arrow a, Arrow (f a)) => ArrowTransformer f a where
    lift :: a b c -> f a b c

instance (Traversable f, Alternative f, ArrowChoice a) => ArrowTransformer (TraversableArrow f f) a where
    lift f = TraversableArrow (genmapT f)

instance (Traversable f, Alternative f, ArrowChoice a, ArrowZero a) => ArrowZero (TraversableArrow f f a) where
    zeroArrow = lift zeroArrow

-- StateArrow
newtype StateArrow s a b c = StateArrow {runStateArrow :: a (b, s) (c, s)}

instance Category a => Category (StateArrow s a) where
    id = StateArrow id
    StateArrow f . StateArrow g = StateArrow (f . g)

instance Arrow a => Arrow (StateArrow s a) where
    arr f = StateArrow (first (arr f))
    first (StateArrow f) = StateArrow (arr swapsnd >>> first f >>> arr swapsnd) where
                                swapsnd ((a,b),c) = ((a,c),b)

class ArrowState s a where
    fetch :: a e s
    store :: a s ()

instance Arrow a => ArrowState s (StateArrow s a) where
    fetch = StateArrow (arr (\ (_, s) -> (s, s)))
    store = StateArrow (arr (\ (s, _) -> ((), s)))

instance Arrow a => ArrowTransformer (StateArrow s) a where
    lift f = StateArrow (first f)

stateArrowLeft :: ArrowChoice a => StateArrow s a b c -> StateArrow s a (Either b d) (Either c d)
stateArrowLeft (StateArrow f) = StateArrow (arr g >>> left f >>> arr h) where
    g (Left b, s) = Left (b, s)
    g (Right b, s) = Right (b, s)
    h (Left (b, s)) = (Left b, s)
    h (Right (b, s)) = (Right b, s)

instance ArrowChoice a => ArrowChoice (StateArrow s a) where
    left f = stateArrowLeft f

-- abstract framework
-- Query
infix 3 &++

class (Arrow q, Monad g, MonadPlus m) => Query q g m | q -> g m where
    grow ::(a -> g (m b)) -> q a b
    transform :: (m a -> m b) -> q a b
    (&++) :: q a b -> q a b -> q a b

class Monad g => Graph g m where
    type Vertex g
    type Edge g
    type Label g
    data VPath g -- injectivity
    data EPath g
    inE :: VPath g -> g (m (EPath g))
    outE :: VPath g -> g (m (EPath g))
    inV :: EPath g -> g (m (VPath g))
    outV :: EPath g -> g (m (VPath g))
    linE :: Label g -> VPath g -> g (m (EPath g))
    loutE :: Label g -> VPath g -> g (m (EPath g))
    vpath :: Vertex g -> m (VPath g)

class Filterable m where
    qfilter :: (a-> Bool) -> m a -> m a

class Groupable m where
    type Key m a
    data Group m a -- make sure this is injective
    qgroup :: (a -> Key m a) -> m a -> m (Group m (m a))
    gmap :: (a -> b) -> (Group m a) -> (Group m b)

class Countable m where
    type Count m a
    qcount :: m a -> (Count m a)
    qlimit :: Int -> m a -> m a

class Sortable m where
    type SortKey m a
    qsort :: (a -> SortKey m a) -> m a -> m a

start :: (Query q g m, Graph g m) => Vertex g -> q a (VPath g)
start a = transform (\ x -> vpath a)

add :: (Query q g m) => a -> q a a
add a = arr id &++ transform (\x->return a)

trim :: (Query q g m, Filterable m) => (a -> Bool) -> q a a
trim f = transform (qfilter f)

group :: (Query q g m, Groupable m) => (a -> Key m a) -> q a (Group m (m a))
group f = transform (qgroup f)

count :: (Query q g m, Countable m) => q a (Count m a)
count = transform ( return . qcount )

limit :: (Query q g m, Countable m) => Int -> q a a
limit n = transform ( qlimit n )

groupCount :: (Query q g m, Countable m, Groupable m, Functor m) => (a -> Key m a) -> q a (Group m (Count m a))
groupCount f = group f >>> transform ( fmap (gmap qcount) )

sort :: (Query q g m, Sortable m) => ( a -> SortKey m a) -> q a a
sort f = transform (qsort f)

selectInE :: (Query q g m, Graph g m) => Label g -> q (VPath g) (EPath g)
selectInE l = grow (linE l)

selectOutE :: (Query q g m, Graph g m) => Label g -> q (VPath g) (EPath g)
selectOutE l = grow (loutE l)

selectInV :: (Query q g m, Graph g m) => q (EPath g) (VPath g)
selectInV = grow (inV)

selectOutV :: (Query q g m, Graph g m) => q (EPath g) (VPath g)
selectOutV = grow (outV)

selectE :: (Query q g m, Graph g m) => Label g -> q (VPath g) (EPath g)
selectE l = selectInE l &++ selectOutE l

selectEV :: (Query q g m, Graph g m) => Label g -> q (VPath g) (VPath g)
selectEV l = (selectInE l >>> selectOutV) &++ (selectOutE l >>> selectInV)

startV :: (Query q g m, Graph g m) => Vertex g -> q a (VPath g)
startV v = transform (\ a -> vpath v)


-- A concrete example

-- Graph
type CGLabel = String
type CGVertex = String
type CGEdge = (CGVertex, CGLabel, CGVertex)
type CGraph = Data.Map.Map CGVertex (Data.Map.Map CGLabel [CGEdge])

type MGraph = State CGraph

getEdges vp getEdgeList filterEdge = do
    g <- get
    let v = (case vp of VLeaf v -> v
                        VCons v ep -> v)
        em = g Data.Map.! v
        es = getEdgeList em
        eins = filter (filterEdge v) es
        eps = map (\ ein -> ECons ein vp) eins in
        return eps

instance Graph MGraph [] where
    type Vertex MGraph = CGVertex
    type Edge MGraph = CGEdge
    type Label MGraph = CGLabel
    data VPath MGraph = VLeaf (Vertex MGraph) | VCons (Vertex MGraph) (EPath MGraph) deriving Show
    data EPath MGraph = ECons (Edge MGraph) (VPath MGraph) deriving Show
    inE vp = getEdges vp (foldr (++) []) (\ v (vout, l, vin) -> vin == v)
    linE l vp = getEdges vp ( \ em -> if Data.Map.member l em then em Data.Map.! l else [] ) (\ v (vout, l, vin) -> vin == v)
    outE vp = getEdges vp (foldr (++) []) (\ v (vout, l, vin) -> vout == v)
    loutE l vp = getEdges vp (\ em -> if Data.Map.member l em then em Data.Map.! l else [] ) (\ v (vout, l, vin) -> vout == v)
    inV p = case p of (ECons (vin, l, vout) vp) -> return [VCons vin p]
    outV p = case p of (ECons (vin, l, vout) vp) -> return [VCons vout p]
    vpath v = return (VLeaf v)

data CQuery a b = CQuery {unCQuery :: [a] -> MGraph [b]}

instance Category CQuery where
    id = CQuery return
    CQuery f . CQuery g = CQuery (g >=> f)

instance Arrow CQuery where
    arr f = CQuery (return . (fmap f))
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
    qcount as = (length as, as)
    qlimit n as = take n as

instance Sortable [] where
    type SortKey [] a = CGVertex
    qsort f as = Data.List.sortBy (\ a b -> compare (f a) (f b)) as

instance Groupable [] where
    type Key [] a = CGVertex  
    data Group [] a = Group { unGroup :: (CGLabel, a) } deriving Show
    qgroup extractKey as = fmap Group (Data.Map.toList grp) where
        grp = foldr f Data.Map.empty as where
            f a m = if Data.Map.member v m then Data.Map.adjust (++[a]) v m else Data.Map.insert v [a] m where
                v = extractKey a
    gmap f = \ (Group (l, as)) -> Group (l, f as)

graphFromList :: [CGEdge] -> CGraph
graphFromList es = foldr (\ (vout, l, vin) ->
            insertVertex vin . insertVertex vout . insertEdge vin l (vout, l, vin) .  insertEdge vout l (vout, l, vin)
        ) Data.Map.empty es where
            insertVertex v m = if Data.Map.member v m then m else Data.Map.insert v Data.Map.empty m
            insertEdge v l e m = Data.Map.adjust (\ em ->
                                     if Data.Map.member l em then Data.Map.adjust (\es -> es ++ [e]) l em else Data.Map.insert l [e] em) v m

g :: CGraph
g = graphFromList [("1","a","2"), ("1","a","4"), ("4","c","3"), ("4","c","5"), ("3","a","5"), ("5","d","4"), ("2", "b","3"), ("3", "c", "1")]

main :: IO ()
main = do
    let (a, _) = runState (do unCQuery (start "1" >>> selectE "a" >>> selectOutV >>> groupCount (\ p -> case p of VLeaf v -> v
                                                                                                                  VCons v ep -> v)) []) g in
        print a
        
