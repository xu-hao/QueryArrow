{-# LANGUAGE MultiParamTypeClasses, FunctionalDependencies, TypeFamilies, FlexibleInstances, TypeSynonymInstances, FlexibleContexts #-}

-- iRODS 4.2 genquery library
-- http://www.irods.org
-- github: https://github.com/irods/irods

module QueryArrow where

import Control.Arrow
import Control.Category
import Control.Monad
import Prelude hiding (id,(.), concat, foldr)
import Data.Functor.Identity

class Commutative m n where
    commutative :: m (n a) -> n (m a)

class CommutativeFL m n where
    commutativefl :: m (n a b) -> n (m a) b

class CommutativeSL m n where
    commutativesl :: m (n a b) -> n a (m b)
    
class CommutativeFR m n where
    commutativefr :: n (m a) b -> m (n a b)

class CommutativeSR m n where
    commutativesr :: n a (m b) -> m (n a b)
    
class Commutative2L m n where
    commutative2l :: m (n a b) -> n (m a) (m b)

class Commutative2R n m where
    commutative2r :: n (m a) (m b) -> m (n a b)
    
instance (Monad n) => Commutative Identity n where
    commutative (Identity na) = do
        a <- na
        return (Identity a)
        
instance (Monad n) => Commutative n Identity where
    commutative nia = Identity (do
        (Identity a) <- nia
        return a)
        
instance (Monad m) => CommutativeFR m (,) where
    commutativefr (ma, b) = do
        a <- ma
        return (a, b)
    
instance Commutative2R (,) Identity where
    commutative2r  (Identity a, Identity b) = Identity (a, b)
        
instance Commutative2L Identity (,) where
    commutative2l (Identity (a, b)) = (Identity a, Identity b)
    
-- abstract framework
-- Query

class (Arrow q) => Query q where
    start :: a -> q x a

-- A type constructor for Semigroup
-- similar to Alternative but doesn't require m to be Applicative
-- similar to Alt but doesn't require m to be a Functor
    
class (Query q) => Injectable q l where
    inj :: q a (l a)
    
class Query q => Monoidal q a b c where
    aplus :: q x a -> q x b -> q x c  

class Query q => Multiplicable q l a b where
    mtimes :: q (l a) b

class (Query q) => Growable q l where
    grow :: q a b -> q (l a) (l b)

class Query q => Filterable q l where
    qfilter :: q a Bool -> q (l a) (l a)

class Query q => Groupable q l a grp key | q l -> grp where
    qgroup :: q a key -> q (l a) (l (grp l key (l a)))
    gmap :: q (l a) (c l a) -> q (grp l key (l a)) (grp l key (c l a))

class Query q => Countable q l count | q l -> count where
    qcount :: q (l a) (count l a)
    qlimit :: Int -> q (l a) (l a)

class Query q => Sortable q l where
    qsort :: q (a,a) Ordering -> q (l a) (l a)

class (Query q) => Graph g q l | g -> q l where
    type Vertex g
    type Edge g
    type Label g
    data VPath g -- injectivity
    data EPath g
    inE :: q (VPath g) (l (EPath g))
    outE :: q (VPath g) (l (EPath g))
    inV :: q (EPath g) (VPath g)
    outV :: q (EPath g) (VPath g)
    linE :: Label g -> q (VPath g) (l (EPath g))
    loutE :: Label g -> q (VPath g) (l (EPath g))
    vpath :: Vertex g -> q x (VPath g)

infix 3 &++
(&++) :: (Monoidal q (l b) (l b) (l b)) => q a (l b) -> q a (l b) -> q a (l b)
(&++) = aplus

insert :: (Injectable q l, Monoidal q (l a) (l a) (l a)) => a -> q (l a) (l a)
insert a = arr id &++ (start a >>> inj)

groupCount :: (Countable q l count, Growable q l, Groupable q l a grp key) => q a key -> q (l a) (l (grp l key (count l a)))
groupCount f = qgroup f >>> grow (gmap qcount)

selectInE :: (Graph g q l, Growable q l, Multiplicable q l (l (EPath g)) (l (EPath g))) => Label g -> q (l (VPath g)) (l (EPath g))
selectInE l =  grow (linE l) >>> mtimes

selectOutE :: (Graph g q l, Growable q l, Multiplicable q l (l (EPath g)) (l (EPath g))) => Label g -> q (l (VPath g)) (l (EPath g))
selectOutE l = grow (loutE l) >>> mtimes

selectInV :: (Graph g q l, Growable q l) => q (l (EPath g)) (l (VPath g))
selectInV = grow inV

selectOutV :: (Graph g q l, Growable q l) => q (l (EPath g)) (l (VPath g))
selectOutV = grow outV

selectE :: (Graph g q l, Growable q l, Multiplicable q l (l (EPath g)) (l (EPath g)), Monoidal q (l (EPath g)) (l (EPath g)) (l (EPath g))) => Label g -> q (l (VPath g)) (l (EPath g))
selectE l = selectInE l &++ selectOutE l

selectEV :: (Graph g q l, Growable q l, Multiplicable q l (l (EPath g)) (l (EPath g)), Monoidal q (l (VPath g)) (l (VPath g)) (l (VPath g))) => Label g -> q (l (VPath g)) (l (VPath g))
selectEV l = (selectInE l >>> selectOutV) &++ (selectOutE l >>> selectInV)

startV :: (Graph g q l, Injectable q l) => Vertex g -> q x (l (VPath g))
startV v = vpath v >>> inj