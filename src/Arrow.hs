module Arrow where

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
