{-# LANGUAGE FlexibleContexts #-}

import FO
import Parser

import Test.QuickCheck
import Control.Applicative ((<$>), (<*>))
import Control.Monad (replicateM)
import Text.Parsec (runParser)
import Data.Functor.Identity (runIdentity, Identity)
import Data.Map.Strict ((!), Map, empty)
import Control.Monad.Trans.State.Strict (evalState)
import Debug.Trace (trace)
import Test.Hspec
import SQL.SQL

newtype Char2 = Char2 {unChar2 :: Char}

instance Arbitrary Char2 where
    arbitrary = Char2 <$> choose ('a', 'z')

class ArbitraryN a where
    arbitraryN :: Int -> Gen a
    
instance ArbitraryN Atom where
    arbitraryN 0 = Atom <$> arbitrary <*> arbitrary
    arbitraryN n | n > 0 = oneof [Atom <$> arbitrary <*> arbitrary, Exists <$> string2 <*> arbitraryN (n - 1)]
    arbitraryN n = error (show n)
        
instance ArbitraryN Lit where
    arbitraryN n = Lit <$> arbitrary <*> arbitraryN (n - 1)

instance ArbitraryN a => ArbitraryN [a] where
    arbitraryN n = do
        (Positive m) <- arbitrary
        let n' = n `quot` m + 1
        replicateM m (arbitraryN n')        
        
string2 :: Gen String
string2 = (:) <$> (unChar2 <$> arbitrary) <*> (map unChar2 <$> arbitrary)        

instance Arbitrary Pred where
    arbitrary = Pred <$> string2 <*> arbitrary

instance Arbitrary Expr where
    arbitrary = oneof [VarExpr <$> string2, IntExpr <$> arbitrary, StringExpr <$> string2]
    
instance Arbitrary Sign where
    arbitrary = oneof [return Pos, return Neg]

instance Arbitrary Atom where
    arbitrary = sized arbitraryN

instance Arbitrary Lit where
    arbitrary = sized arbitraryN

instance Arbitrary ResultValue where
    arbitrary = oneof [IntValue <$> arbitrary, StringValue <$> string2]

instance Arbitrary MapDB where
    arbitrary = do
        (Positive m) <- arbitrary
        let arbitraryList = oneof [replicateM m (IntValue <$> arbitrary), replicateM m (StringValue <$> string2)]
        MapDB <$> string2 <*> string2 <*> (zip <$> arbitraryList <*> arbitraryList)

newtype LimitedMapDB = LimitedMapDB MapDB deriving Show
instance Arbitrary LimitedMapDB where
    arbitrary = do
        (Positive m) <- arbitrary
        (Positive m1) <- arbitrary
        strList <- replicateM m string2
        let arbitraryList = replicateM m1 (oneof ((return . StringValue) <$> strList))
        LimitedMapDB <$> (MapDB <$> string2 <*> string2 <*> (zip <$> arbitraryList <*> arbitraryList))

runQuery ::  [Database Identity row] -> String -> [row]
runQuery dbs query2 =
    case runParser progp (constructPredMap dbs) "" query2 of
            Left err -> let errmsg = "cannot parse " ++ query2 ++ show err in trace errmsg error ""
            Right (lits, _) -> runIdentity (do 
                let qu2 = Query (freeVars lits) lits
                (results, _) <- getAllResults dbs qu2
                return results)
                
to1 :: Ord k => [k] -> [Map k t] -> [[t]]
to1 vars  = map (\res -> [res ! var | var <- vars])

to2 :: [(a,a)] -> [[a]]
to2 = map (\(a,b) -> [a, b])

-- test that simple query of the form p(x,y) returns all rows
test0 :: MapDB -> Bool
test0 db = case db of 
    MapDB _ predName2 rows ->
        let query2 = predName2 ++ "(x,y)"
            results = runQuery [Database db] query2 in
            to1 ["x", "y"] results == to2 rows

-- test that simple query of the form p(x,y) p(y,z)
test1 :: LimitedMapDB -> Bool
test1 (LimitedMapDB db) = case db of 
    MapDB _ predName2 rows ->
        let query2 = predName2 ++ "(x,y)" ++ predName2 ++ "(y,z)"
            results = to1 ["x","y","z"] (runQuery [Database db] query2)
            rr = to2 rows in
            results ==  [ [a1, a2, b2] | [a1,a2] <- rr, [b1,b2] <- rr, a2 == b1]

-- test that simple query of the form p(x,y) ~exists p(y,z)
test2 :: LimitedMapDB -> Bool
test2 (LimitedMapDB db) = case db of 
    MapDB _ predName2 rows ->
        let query2 = predName2 ++ "(x,y)~(exists z." ++ predName2 ++ "(y,z))"
            results = to1 ["x","y"] (runQuery [Database db] query2)
            rr = to2 rows in
            results == [ [a,b] | [a,b] <- rr, null [ [a1,b1] | [a1,b1] <- rr, b==a1]]

-- test that simple query of the form p(x,y) exists p(y,z)
test3 :: LimitedMapDB -> Bool
test3 (LimitedMapDB db) = case db of 
    MapDB _ predName2 rows ->
        let query2 = predName2 ++ "(x,y)(exists z." ++ predName2 ++ "(y,z))"
            results = to1 ["x","y"] (runQuery [Database db] query2)
            rr = to2 rows in
            -- trace ("rr: " ++show rr++"\nresults: "++show results)
                results == [ [a,b] | [a,b] <- rr, _ <- [ [a1,b1] | [a1,b1] <- rr, b==a1]]

-- test that simple query of the form p(x,y) p(y,z) ~p(x,z)
test4 :: LimitedMapDB -> Bool
test4 (LimitedMapDB db) = case db of 
    MapDB _ predName2 rows ->
        let query2 = predName2 ++ "(x,y)" ++ predName2 ++ "(y,z)~"++predName2++"(x,z)"
            results = to1 ["x","y","z"] (runQuery [Database db] query2)
            rr = to2 rows in
            -- trace ("rr: " ++concatMap (\x-> show x++"\n") rr ++"\nresults: "++concatMap (\x-> show x++"\n") results)
                results == [ [a,b,c] | [a,b,c] <- [ [a1, a2, b2] | [a1,a2] <- rr, [b1,b2] <- rr, a2 == b1], [a, c] `notElem` rr]

-- test that simple query of the form p(x,y) | p(y,x)
test5 :: LimitedMapDB -> Bool
test5 (LimitedMapDB db) = case db of 
    MapDB _ predName2 rows ->
        let query2 = predName2 ++ "(x,y)|"++predName2 ++ "(y,x)"
            results = to1 ["x","y"] (runQuery [Database db, Database (EqDB "")] query2)
            rr = to2 rows in
            -- trace ("rr: " ++concatMap (\x-> show x++"\n") rr ++"\nresults: "++concatMap (\x-> show x++"\n") results)
                results == [ [a,b] | [a,b] <- rr]++[[b,a]|[ a,b]<-rr]

testsFunc ::(Show a, Arbitrary a)=>(a-> Bool)->IO ()
testsFunc = quickCheckWith stdArgs {maxSize = 5}

main :: IO ()
main = hspec $ do
    describe "tests" $ do
        it "test0" $ do
            property $ test0
        it "test1" $ do
            property $ test1
        it "test2" $ do
            property $ test2
        it "test3" $ do
            property $ test3
        it "test4" $ do
            property $ test4
        it "test5" $ do
            property $ test5
        it "test that freshSQLVar generates fresh SQL vars" $ do
            (v1, v2) <- return (evalState (do
                v1 <- freshSQLVar "t"
                v2 <- freshSQLVar "t"
                return (v1, v2)) (empty, BuiltIn (error "error") [], empty, empty, empty, empty, []))
            v1 `shouldBe` "t"
            v2 `shouldBe` "t0"
    