{-# LANGUAGE FlexibleContexts, MultiParamTypeClasses, FlexibleInstances, OverloadedStrings, GADTs, ExistentialQuantification, TemplateHaskell #-}
{-# LANGUAGE TypeFamilies, MultiParamTypeClasses, FlexibleInstances, UndecidableInstances, FlexibleContexts #-}

module Client.Template where

import FO.Data (Pred(..), Formula(..), Var(..), Expr(..), Atom(..), Aggregator(..), Summary(..), Lit(..), Sign(..), PredType(..), ParamType(..))
import DB.DB
import Translation
import QueryPlan
import DB.ResultStream
import Rewriting
import Config
import Utils
import Data.Namespace.Path
import qualified SQL.HDBC.PostgreSQL as PostgreSQL

import Prelude hiding (lookup)
import Data.Set (Set, singleton, fromList, empty)
import Data.Text (Text)
import qualified Data.Text as Text
import Control.Monad.Reader
import Control.Applicative (liftA2, pure)
import Control.Monad.Trans.Resource (runResourceT)
import Data.Map.Strict (lookup)
import qualified Data.Map.Strict as Map
import Control.Monad.Trans.Either (EitherT, runEitherT)
import Control.Monad.Error.Class (throwError)
import Control.Monad.IO.Class (liftIO)
import Language.Haskell.TH hiding (Pred)
import Language.Haskell.TH.Syntax (VarBangType)
import Data.Char (toLower)
import Data.List (nub)
import Foreign.C.String
import Foreign.Ptr
import Foreign.Storable
import Foreign.C.Types
import Foreign.Marshal.Array
import Foreign.StablePtr
import Control.Arrow ((+++), (***))
import Data.Int

eCAT_NO_ROWS_FOUND :: Int
eCAT_NO_ROWS_FOUND = -1

type Error = (Int, Text)

infixl 5 @@
infixl 4 .*.
infixl 3 .+.

(@@) :: (a -> Pred) -> [Expr] -> Reader a Formula
label @@ args = do
    p <- ask
    return (FAtomic (Atom (label p) args))

cre :: (a -> Pred) -> [Expr] -> Reader a Formula
cre label args = do
    p <- ask
    return (FInsert (Lit Pos (Atom (label p) args)))

del :: (a -> Pred) -> [Expr] -> Reader a Formula
del label args = do
    p <- ask
    return (FInsert (Lit Neg (Atom (label p) args)))

notE :: Liftable a b => a -> Reader b Formula
notE a =
    Aggregate Not <$> liftIt a

existsE :: Liftable a b => a -> Reader b Formula
existsE a =
    Aggregate Exists <$> liftIt a

class Liftable a b where
    liftIt :: a -> Reader b Formula

instance Liftable (Reader a Formula) a where
    liftIt = id

instance Liftable Formula a where
    liftIt = pure

class Combinable a b c where
    (.*.) :: a -> b -> Reader c Formula
    (.+.) :: a -> b -> Reader c Formula

instance (Liftable a c, Liftable b c) => Combinable a b c where
    a .*. b = liftA2 FSequencing (liftIt a) (liftIt b)
    a .+. b = liftA2 FChoice (liftIt a) (liftIt b)

var :: String -> Expr
var = VarExpr . Var

aggregate :: Liftable a b => [(Var, Summary)] -> [Var] -> a -> Reader b Formula
aggregate s groupby a = Aggregate (Summarize s groupby) <$> liftIt a

formula :: Liftable a b => b -> a -> Formula
formula p r = runReader (liftIt r) p

data Session a = forall db. (IDatabaseUniformRowAndDBFormula MapResultRow Formula db) => Session db (ConnectionType db) a

execQuery :: (Liftable a b) => Session b -> a -> MapResultRow -> EitherT Error IO ()
execQuery (Session db conn predicates) form params =
  liftIO $ runResourceT (depleteResultStream (doQueryWithConn db conn empty (formula predicates form) (fromList (Map.keys params)) (listResultStream [params])))

getResults :: (Liftable a b) => Session b -> a -> MapResultRow -> EitherT Error IO [MapResultRow]
getResults (Session db conn predicates) form params =
  liftIO $ runResourceT (getAllResultsInStream (doQueryWithConn db conn empty (formula predicates form) (fromList (Map.keys params)) (listResultStream [params])))

getResultValues :: (Liftable a b) => Session b -> [Var] -> a -> MapResultRow -> EitherT Error IO [ResultValue]
getResultValues (Session db conn predicates) vars form params = do
  count <- liftIO $ runResourceT (resultStreamTake 1 (doQueryWithConn db conn (fromList vars) (formula predicates form) (fromList (Map.keys params)) (listResultStream [params])))
  case count of
      row : _ -> case mapM (\var -> lookup var row) vars of
                  Just r -> return r
                  Nothing -> throwError (-1, "error 1")
      _ -> throwError (eCAT_NO_ROWS_FOUND, "error 2")

getAllResultValues :: (Liftable a b) => Session b -> [Var] -> a -> MapResultRow -> EitherT Error IO [[ResultValue]]
getAllResultValues (Session db conn predicates) vars form params = do
  count <- liftIO $ runResourceT (getAllResultsInStream (doQueryWithConn db conn (fromList vars) (formula predicates form) (fromList (Map.keys params)) (listResultStream [params])))
  case count of
      [] -> throwError (eCAT_NO_ROWS_FOUND, "error 2")
      rows ->
          case mapM (\row -> mapM (\var -> lookup var row) vars) rows of
              Just r -> return r
              Nothing -> throwError (-1, "error 1")

getSomeResultValues :: (Liftable a b) => Session b -> Int -> [Var] -> a -> MapResultRow -> EitherT Error IO [[ResultValue]]
getSomeResultValues (Session db conn predicates) n vars form params = do
  count <- liftIO $ runResourceT (resultStreamTake n (doQueryWithConn db conn (fromList vars) (Aggregate (Limit n) (formula predicates form)) (fromList (Map.keys params)) (listResultStream [params])))
  case count of
      [] -> throwError (eCAT_NO_ROWS_FOUND, "error 2")
      rows ->
          case mapM (\row -> mapM (\var -> lookup var row) vars) rows of
              Just r -> return r
              Nothing -> throwError (-1, "error 1")

getIntResult :: (Liftable a b) => Session b ->[ Var ]-> a -> MapResultRow -> EitherT Error IO Int64
getIntResult session vars form params = do
    r:_ <- getResultValues session vars form params
    return (resultValueToInt r)

getStringResult :: (Liftable a b) => Session b -> [Var ]-> a -> MapResultRow -> EitherT Error IO Text
getStringResult session vars form params = do
    r:_ <- getResultValues session vars form params
    return (resultValueToString r)

getStringArrayResult :: (Liftable a b) => Session b -> [Var] -> a -> MapResultRow -> EitherT Error IO [Text]
getStringArrayResult session vars form params = do
    r <- getResultValues session vars form params
    return (map resultValueToString r)

getSomeStringArrayResult :: (Liftable a b) => Session b -> Int -> [Var] -> a -> MapResultRow -> EitherT Error IO [[Text]]
getSomeStringArrayResult session n vars form params = do
    r <- getSomeResultValues session n vars form params
    return (map (map resultValueToString) r)

getAllStringArrayResult :: (Liftable a b) => Session b -> [Var] -> a -> MapResultRow -> EitherT Error IO [[Text]]
getAllStringArrayResult session vars form params = do
    r <- getAllResultValues session vars form params
    return (map (map resultValueToString) r)

resultValueToInt :: ResultValue -> Int64
resultValueToInt (IntValue i) = fromIntegral i
resultValueToInt (StringValue i) = read (Text.unpack i)

resultValueToString :: ResultValue -> Text
resultValueToString (IntValue i) = Text.pack (show i)
resultValueToString (StringValue i) = i

field :: String -> VarBangType
field x = (mkName x, Bang NoSourceUnpackedness NoSourceStrictness, ConT (mkName "Pred"))

struct :: [String] -> DecQ
struct xs = dataD (return []) (mkName "Predicates") [] Nothing [return (RecC (mkName "Predicates") (map field xs))] (return [])

functype :: Int -> TypeQ -> TypeQ
functype 0 t = t
functype n t = functype (n - 1) [t|CString -> $(t)|]

data Type2 = StringType | IntType

cstringToText :: CString -> IO Text
cstringToText str = do
    str2 <- peekCString str
    return (Text.pack str2)

textToBuffer :: CString -> Int -> Text -> IO ()
textToBuffer str n text = do
    let str2 = Text.unpack text
    pokeArray0 (castCharToCChar '\0') str (map castCharToCChar (take (n - 1) str2))

intToBuffer :: Ptr CLong -> Int64 -> IO ()
intToBuffer buf i =
    poke buf (fromIntegral i)

arrayToBuffer :: Ptr CString -> Int -> [Text] -> IO ()
arrayToBuffer buf n txt = do
    strs <- mapM (newCString . Text.unpack) (take n txt)
    pokeArray buf strs

arrayToAllocatedBuffer :: CString -> Int -> Int -> [[Text]] -> IO ()
arrayToAllocatedBuffer buf n1 n2 txt = do
    zipWithM_ (\i txt -> textToBuffer (plusPtr  buf (n1*i)) n1 txt) [0..n2-1] (take n2 (concat txt))

arrayToAllocatedBuffer2 :: Ptr CString -> Ptr CInt -> Int -> [[Text]] -> IO ()
arrayToAllocatedBuffer2 buf buflens n txt = do
    lens <- peekArray n buflens
    bufs <- peekArray n buf
    mapM_ (\(txt, ptr, len) -> do
          textToBuffer ptr (fromIntegral len) txt) (zip3 (take n (concat txt)) bufs lens)

arrayToAllocateBuffer :: Ptr (Ptr CString) -> [[Text]] -> IO ()
arrayToAllocateBuffer buf txt = do
    arr <- mallocArray (length txt)
    arrelems <- mapM (newCString . Text.unpack) (concat txt)
    pokeArray arr arrelems
    poke buf arr

processRes :: EitherT Error IO a -> (a -> IO ()) -> IO Int
processRes a f = do
    res <- runEitherT a
    case res of
        Left (ec, _) ->
            return (fromIntegral ec)
        Right a -> do
            f a
            return 0

processRes2 :: EitherT Error IO [a] -> ([a] -> IO ()) -> IO Int
processRes2 a f = do
    res <- runEitherT a
    case res of
        Left (ec, _) ->
            return (fromIntegral ec)
        Right a -> do
            f a
            return (length a)

queryFunction :: String -> [Type2] -> [Type2] -> DecsQ
queryFunction name inputtypes outputtypes = do
    let fn = mkName ("get_" ++ name)
    p <- [p|session|]
    let args = map (\i -> "arg" ++ show i) [1..length inputtypes]
    let argnames = map mkName args
    let rets = map (\i -> "ret" ++ show i) [1..length outputtypes]
    let ps = p : map VarP argnames
    -- this is the input map
    let argList = listE (map (\i -> [| (Var $(stringE i), StringValue $(varE (mkName i))) |]) args)
    -- this is the args to the predicate in QAL
    let argList2 = listE (map (\i -> [| var $(stringE i)|]) (args ++ rets))
    -- this is the list of ret vars
    let retList = listE (map (\i -> [| Var $(stringE i) |]) rets)
    let func = case outputtypes of
                    [StringType] -> [|getStringResult|]
                    [IntType] -> [|getIntResult|]
                    _ -> [|getStringArrayResult|]
    runIO $ putStrLn ("generating function " ++ show fn)
    b <- [|$(func) session $(retList) (local_zone @@ $(argList2)) (Map.fromList $(argList))|]
    sequence [funD fn [return (Clause ps (NormalB b) [])]]

queryLongFunction :: String -> [Type2] -> [Type2] -> DecsQ
queryLongFunction name inputtypes outputtypes = do
    let fn = mkName ("get_int_" ++ name)
    p <- [p|session|]
    let args = map (\i -> "arg" ++ show i) [1..length inputtypes]
    let argnames = map mkName args
    let rets = map (\i -> "ret" ++ show i) [1..length outputtypes]
    let ps = p : map VarP argnames
    -- this is the input map
    let argList = listE (map (\i -> [| (Var $(stringE i), StringValue $(varE (mkName i))) |]) args)
    -- this is the args to the predicate in QAL
    let argList2 = listE (map (\i -> [| var $(stringE i)|]) (args ++ rets))
    -- this is the list of ret vars
    let retList = listE (map (\i -> [| Var $(stringE i) |]) rets)
    case outputtypes of
                  [StringType] -> do
                      runIO $ putStrLn ("generating function " ++ show fn)
                      b <- [|getIntResult session $(retList) (local_zone @@ $(argList2)) (Map.fromList $(argList))|]
                      sequence [funD fn [return (Clause ps (NormalB b) [])]]
                  _ -> return []

querySomeFunction :: String -> [Type2] -> [Type2] -> DecsQ
querySomeFunction name inputtypes outputtypes = do
    let fn = mkName ("get_some_" ++ name)
    p <- [p|session|]
    let n = mkName ("a")
    np <- [p|a|]
    let args = map (\i -> "arg" ++ show i) [1..length inputtypes]
    let argnames = map mkName args
    let rets = map (\i -> "ret" ++ show i) [1..length outputtypes]
    let ps = p : np : map VarP argnames
    -- this is the input map
    let argList = listE (map (\i -> [| (Var $(stringE i), StringValue $(varE (mkName i))) |]) args)
    -- this is the args to the predicate in QAL
    let argList2 = listE (map (\i -> [| var $(stringE i)|]) (args ++ rets))
    -- this is the list of ret vars
    let retList = listE (map (\i -> [| Var $(stringE i) |]) rets)
    let func = [|getSomeStringArrayResult|]
    runIO $ putStrLn ("generating function " ++ show fn)
    b <- [|$(func) session $(varE n) $(retList) (local_zone @@ $(argList2)) (Map.fromList $(argList))|]
    sequence [funD fn [return (Clause ps (NormalB b) [])]]

queryAllFunction :: String -> [Type2] -> [Type2] -> DecsQ
queryAllFunction name inputtypes outputtypes = do
    let fn = mkName ("get_all_" ++ name)
    p <- [p|session|]
    let args = map (\i -> "arg" ++ show i) [1..length inputtypes]
    let argnames = map mkName args
    let rets = map (\i -> "ret" ++ show i) [1..length outputtypes]
    let ps = p : map VarP argnames
    -- this is the input map
    let argList = listE (map (\i -> [| (Var $(stringE i), StringValue $(varE (mkName i))) |]) args)
    -- this is the args to the predicate in QAL
    let argList2 = listE (map (\i -> [| var $(stringE i)|]) (args ++ rets))
    -- this is the list of ret vars
    let retList = listE (map (\i -> [| Var $(stringE i) |]) rets)
    let func = [|getAllStringArrayResult|]
    runIO $ putStrLn ("generating function " ++ show fn)
    b <- [|$(func) session $(retList) (local_zone @@ $(argList2)) (Map.fromList $(argList))|]
    sequence [funD fn [return (Clause ps (NormalB b) [])]]


hsQueryForeign :: String -> [Type2] -> [Type2] -> DecsQ
hsQueryForeign name inputtypes outputtypes = do
  runIO $ putStrLn ("generating foreign " ++ ("hs_get_" ++ name))
  let b = conT (mkName "Predicates")
  sequence [ForeignD <$> (ExportF CCall ("hs_get_" ++ name) (mkName ("hs_get_" ++ name)) <$> [t|StablePtr (Session $(b)) -> $(functype (length inputtypes) (case outputtypes of
    [StringType] -> [t|CString -> Int -> IO Int|]
    [IntType] -> [t|Ptr CLong -> IO Int|]
    _ -> [t|Ptr CString -> Int -> IO Int|]))|])]

hsQueryLongForeign :: String -> [Type2] -> [Type2] -> DecsQ
hsQueryLongForeign name inputtypes outputtypes = do
    runIO $ putStrLn ("generating foreign " ++ ("hs_get_int_" ++ name))
    let b = conT (mkName "Predicates")
    case outputtypes of
        [StringType] ->
            sequence [ForeignD <$> (ExportF CCall ("hs_get_int_" ++ name) (mkName ("hs_get_int_" ++ name)) <$> [t|StablePtr (Session $(b)) -> $(functype (length inputtypes) [t|Ptr CLong -> IO Int|])|])]
        _ -> return []

hsQuerySomeForeign :: String -> [Type2] -> [Type2] -> DecsQ
hsQuerySomeForeign name inputtypes outputtypes = do
    runIO $ putStrLn ("generating foreign " ++ ("hs_get_some_" ++ name))
    let b = conT (mkName "Predicates")
    sequence [ForeignD <$> (ExportF CCall ("hs_get_some_" ++ name) (mkName ("hs_get_some_" ++ name)) <$> [t|StablePtr (Session $(b)) -> $(functype (length inputtypes) ([t| CString -> Int -> Int -> IO Int|]))|])]

hsQuerySome2Foreign :: String -> [Type2] -> [Type2] -> DecsQ
hsQuerySome2Foreign name inputtypes outputtypes = do
    runIO $ putStrLn ("generating foreign " ++ ("hs_get_some2_" ++ name))
    let b = conT (mkName "Predicates")
    sequence [ForeignD <$> (ExportF CCall ("hs_get_some2_" ++ name) (mkName ("hs_get_some2_" ++ name)) <$> [t|StablePtr (Session $(b)) -> $(functype (length inputtypes) ([t| Ptr CString -> Ptr CInt -> Int -> IO Int|]))|])]

hsQueryAllForeign :: String -> [Type2] -> [Type2] -> DecsQ
hsQueryAllForeign name inputtypes outputtypes = do
    runIO $ putStrLn ("generating foreign " ++ ("hs_get_all_" ++ name))
    let b = conT (mkName "Predicates")
    sequence [ForeignD <$> (ExportF CCall ("hs_get_all_" ++ name) (mkName ("hs_get_all_" ++ name)) <$> [t|StablePtr (Session $(b)) -> $(functype (length inputtypes) ([t| Ptr (Ptr CString) -> IO Int|]))|])]

hsQueryFunction :: String -> [Type2] -> [Type2] -> DecsQ
hsQueryFunction n inputtypes outputtypes = do
    let fn = mkName ("get_" ++ n)
    let fn2 = mkName ("hs_get_" ++ n)
    let retn = mkName ("retn")
    let retlen = mkName ("retlen")
    p <- [p|sessionptr|]
    let args = map (\i -> "arg" ++ show i) [1..length inputtypes]
    let argnames = map mkName args
    let ps = p : map VarP argnames ++ [VarP retn] ++ case outputtypes of
                  [StringType] -> [VarP retlen]
                  [IntType] -> []
                  _ -> [VarP retlen]
    let argList = map (\i -> [| liftIO $ cstringToText $(varE i) |]) argnames
    let retp = case outputtypes of
                  [StringType] -> [|textToBuffer $(varE retn) $(varE retlen)|]
                  [IntType] -> [|intToBuffer $(varE retn)|]
                  _ -> [|arrayToBuffer $(varE retn) $(varE retlen)|]
    runIO $ putStrLn ("generating function " ++ show fn2)
    let app = foldl (\expr name -> [|$(expr) $(varE name)|]) [|$(varE fn) session|] argnames
    let b0 = foldl (\expr (arg, name) -> [|do
                                        $(varP name) <- $(arg)
                                        $(expr)|]) [|do
                                                          session <- liftIO $ deRefStablePtr sessionptr
                                                          $(app)|] (zip argList argnames)
    b <- [|processRes $(b0) $(retp)|]
    sequence [funD fn2 [return (Clause ps (NormalB b) [])]]

hsQueryLongFunction :: String -> [Type2] -> [Type2] -> DecsQ
hsQueryLongFunction n inputtypes outputtypes =
  case outputtypes of
      [StringType] -> do
          let fn = mkName ("get_int_" ++ n)
          let fn2 = mkName ("hs_get_int_" ++ n)
          let retn = mkName ("retn")
          let retlen = mkName ("retlen")
          p <- [p|sessionptr|]
          let args = map (\i -> "arg" ++ show i) [1..length inputtypes]
          let argnames = map mkName args
          let ps = p : map VarP argnames ++ [VarP retn]
          let argList = map (\i -> [| liftIO $ cstringToText $(varE i) |]) argnames
          runIO $ putStrLn ("generating function " ++ show fn2)
          let app = foldl (\expr name -> [|$(expr) $(varE name)|]) [|$(varE fn) session|] argnames
          let b0 = foldl (\expr (arg, name) -> [|do
                                              $(varP name) <- $(arg)
                                              $(expr)|]) [|do
                                                                session <- liftIO $ deRefStablePtr sessionptr
                                                                $(app)|] (zip argList argnames)
          b <- [|processRes $(b0) (intToBuffer $(varE retn))|]
          sequence [funD fn2 [return (Clause ps (NormalB b) [])]]
      _ -> return []

hsQuerySomeFunction :: String -> [Type2] -> [Type2] -> DecsQ
hsQuerySomeFunction n inputtypes outputtypes = do
    let fn = mkName ("get_some_" ++ n)
    let fn2 = mkName ("hs_get_some_" ++ n)
    let retn = mkName ("retn")
    let retlen = mkName ("retlen")
    let retlen2 = mkName ("retlen2")
    p <- [p|sessionptr|]
    let args = map (\i -> "arg" ++ show i) [1..length inputtypes]
    let argnames = map mkName args
    let ps = p : map VarP argnames ++ [VarP retn, VarP retlen, VarP retlen2]
    let argList = map (\i -> [| liftIO $ cstringToText $(varE i) |]) argnames
    let retp = [|arrayToAllocatedBuffer $(varE retn) $(varE retlen) $(varE retlen2)|]
    runIO $ putStrLn ("generating function " ++ show fn2)
    let app = foldl (\expr name -> [|$(expr) $(varE name)|]) [|$(varE fn) session $(varE retlen2)|] argnames
    let b0 = foldl (\expr (arg, name) -> [|do
                                        $(varP name) <- $(arg)
                                        $(expr)|]) [|do
                                                          session <- liftIO $ deRefStablePtr sessionptr
                                                          $(app)|] (zip argList argnames)
    b <- [|processRes2 $(b0) $(retp)|]
    sequence [funD fn2 [return (Clause ps (NormalB b) [])]]

hsQuerySome2Function :: String -> [Type2] -> [Type2] -> DecsQ
hsQuerySome2Function n inputtypes outputtypes = do
    let fn = mkName ("get_some_" ++ n)
    let fn2 = mkName ("hs_get_some2_" ++ n)
    let retn = mkName ("retn")
    let retlen = mkName ("retlen")
    let retlen2 = mkName ("retlen2")
    p <- [p|sessionptr|]
    let args = map (\i -> "arg" ++ show i) [1..length inputtypes]
    let argnames = map mkName args
    let ps = p : map VarP argnames ++ [VarP retn, VarP retlen, VarP retlen2]
    let argList = map (\i -> [| liftIO $ cstringToText $(varE i) |]) argnames
    let retp = [|arrayToAllocatedBuffer2 $(varE retn) $(varE retlen) $(varE retlen2)|]
    runIO $ putStrLn ("generating function " ++ show fn2)
    let app = foldl (\expr name -> [|$(expr) $(varE name)|]) [|$(varE fn) session $(varE retlen2)|] argnames
    let b0 = foldl (\expr (arg, name) -> [|do
                                        $(varP name) <- $(arg)
                                        $(expr)|]) [|do
                                                          session <- liftIO $ deRefStablePtr sessionptr
                                                          $(app)|] (zip argList argnames)
    b <- [|processRes2 $(b0) $(retp)|]
    sequence [funD fn2 [return (Clause ps (NormalB b) [])]]

hsQueryAllFunction :: String -> [Type2] -> [Type2] -> DecsQ
hsQueryAllFunction n inputtypes outputtypes = do
    let fn = mkName ("get_all_" ++ n)
    let fn2 = mkName ("hs_get_all_" ++ n)
    let retn = mkName ("retn")
    p <- [p|sessionptr|]
    let args = map (\i -> "arg" ++ show i) [1..length inputtypes]
    let argnames = map mkName args
    let ps = p : map VarP argnames ++ [VarP retn]
    let argList = map (\i -> [| liftIO $ cstringToText $(varE i) |]) argnames
    let retp = [|arrayToAllocateBuffer $(varE retn)|]
    runIO $ putStrLn ("generating function " ++ show fn2)
    let app = foldl (\expr name -> [|$(expr) $(varE name)|]) [|$(varE fn) session|] argnames
    let b0 = foldl (\expr (arg, name) -> [|do
                                        $(varP name) <- $(arg)
                                        $(expr)|]) [|do
                                                          session <- liftIO $ deRefStablePtr sessionptr
                                                          $(app)|] (zip argList argnames)
    b <- [|processRes2 $(b0) $(retp)|]
    sequence [funD fn2 [return (Clause ps (NormalB b) [])]]

createFunction :: String -> Int -> DecQ
createFunction n a = do
    let fn = mkName ("create_" ++ n)
    p <- [p|session|]
    let args = map (\i -> "arg" ++ show i) [1..a]
    let argnames = map mkName args
    let ps = p : map VarP argnames
    let argList = listE (map (\i -> [| (Var $(stringE i), StringValue $(varE (mkName i))) |]) args)
    let argList2 = listE (map (\i -> [| var $(stringE i)|]) args)
    let func = [|execQuery|]
    b <- [|$(func) session (local_zone @@ $(argList2)) (Map.fromList $(argList))|]
    funD fn [return (Clause ps (NormalB b) [])]

hsCreateForeign :: String -> Int -> DecQ
hsCreateForeign n a = do
    let b = conT (mkName "Predicates")
    ForeignD <$> (ExportF CCall ("hs_create_" ++ n) (mkName ("hs_create_" ++ n)) <$> [t|StablePtr (Session $(b)) -> $(functype a [t|IO Int|])|])

hsUpdateForeign :: String -> Int -> DecQ
hsUpdateForeign n a = do
    let b = conT (mkName "Predicates")
    ForeignD <$> (ExportF CCall ("hs_update_" ++ n) (mkName ("hs_create_" ++ n)) <$> [t|StablePtr (Session $(b)) -> $(functype a [t|IO Int|])|])

hsCreateFunction :: String -> Int -> DecQ
hsCreateFunction n a = do
    let fn = mkName ("create_" ++ n)
    let fn2 = mkName ("hs_create_" ++ n)
    p <- [p|sessionptr|]
    let args = map (\i -> "arg" ++ show i) [1..a]
    let argnames = map mkName args
    let ps = p : map VarP argnames
    let argList = map (\i -> [| liftIO $ cstringToText $(varE i) |]) argnames
    let app = foldl (\expr name -> [|$(expr) $(varE name)|]) [|$(varE fn) session|] argnames
    let b0 = foldl (\expr (arg, name) -> [|do
                                        $(varP name) <- $(arg)
                                        $(expr)|]) [|do
                                                          session <- liftIO $ deRefStablePtr sessionptr
                                                          $(app)|] (zip argList argnames)
    b <- [|processRes $(b0) (const (return ()))|]
    funD fn2 [return (Clause ps (NormalB b) [])]

createFunctionArray :: String -> Int -> DecQ
createFunctionArray n a = do
    let fn = mkName ("array_create_" ++ n)
    p <- [p|session|]
    p2 <- [p|argarray|]
    let args = map (\i -> "arg" ++ show i) [1..a]
    let ps = [p , p2]
    let argList = listE (map (\i -> [| Var $(stringE i) |]) args)
    let argList2 = listE (map (\i -> [| var $(stringE i)|]) args)
    let func = [|execQuery|]
    b <- [|$(func) session (local_zone @@ $(argList2)) (Map.fromList (zip $(argList) (map StringValue argarray)))|]
    funD fn [return (Clause ps (NormalB b) [])]

hsCreateForeignArray :: String -> DecQ
hsCreateForeignArray n = do
    let b = conT (mkName "Predicates")
    ForeignD <$> (ExportF CCall ("hs_array_create_" ++ n) (mkName ("hs_array_create_" ++ n)) <$> [t|StablePtr (Session $(b)) -> Ptr CString -> Int -> IO Int|])

hsCreateFunctionArray :: String -> DecQ
hsCreateFunctionArray n = do
    let fn = mkName ("array_create_" ++ n)
    let fn2 = mkName ("hs_array_create_" ++ n)
    p <- [p|sessionptr|]
    p2 <- [p|argarray|]
    p3 <- [p|len|]
    let ps = [p, p2, p3]
    let b0 = [|do
                  argarray <- liftIO $ peekArray len argarray
                  argarray <- liftIO $ mapM cstringToText argarray
                  session <- liftIO $ deRefStablePtr sessionptr
                  $(varE fn) session argarray|]
    b <- [|processRes $(b0) (const (return ()))|]
    funD fn2 [return (Clause ps (NormalB b) [])]

deleteFunction :: String -> Int -> DecQ
deleteFunction n a = do
    let fn = mkName ("delete_" ++ n)
    p <- [p|session|]
    let args = map (\i -> "arg" ++ show i) [1..a]
    let argnames = map mkName args
    let ps = p : map VarP argnames
    let argList = listE (map (\i -> [| (Var $(stringE i), StringValue $(varE (mkName i))) |]) args)
    let argList2 = listE (map (\i -> [| var $(stringE i)|]) args)
    let func = [|execQuery|]
    b <- [|$(func) session (local_zone @@ $(argList2)) (Map.fromList $(argList))|]
    funD fn [return (Clause ps (NormalB b) [])]

hsDeleteForeign :: String -> Int -> DecQ
hsDeleteForeign n a = do
    let b = conT (mkName "Predicates")
    ForeignD <$> (ExportF CCall ("hs_delete_" ++ n) (mkName ("hs_delete_" ++ n)) <$> [t|StablePtr (Session $(b)) -> $(functype a [t|IO Int|])|])

hsDeleteFunction :: String -> Int -> DecQ
hsDeleteFunction n a = do
    let fn = mkName ("delete_" ++ n)
    let fn2 = mkName ("hs_delete_" ++ n)
    p <- [p|sessionptr|]
    let args = map (\i -> "arg" ++ show i) [1..a]
    let argnames = map mkName args
    let ps = p : map VarP argnames
    let argList = map (\i -> [| liftIO $ cstringToText $(varE i) |]) argnames
    let app = foldl (\expr name -> [|$(expr) $(varE name)|]) [|$(varE fn) session|] argnames
    let b0 = foldl (\expr (arg, name) -> [|do
                                        $(varP name) <- $(arg)
                                        $(expr)|]) [|do
                                                          session <- liftIO $ deRefStablePtr sessionptr
                                                          $(app)|] (zip argList argnames)
    b <- [|processRes $(b0) (const (return ()))|]
    funD fn2 [return (Clause ps (NormalB b) [])]

getRewritingRules :: String -> IO ([InsertRewritingRule], [InsertRewritingRule], [InsertRewritingRule])
getRewritingRules path = do
  transinfo <- getConfig path
  db <- PostgreSQL.getDB (db_info (head (db_plugins transinfo)))
  case db of
      AbstractDatabase db2 -> do
          let predmap0 = constructDBPredMap db2
          putStrLn "predicates loaded "
          print predmap0
          -- trace ("preds:\n" ++ intercalate "\n" (map show (elems predmap0))) $ return ()
          (rewriting, _, _) <- getRewriting predmap0 transinfo
          return rewriting

getPredicates :: String -> IO ([Pred])
getPredicates path = do
  transinfo <- getConfig path
  db <- PostgreSQL.getDB (db_info (head (db_plugins transinfo)))
  case db of
      AbstractDatabase db2 -> do
          return (getPreds db2)

functions :: String -> DecsQ
functions path = do
    (qr, ir, dr) <- runIO (getRewritingRules path)
    qr1 <- concat <$> mapM (\(InsertRewritingRule (Atom (Pred (ObjectPath _ n0) (PredType _ ts) ) _) _) ->
                        let n = map toLower (drop 2 n0)
                            getInputOutputTypes [] = ([], [])
                            getInputOutputTypes (Key "Int" : t) = ((IntType :) *** id) (getInputOutputTypes t)
                            getInputOutputTypes (Key "Text" : t) = ((StringType :) *** id) (getInputOutputTypes t)
                            getInputOutputTypes (Property "Int" : t) = ([], IntType : getTypes t)
                            getInputOutputTypes (Property "Text" : t) = ([], StringType : getTypes t)
                            getInputOutputTypes t = error ("getInputOutputTypes: error unsupported type " ++ show t)
                            getTypes [] = []
                            getTypes (Property "Int" : t) = IntType : getTypes t
                            getTypes (Property "Text" : t) = IntType : getTypes t
                            getTypes t = error ("getTypes: error unsupported type " ++ show t)
                            (inputtypes, outputtypes) = getInputOutputTypes ts in
                            concat <$> sequence [
                              queryFunction n inputtypes outputtypes, hsQueryFunction n inputtypes outputtypes, hsQueryForeign n inputtypes outputtypes,
                              queryLongFunction n inputtypes outputtypes, hsQueryLongFunction n inputtypes outputtypes, hsQueryLongForeign n inputtypes outputtypes,
                              querySomeFunction n inputtypes outputtypes, hsQuerySomeFunction n inputtypes outputtypes, hsQuerySomeForeign n inputtypes outputtypes,
                              hsQuerySome2Function n inputtypes outputtypes, hsQuerySome2Foreign n inputtypes outputtypes,
                              queryAllFunction n inputtypes outputtypes, hsQueryAllFunction n inputtypes outputtypes, hsQueryAllForeign n inputtypes outputtypes]
                    ) qr
    ir1 <- concat <$> mapM (\(InsertRewritingRule (Atom (Pred (ObjectPath _ n0) _) args) _) ->
                        let n = map toLower (drop 2 n0) in
                            sequence [createFunction n (length args), hsCreateFunction n (length args), hsCreateForeign n (length args), hsUpdateForeign n (length args),
                              createFunctionArray n (length args), hsCreateFunctionArray n, hsCreateForeignArray n]
                    ) ir
    dr1 <- concat <$> mapM (\(InsertRewritingRule (Atom (Pred (ObjectPath _ n0) _) args) _) ->
                        let n = map toLower (drop 2 n0) in
                            sequence [deleteFunction n (length args), hsDeleteFunction n (length args), hsDeleteForeign n (length args)]
                    ) dr
    preds <- runIO (getPredicates path)
    runIO $ mapM_ (\(Pred (ObjectPath _ n0) _) -> putStrLn ("generating struct field: _" ++ map toLower n0)) preds
    st <- struct (nub (map (\(InsertRewritingRule (Atom (Pred (ObjectPath _ n0) _) _) _) -> map toLower (drop 2 n0)) (qr ++ ir ++ dr)) ++ map (\(Pred (ObjectPath _ n0) _) -> "_" ++ map toLower n0) preds)
    return  (st : qr1 ++ ir1 ++ dr1)
