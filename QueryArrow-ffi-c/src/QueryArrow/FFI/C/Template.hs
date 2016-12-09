{-# LANGUAGE FlexibleContexts, MultiParamTypeClasses, FlexibleInstances, OverloadedStrings, GADTs, ExistentialQuantification, TemplateHaskell #-}

module QueryArrow.FFI.C.Template where

import QueryArrow.FO.Data (Pred(..), Formula(..), Var(..), Expr(..), Atom(..), Aggregator(..), Summary(..), Lit(..), Sign(..), PredType(..), ParamType(..), Serialize(..), constructPredTypeMap, CastType(..))
import QueryArrow.DB.DB
import QueryArrow.Rewriting
import Data.Namespace.Path

import Prelude hiding (lookup)
import Data.Text (Text)
import qualified Data.Text as Text
import Control.Monad.Reader
import qualified Data.Map.Strict as Map
import Control.Monad.IO.Class (liftIO)
import Language.Haskell.TH hiding (Pred)
import Data.Char (toLower)
import Foreign.C.String
import Foreign.Ptr
import Foreign.Storable
import Foreign.C.Types
import Foreign.Marshal.Array
import Foreign.StablePtr
import Data.Int
import Control.Arrow ((***))
import System.Log.Logger (infoM)
import QueryArrow.FFI.Service
import QueryArrow.FFI.Auxiliary
import QueryArrow.Data.Abstract
import QueryArrow.Data.Template
import Data.Maybe (fromMaybe)
import Data.Map.Strict (lookup)

functype :: Int -> TypeQ -> TypeQ
functype 0 t = t
functype n t = functype (n - 1) [t|CString -> $(t)|]

data Type2 = StringType | IntType deriving (Show)

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
    infoM "Plugin" ("arrayToAllocatedBuffer: converting " ++ show txt)
    zipWithM_ (\i txt -> textToBuffer (plusPtr  buf (n1*i)) n1 txt) [0..n2-1] (take n2 (concat txt))

arrayToAllocatedBuffer2 :: Ptr CString -> Ptr CInt -> Int -> [[Text]] -> IO ()
arrayToAllocatedBuffer2 buf buflens n txt = do
    lens <- peekArray n buflens
    bufs <- peekArray n buf
    mapM_ (\(txt, ptr, len) -> do
          textToBuffer ptr (fromIntegral len) txt) (zip3 (take n (concat txt)) bufs lens)

arrayToAllocateBuffer :: Ptr (Ptr CString) -> Ptr CInt -> [[Text]] -> IO ()
arrayToAllocateBuffer buf lenbuf txt = do
    infoM "Plugin" ("return all " ++ show txt)
    let totaltxt = concat txt
    let totallen = length totaltxt
    poke lenbuf (fromIntegral totallen)
    arrelems <- mapM (newCString . Text.unpack) totaltxt
    arr <- newArray arrelems
    poke buf arr

queryFunction :: String -> [Type2] -> [Type2] -> DecsQ
queryFunction name inputtypes outputtypes = do
    let fn = mkName ("get_" ++ name)
    let pn = varE (mkName name)
    s <- [p|svcptr|]
    p <- [p|session|]
    let args = map (\i -> "arg" ++ show i) [1..length inputtypes]
    let argnames = map mkName args
    let rets = map (\i -> "ret" ++ show i) [1..length outputtypes]
    let ps = s : p : map VarP argnames
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
    b <- [|$(func) svcptr session $(retList) ($(pn) @@ $(argList2)) (Map.fromList $(argList))|]
    sequence [funD fn [return (Clause ps (NormalB b) [])]]

queryLongFunction :: String -> [Type2] -> [Type2] -> DecsQ
queryLongFunction name inputtypes outputtypes = do
    let fn = mkName ("get_int_" ++ name)
    let pn = varE (mkName name)
    s <- [p|svcptr|]
    p <- [p|session|]
    let args = map (\i -> "arg" ++ show i) [1..length inputtypes]
    let argnames = map mkName args
    let rets = map (\i -> "ret" ++ show i) [1..length outputtypes]
    let ps = s : p : map VarP argnames
    -- this is the input map
    let argList = listE (map (\i -> [| (Var $(stringE i), StringValue $(varE (mkName i))) |]) args)
    -- this is the args to the predicate in QAL
    let argList2 = listE (map (\i -> [| var $(stringE i)|]) (args ++ rets))
    -- this is the list of ret vars
    let retList = listE (map (\i -> [| Var $(stringE i) |]) rets)
    case outputtypes of
                  [StringType] -> do
                      runIO $ putStrLn ("generating function " ++ show fn)
                      b <- [|getIntResult svcptr session $(retList) ($(pn) @@ $(argList2)) (Map.fromList $(argList))|]
                      sequence [funD fn [return (Clause ps (NormalB b) [])]]
                  _ -> do
                      runIO $ appendFile "/tmp/log" ("cannot generate function " ++ show fn ++ show outputtypes ++ "\n")
                      return []

querySomeFunction :: String -> [Type2] -> [Type2] -> DecsQ
querySomeFunction name inputtypes outputtypes = do
    let fn = mkName ("get_some_" ++ name)
    let pn = varE (mkName name)
    s <- [p|svcptr|]
    p <- [p|session|]
    let n = mkName ("a")
    np <- [p|a|]
    let args = map (\i -> "arg" ++ show i) [1..length inputtypes]
    let argnames = map mkName args
    let rets = map (\i -> "ret" ++ show i) [1..length outputtypes]
    let ps = s : p : np : map VarP argnames
    -- this is the input map
    let argList = listE (map (\i -> [| (Var $(stringE i), StringValue $(varE (mkName i))) |]) args)
    -- this is the args to the predicate in QAL
    let argList2 = listE (map (\i -> [| var $(stringE i)|]) (args ++ rets))
    -- this is the list of ret vars
    let retList = listE (map (\i -> [| Var $(stringE i) |]) rets)
    let func = [|getSomeStringArrayResult|]
    runIO $ putStrLn ("generating function " ++ show fn)
    b <- [|$(func) svcptr session $(varE n) $(retList) ($(pn) @@ $(argList2)) (Map.fromList $(argList))|]
    sequence [funD fn [return (Clause ps (NormalB b) [])]]

queryAllFunction :: String -> [Type2] -> [Type2] -> DecsQ
queryAllFunction name inputtypes outputtypes = do
    let fn = mkName ("get_all_" ++ name)
    let pn = varE (mkName name)
    s <- [p|svcptr|]
    p <- [p|session|]
    let args = map (\i -> "arg" ++ show i) [1..length inputtypes]
    let argnames = map mkName args
    let rets = map (\i -> "ret" ++ show i) [1..length outputtypes]
    let ps = s : p : map VarP argnames
    -- this is the input map
    let argList = listE (map (\i -> [| (Var $(stringE i), StringValue $(varE (mkName i))) |]) args)
    -- this is the args to the predicate in QAL
    let argList2 = listE (map (\i -> [| var $(stringE i)|]) (args ++ rets))
    -- this is the list of ret vars
    let retList = listE (map (\i -> [| Var $(stringE i) |]) rets)
    let func = [|getAllStringArrayResult|]
    runIO $ putStrLn ("generating function " ++ show fn)
    b <- [|$(func) svcptr session $(retList) ($(pn) @@ $(argList2)) (Map.fromList $(argList))|]
    sequence [funD fn [return (Clause ps (NormalB b) [])]]

{-|
    this function has the following format
    function(params, nparams, output, ncols, nrows)
-}
queryAll2Function :: String -> [Type2] -> [Type2] -> DecsQ
queryAll2Function name inputtypes outputtypes = do
    let fn = mkName ("get_all2_" ++ name)
    let pn = varE (mkName name)
    let s = mkName "svcptr"
    let p = mkName "session"
    let args = map (\i -> "arg" ++ show i) [1..length inputtypes]
    let listarg = mkName "listarg"
    let rets = map (\i -> "ret" ++ show i) [1..length outputtypes]
    let ps = [VarP s, VarP p, VarP listarg]
    -- this is the input map
    let argList = listE (map (\i -> [| (Var $(stringE i), StringValue $(varE (mkName i))) |]) args)
    -- this is the args to the predicate in QAL
    let argList2 = listE (map (\i -> [| var $(stringE i)|]) (args ++ rets))
    let argListPat = listP (map (\i -> (varP (mkName i))) args)
    -- this is the list of ret vars
    let retList = listE (map (\i -> [| Var $(stringE i) |]) rets)
    let func = [|getAllStringArrayResult|]
    runIO $ putStrLn ("generating function " ++ show fn)
    b <- [|let $(argListPat) = $(varE listarg) in
                      $(func) svcptr session $(retList) ($(pn) @@ $(argList2)) (Map.fromList $(argList))|]
    sequence [funD fn [return (Clause ps (NormalB b) [])]]

hsQueryForeign :: String -> [Type2] -> [Type2] -> DecsQ
hsQueryForeign name inputtypes outputtypes = do
  runIO $ putStrLn ("generating foreign " ++ ("hs_get_" ++ name))
  sequence [ForeignD <$> (ExportF CCall ("hs_get_" ++ name) (mkName ("hs_get_" ++ name)) <$> [t|forall a . StablePtr (QueryArrowService a) -> StablePtr a -> $(functype (length inputtypes) (case outputtypes of
    [StringType] -> [t|CString -> CInt -> IO Int|]
    [IntType] -> [t|Ptr CLong -> IO Int|]
    _ -> [t|Ptr CString -> CInt -> IO Int|]))|])]

hsQueryLongForeign :: String -> [Type2] -> [Type2] -> DecsQ
hsQueryLongForeign name inputtypes outputtypes = do
    let fn = "hs_get_int_" ++ name
    runIO $ putStrLn ("generating foreign " ++ fn)
    case outputtypes of
        [StringType] ->
            sequence [ForeignD <$> (ExportF CCall ("hs_get_int_" ++ name) (mkName fn) <$> [t|forall a . StablePtr (QueryArrowService a) -> StablePtr a -> $(functype (length inputtypes) [t|Ptr CLong -> IO Int|])|])]
        _ -> do
            runIO $ appendFile "/tmp/log" ("cannot generate function " ++ show fn ++ show outputtypes ++ "\n")
            return []

hsQuerySomeForeign :: String -> [Type2] -> [Type2] -> DecsQ
hsQuerySomeForeign name inputtypes outputtypes = do
    runIO $ putStrLn ("generating foreign " ++ ("hs_get_some_" ++ name))
    sequence [ForeignD <$> (ExportF CCall ("hs_get_some_" ++ name) (mkName ("hs_get_some_" ++ name)) <$> [t|forall a . StablePtr (QueryArrowService a) -> StablePtr a -> $(functype (length inputtypes) ([t| CString -> CInt -> CInt -> IO Int|]))|])]

hsQuerySome2Foreign :: String -> [Type2] -> [Type2] -> DecsQ
hsQuerySome2Foreign name inputtypes outputtypes = do
    runIO $ putStrLn ("generating foreign " ++ ("hs_get_some2_" ++ name))
    sequence [ForeignD <$> (ExportF CCall ("hs_get_some2_" ++ name) (mkName ("hs_get_some2_" ++ name)) <$> [t|forall a . StablePtr (QueryArrowService a) -> StablePtr a -> $(functype (length inputtypes) ([t| Ptr CString -> Ptr CInt -> CInt -> IO Int|]))|])]

hsQueryAllForeign :: String -> [Type2] -> [Type2] -> DecsQ
hsQueryAllForeign name inputtypes outputtypes = do
    runIO $ putStrLn ("generating foreign " ++ ("hs_get_all_" ++ name))
    sequence [ForeignD <$> (ExportF CCall ("hs_get_all_" ++ name) (mkName ("hs_get_all_" ++ name)) <$> [t|forall a . StablePtr (QueryArrowService a) -> StablePtr a -> $(functype (length inputtypes) ([t| Ptr (Ptr CString) -> Ptr CInt -> IO Int|]))|])]

hsQueryAll2Foreign :: String -> [Type2] -> [Type2] -> DecsQ
hsQueryAll2Foreign name inputtypes outputtypes = do
    runIO $ putStrLn ("generating foreign " ++ ("hs_get_all2_" ++ name))
    sequence [ForeignD <$> (ExportF CCall ("hs_get_all2_" ++ name) (mkName ("hs_get_all2_" ++ name)) <$> [t|forall a . StablePtr (QueryArrowService a) -> StablePtr a -> Ptr CString -> CInt -> Ptr (Ptr CString) -> Ptr CInt -> IO Int|])]

hsQueryFunction :: String -> [Type2] -> [Type2] -> DecsQ
hsQueryFunction n inputtypes outputtypes = do
    let fn = mkName ("get_" ++ n)
    let fn2 = mkName ("hs_get_" ++ n)
    let retn = mkName ("retn")
    let retlen = mkName ("retlen")
    s <- [p|svcptr|]
    p <- [p|sessionptr|]
    let args = map (\i -> "arg" ++ show i) [1..length inputtypes]
    let argnames = map mkName args
    let ps = s : p : map VarP argnames ++ [VarP retn] ++ case outputtypes of
                  [StringType] -> [VarP retlen]
                  [IntType] -> []
                  _ -> [VarP retlen]
    let argList = map (\i -> [| liftIO $ cstringToText $(varE i) |]) argnames
    let retp = case outputtypes of
                  [StringType] -> [|textToBuffer $(varE retn) (fromIntegral $(varE retlen))|]
                  [IntType] -> [|intToBuffer $(varE retn)|]
                  _ -> [|arrayToBuffer $(varE retn) (fromIntegral $(varE retlen))|]
    runIO $ putStrLn ("generating function " ++ show fn2)
    let app = foldl (\expr name -> [|$(expr) $(varE name)|]) [|$(varE fn) svc session|] argnames
    let b0 = foldl (\expr (arg, name) -> [|do
                                        $(varP name) <- $(arg)
                                        $(expr)|]) [|do
                                          svc <- liftIO $ deRefStablePtr svcptr
                                          session <- liftIO $ deRefStablePtr sessionptr
                                          $(app)|] (zip argList argnames)
    b <- [|processRes $(b0) $(retp)|]
    sequence [funD fn2 [return (Clause ps (NormalB b) [])]]

hsQueryLongFunction :: String -> [Type2] -> [Type2] -> DecsQ
hsQueryLongFunction n inputtypes outputtypes = do
  let fn = mkName ("get_int_" ++ n)
  case outputtypes of
      [StringType] -> do
          let fn2 = mkName ("hs_get_int_" ++ n)
          let retn = mkName ("retn")
          s <- [p|svcptr|]
          p <- [p|sessionptr|]
          let args = map (\i -> "arg" ++ show i) [1..length inputtypes]
          let argnames = map mkName args
          let ps = s : p : map VarP argnames ++ [VarP retn]
          let argList = map (\i -> [| liftIO $ cstringToText $(varE i) |]) argnames
          runIO $ putStrLn ("generating function " ++ show fn2)
          let app = foldl (\expr name -> [|$(expr) $(varE name)|]) [|$(varE fn) svc session|] argnames
          let b0 = foldl (\expr (arg, name) -> [|do
                                              $(varP name) <- $(arg)
                                              $(expr)|]) [|do
                                                                svc <- liftIO $ deRefStablePtr svcptr
                                                                session <- liftIO $ deRefStablePtr sessionptr
                                                                $(app)|] (zip argList argnames)
          b <- [|processRes $(b0) (intToBuffer $(varE retn))|]
          sequence [funD fn2 [return (Clause ps (NormalB b) [])]]
      _ -> do
          runIO $ appendFile "/tmp/log" ("cannot generate function " ++ show fn ++ show outputtypes ++ "\n")
          return []

hsQuerySomeFunction :: String -> [Type2] -> [Type2] -> DecsQ
hsQuerySomeFunction n inputtypes outputtypes = do
    let fn = mkName ("get_some_" ++ n)
    let fn2 = mkName ("hs_get_some_" ++ n)
    let retn = mkName ("retn")
    let retlen = mkName ("retlen")
    let retlen2 = mkName ("retlen2")
    s <- [p|svcptr|]
    p <- [p|sessionptr|]
    let args = map (\i -> "arg" ++ show i) [1..length inputtypes]
    let argnames = map mkName args
    let ps = s : p : map VarP argnames ++ [VarP retn, VarP retlen, VarP retlen2]
    let argList = map (\i -> [| liftIO $ cstringToText $(varE i) |]) argnames
    let retp = [|arrayToAllocatedBuffer $(varE retn) (fromIntegral $(varE retlen)) (fromIntegral $(varE retlen2))|]
    runIO $ putStrLn ("generating function " ++ show fn2)
    let app = foldl (\expr name -> [|$(expr) $(varE name)|]) [|$(varE fn) svc session (fromIntegral $(varE retlen2))|] argnames
    let b0 = foldl (\expr (arg, name) -> [|do
                                        $(varP name) <- $(arg)
                                        $(expr)|]) [|do
                                                          svc <- liftIO $ deRefStablePtr svcptr
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
    s <- [p|svcptr|]
    p <- [p|sessionptr|]
    let args = map (\i -> "arg" ++ show i) [1..length inputtypes]
    let argnames = map mkName args
    let ps = s : p : map VarP argnames ++ [VarP retn, VarP retlen, VarP retlen2]
    let argList = map (\i -> [| liftIO $ cstringToText $(varE i) |]) argnames
    let retp = [|arrayToAllocatedBuffer2 $(varE retn) $(varE retlen) (fromIntegral $(varE retlen2))|]
    runIO $ putStrLn ("generating function " ++ show fn2)
    let app = foldl (\expr name -> [|$(expr) $(varE name)|]) [|$(varE fn) svc session (fromIntegral $(varE retlen2))|] argnames
    let b0 = foldl (\expr (arg, name) -> [|do
                                        $(varP name) <- $(arg)
                                        $(expr)|]) [|do
                                                          svc <- liftIO $ deRefStablePtr svcptr
                                                          session <- liftIO $ deRefStablePtr sessionptr
                                                          $(app)|] (zip argList argnames)
    b <- [|processRes2 $(b0) $(retp)|]
    sequence [funD fn2 [return (Clause ps (NormalB b) [])]]

hsQueryAllFunction :: String -> [Type2] -> [Type2] -> DecsQ
hsQueryAllFunction n inputtypes outputtypes = do
    let fn = mkName ("get_all_" ++ n)
    let fn2 = mkName ("hs_get_all_" ++ n)
    let retn = mkName ("retn")
    let retlen = mkName ("retlen")
    s <- [p|svcptr|]
    p <- [p|sessionptr|]
    let args = map (\i -> "arg" ++ show i) [1..length inputtypes]
    let argnames = map mkName args
    let ps = s : p : map VarP argnames ++ [VarP retn, VarP retlen]
    let argList = map (\i -> [| liftIO $ cstringToText $(varE i) |]) argnames
    let retp = [|arrayToAllocateBuffer $(varE retn) $(varE retlen)|]
    runIO $ putStrLn ("generating function " ++ show fn2)
    let app = foldl (\expr name -> [|$(expr) $(varE name)|]) [|$(varE fn) svc session|] argnames
    let b0 = foldl (\expr (arg, name) -> [|do
                                        $(varP name) <- $(arg)
                                        $(expr)|]) [|do
                                                          svc <- liftIO $ deRefStablePtr svcptr
                                                          session <- liftIO $ deRefStablePtr sessionptr
                                                          $(app)|] (zip argList argnames)
    b <- [|processRes2 $(b0) $(retp)|]
    sequence [funD fn2 [return (Clause ps (NormalB b) [])]]

hsQueryAll2Function :: String -> [Type2] -> [Type2] -> DecsQ
hsQueryAll2Function n inputtypes outputtypes = do
    let fn = mkName ("get_all2_" ++ n)
    let fn2 = mkName ("hs_get_all2_" ++ n)
    let retn = mkName "retn"
    let retlen = mkName "retlen"
    let s = mkName "svcptr"
    let p = mkName "sessionptr"
    let listarg = mkName "listarg"
    let cargs = mkName "cargs"
    let cnargs = mkName "cnargs"
    let args = map (\i -> "arg" ++ show i) [1..length inputtypes]
    let argnames = map mkName args
    let ps = [VarP s, VarP p, VarP cargs, VarP cnargs, VarP retn, VarP retlen]
    let argList = map (\i -> [| liftIO $ cstringToText $(varE i) |]) argnames
    let retp = [|arrayToAllocateBuffer $(varE retn) $(varE retlen)|]
    runIO $ putStrLn ("generating function " ++ show fn2)
    let app = [|$(varE fn) svc session $(varE listarg)|]
    let b0 = [|do
                  let nargs = fromIntegral $(varE cnargs)
                  $(varP listarg) <- liftIO $ peekArray nargs $(varE cargs) >>= mapM cstringToText
                  svc <- liftIO $ deRefStablePtr svcptr
                  session <- liftIO $ deRefStablePtr sessionptr
                  $(app)|]
    b <- [|processRes2 $(b0) $(retp)|]
    sequence [funD fn2 [return (Clause ps (NormalB b) [])]]

createFunction :: String -> Int -> DecQ
createFunction n a = do
    let fn = mkName ("create_" ++ n)
    let pn = varE (mkName n)
    s <- [p|svcptr|]
    p <- [p|session|]
    let args = map (\i -> "arg" ++ show i) [1..a]
    let argnames = map mkName args
    let ps = s : p : map VarP argnames
    let argList = listE (map (\i -> [| (Var $(stringE i), StringValue $(varE (mkName i))) |]) args)
    let argList2 = listE (map (\i -> [| var $(stringE i)|]) args)
    let func = [|execAbstract|]
    b <- [|$(func) svcptr session ($(pn) @@+ $(argList2)) (Map.fromList $(argList))|]
    funD fn [return (Clause ps (NormalB b) [])]

hsCreateForeign :: String -> Int -> DecQ
hsCreateForeign n a = do
    ForeignD <$> (ExportF CCall ("hs_create_" ++ n) (mkName ("hs_create_" ++ n)) <$> [t|forall a . StablePtr (QueryArrowService a) -> StablePtr a -> $(functype a [t|IO Int|])|])

hsUpdateForeign :: String -> Int -> DecQ
hsUpdateForeign n a = do
    ForeignD <$> (ExportF CCall ("hs_update_" ++ n) (mkName ("hs_create_" ++ n)) <$> [t|forall a . StablePtr (QueryArrowService a) -> StablePtr a -> $(functype a [t|IO Int|])|])

hsCreateFunction :: String -> Int -> DecQ
hsCreateFunction n a = do
    let fn = mkName ("create_" ++ n)
    let fn2 = mkName ("hs_create_" ++ n)
    s <- [p|svcptr|]
    p <- [p|sessionptr|]
    let args = map (\i -> "arg" ++ show i) [1..a]
    let argnames = map mkName args
    let ps = s : p : map VarP argnames
    let argList = map (\i -> [| liftIO $ cstringToText $(varE i) |]) argnames
    let app = foldl (\expr name -> [|$(expr) $(varE name)|]) [|$(varE fn) svc session|] argnames
    let b0 = foldl (\expr (arg, name) -> [|do
                                        $(varP name) <- $(arg)
                                        $(expr)|]) [|do
                                                          svc <- liftIO $ deRefStablePtr svcptr
                                                          session <- liftIO $ deRefStablePtr sessionptr
                                                          $(app)|] (zip argList argnames)
    b <- [|processRes $(b0) (const (return ()))|]
    funD fn2 [return (Clause ps (NormalB b) [])]

createFunctionArray :: String -> Int -> DecQ
createFunctionArray n a = do
    let fn = mkName ("array_create_" ++ n)
    let pn = varE (mkName n)
    s <- [p|svcptr|]
    p <- [p|session|]
    p2 <- [p|argarray|]
    let args = map (\i -> "arg" ++ show i) [1..a]
    let ps = [s, p , p2]
    let argList = listE (map (\i -> [| Var $(stringE i) |]) args)
    let argList2 = listE (map (\i -> [| var $(stringE i)|]) args)
    let func = [|execAbstract|]
    b <- [|$(func) svcptr session ($(pn) @@+ $(argList2)) (Map.fromList (zip $(argList) (map StringValue argarray)))|]
    funD fn [return (Clause ps (NormalB b) [])]

hsCreateForeignArray :: String -> DecQ
hsCreateForeignArray n = do
    ForeignD <$> (ExportF CCall ("hs_array_create_" ++ n) (mkName ("hs_array_create_" ++ n)) <$> [t|forall a . StablePtr (QueryArrowService a) -> StablePtr a -> Ptr CString -> CInt -> IO Int|])

hsCreateFunctionArray :: String -> DecQ
hsCreateFunctionArray n = do
    let fn = mkName ("array_create_" ++ n)
    let fn2 = mkName ("hs_array_create_" ++ n)
    s <- [p|svcptr|]
    p <- [p|sessionptr|]
    p2 <- [p|argarray|]
    p3 <- [p|len|]
    let ps = [s, p, p2, p3]
    let b0 = [|do
                  argarray2 <- liftIO $ peekArray (fromIntegral len) argarray >>= mapM cstringToText
                  svc <- liftIO $ deRefStablePtr svcptr
                  session <- liftIO $ deRefStablePtr sessionptr
                  $(varE fn) svc session argarray2|]
    b <- [|processRes $(b0) (const (return ()))|]
    funD fn2 [return (Clause ps (NormalB b) [])]

deleteFunction :: String -> Int -> DecQ
deleteFunction n a = do
    let fn = mkName ("delete_" ++ n)
    let pn = varE (mkName n)

    s <- [p|svcptr|]
    p <- [p|session|]
    let args = map (\i -> "arg" ++ show i) [1..a]
    let argnames = map mkName args
    let ps = s : p : map VarP argnames
    let argList = listE (map (\i -> [| (Var $(stringE i), StringValue $(varE (mkName i))) |]) args)
    let argList2 = listE (map (\i -> [| var $(stringE i)|]) args)
    let func = [|execAbstract|]
    b <- [|$(func) svcptr session ($(pn) @@- $(argList2)) (Map.fromList $(argList))|]
    funD fn [return (Clause ps (NormalB b) [])]

hsDeleteForeign :: String -> Int -> DecQ
hsDeleteForeign n a = do
    ForeignD <$> (ExportF CCall ("hs_delete_" ++ n) (mkName ("hs_delete_" ++ n)) <$> [t|forall a . StablePtr (QueryArrowService a) -> StablePtr a -> $(functype a [t|IO Int|])|])

hsDeleteFunction :: String -> Int -> DecQ
hsDeleteFunction n a = do
    let fn = mkName ("delete_" ++ n)
    let fn2 = mkName ("hs_delete_" ++ n)
    s <- [p|svcptr|]
    p <- [p|sessionptr|]
    let args = map (\i -> "arg" ++ show i) [1..a]
    let argnames = map mkName args
    let ps = s : p : map VarP argnames
    let argList = map (\i -> [| liftIO $ cstringToText $(varE i) |]) argnames
    let app = foldl (\expr name -> [|$(expr) $(varE name)|]) [|$(varE fn) svc session|] argnames
    let b0 = foldl (\expr (arg, name) -> [|do
                                        $(varP name) <- $(arg)
                                        $(expr)|]) [|do
                                                          svc <- liftIO $ deRefStablePtr svcptr
                                                          session <- liftIO $ deRefStablePtr sessionptr
                                                          $(app)|] (zip argList argnames)
    b <- [|processRes $(b0) (const (return ()))|]
    funD fn2 [return (Clause ps (NormalB b) [])]

functions :: String -> DecsQ
functions path = do
    (qr, ir, dr, preds) <- runIO (getRewritingRules path)
    let ptm = constructPredTypeMap preds
    qr1 <- concat <$> mapM (\(InsertRewritingRule (Atom ( name@(ObjectPath _ n0)  ) _) _) ->
                        let n = map toLower (drop 2 n0)
                            (PredType _ ts) = fromMaybe (error "error") (lookup name ptm)
                            getInputOutputTypes [] = ([], [])
                            getInputOutputTypes (ParamType _ _ False NumberType : t) = ((StringType :) *** id) (getInputOutputTypes t)
                            getInputOutputTypes (ParamType _ _ False TextType : t) = ((StringType :) *** id) (getInputOutputTypes t)
                            getInputOutputTypes (ParamType _ _ True NumberType : t) = ([], StringType : getTypes t)
                            getInputOutputTypes (ParamType _ _ True TextType : t) = ([], StringType : getTypes t)
                            getInputOutputTypes t = error ("getInputOutputTypes: error unsupported type " ++ show t)
                            getTypes [] = []
                            getTypes (ParamType _ _ True NumberType : t) = StringType : getTypes t
                            getTypes (ParamType _ _ True TextType : t) = StringType : getTypes t
                            getTypes t = error ("getTypes: error unsupported type " ++ show t)
                            (inputtypes, outputtypes) = getInputOutputTypes ts in
                            concat <$> sequence [
                              queryFunction n inputtypes outputtypes, hsQueryFunction n inputtypes outputtypes, hsQueryForeign n inputtypes outputtypes,
                              queryLongFunction n inputtypes outputtypes, hsQueryLongFunction n inputtypes outputtypes, hsQueryLongForeign n inputtypes outputtypes,
                              querySomeFunction n inputtypes outputtypes, hsQuerySomeFunction n inputtypes outputtypes, hsQuerySomeForeign n inputtypes outputtypes,
                              hsQuerySome2Function n inputtypes outputtypes, hsQuerySome2Foreign n inputtypes outputtypes,
                              queryAllFunction n inputtypes outputtypes, hsQueryAllFunction n inputtypes outputtypes, hsQueryAllForeign n inputtypes outputtypes,
                              queryAll2Function n inputtypes outputtypes, hsQueryAll2Function n inputtypes outputtypes, hsQueryAll2Foreign n inputtypes outputtypes]
                    ) qr
    ir1 <- concat <$> mapM (\(InsertRewritingRule (Atom (ObjectPath _ n0) args) _) ->
                        let n = map toLower (drop 2 n0) in
                            sequence [createFunction n (length args), hsCreateFunction n (length args), hsCreateForeign n (length args), hsUpdateForeign n (length args),
                              createFunctionArray n (length args), hsCreateFunctionArray n, hsCreateForeignArray n]
                    ) ir
    dr1 <- concat <$> mapM (\(InsertRewritingRule (Atom (ObjectPath _ n0) args) _) ->
                        let n = map toLower (drop 2 n0) in
                            sequence [deleteFunction n (length args), hsDeleteFunction n (length args), hsDeleteForeign n (length args)]
                    ) dr
    return (qr1 ++ ir1 ++ dr1)
