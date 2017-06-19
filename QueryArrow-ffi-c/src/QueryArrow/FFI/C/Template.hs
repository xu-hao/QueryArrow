{-# LANGUAGE FlexibleContexts, MultiParamTypeClasses, FlexibleInstances, OverloadedStrings, GADTs, ExistentialQuantification, TemplateHaskell #-}

module QueryArrow.FFI.C.Template where

import QueryArrow.FO.Data
import QueryArrow.FO.Utils
import QueryArrow.Rewriting
import QueryArrow.Translation
import QueryArrow.Config
import QueryArrow.Utils
import QueryArrow.SQL.ICAT
import QueryArrow.FFI.Service
import QueryArrow.FFI.Auxiliary
import QueryArrow.SQL.HDBC.PostgreSQL

import Data.Namespace.Namespace
import Data.Namespace.Path

import Prelude hiding (lookup)
import Data.Text (Text)
import qualified Data.Text as Text
import Control.Monad.Reader
import Control.Monad.IO.Class (liftIO)
import Language.Haskell.TH hiding (Pred)
import Data.Char (toLower, toUpper)
import Foreign.C.String
import Foreign.Ptr
import Foreign.Storable
import Foreign.C.Types
import Foreign.Marshal.Array
import Foreign.StablePtr
import Data.Int
import Control.Arrow ((***))
import System.Log.Logger (infoM)
import Data.Maybe (fromMaybe, fromJust, isJust)
import Data.Map.Strict (lookup)
import qualified Data.Map.Strict as Map
import Data.Aeson
import Data.Aeson.TH
import Data.List (find)
import Debug.Trace

functype :: Int -> TypeQ -> TypeQ
functype 0 t = t
functype n t = functype (n - 1) [t|CString -> $(t)|]

data Type2 = StringListType | StringType | IntType deriving (Show, Eq)

cstringToText :: CString -> IO Text
cstringToText str = do
    str2 <- peekCString str
    return (Text.pack str2)

cstringToInt64 :: CString -> IO Int64
cstringToInt64 str = do
    str2 <- peekCString str
    return (read str2)

-- | convert a null terminated array of strings to a list of texts
cstringArrayToTextList :: Ptr CString -> IO [Text]
cstringArrayToTextList carrstr = do
    arrcstr <- peekArray0 nullPtr carrstr
    arrstr <- mapM peekCString arrcstr
    return (map Text.pack arrstr)

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
arrayToAllocatedBuffer buf n1 n2 txt0 = do
    infoM "Plugin" ("arrayToAllocatedBuffer: converting " ++ show txt0)
    zipWithM_ (\i txt -> textToBuffer (plusPtr  buf (n1*i)) n1 txt) [0..n2-1] (take n2 (concat txt0))

arrayToAllocatedBuffer2 :: Ptr CString -> Ptr CInt -> Int -> [[Text]] -> IO ()
arrayToAllocatedBuffer2 buf buflens n txt0 = do
    lens <- peekArray n buflens
    bufs <- peekArray n buf
    mapM_ (\(txt, ptr, len) -> do
          textToBuffer ptr (fromIntegral len) txt) (zip3 (take n (concat txt0)) bufs lens)

arrayToAllocateBuffer :: Ptr (Ptr CString) -> Ptr CInt -> [[Text]] -> IO ()
arrayToAllocateBuffer buf lenbuf txt = do
    infoM "Plugin" ("return all " ++ show txt)
    let totaltxt = concat txt
    let totallen = length totaltxt
    poke lenbuf (fromIntegral totallen)
    arrelems <- mapM (newCString . Text.unpack) totaltxt
    arr <- newArray arrelems
    poke buf arr

param :: String -> Type2 -> ExpQ
param i StringType = [| AbstractResultValue (StringValue $(varE (mkName i))) |] 
param i IntType = [| AbstractResultValue (Int64Value $(varE (mkName i))) |]
param i StringListType = [| AbstractResultValue (listValue (map StrinValue $(varE (mkName i)))) |]

queryFunction :: String -> [Type2] -> [Type2] -> DecsQ
queryFunction name inputtypes outputtypes = do
    let fn = mkName ("get_" ++ name)
    let pn = stringE ("D_" ++ map toUpper name)
    s <- [p|svcptr|]
    p <- [p|session|]
    let args = map (\i -> "arg" ++ show i) [1..length inputtypes]
    let argnames = map mkName args
    let rets = map (\i -> "ret" ++ show i) [1..length outputtypes]
    let ps = s : p : map VarP argnames
    -- this is the input map
    let argList = listE (zipWith (\i it -> [| (Var $(stringE i), $(param i it) ) |]) args inputtypes)
    -- this is the args to the predicate in QAL
    let argList2 = listE (map (\i -> [| var $(stringE i)|]) (args ++ rets))
    -- this is the list of ret vars
    let retList = listE (map (\i -> [| Var $(stringE i) |]) rets)
    let func = case outputtypes of
                    [StringType] -> [|getStringResult|]
                    [IntType] -> [|getStringResult|]
                    _ -> [|getStringArrayResult|]
    runIO $ putStrLn ("generating function " ++ show fn)
    b <- [|$(func) svcptr session $(retList) ( ($(pn) @@ $(argList2))) (Map.fromList $(argList))|]
    sequence [funD fn [return (Clause ps (NormalB b) [])]]

queryLongFunction :: String -> [Type2] -> [Type2] -> DecsQ
queryLongFunction name inputtypes outputtypes = do
    let fn = mkName ("get_int_" ++ name)
    let pn = stringE ("D_" ++ map toUpper name)
    s <- [p|svcptr|]
    p <- [p|session|]
    let args = map (\i -> "arg" ++ show i) [1..length inputtypes]
    let argnames = map mkName args
    let rets = map (\i -> "ret" ++ show i) [1..length outputtypes]
    let ps = s : p : map VarP argnames
    -- this is the input map
    let argList = listE (zipWith (\i it -> [| (Var $(stringE i), $(param i it) ) |]) args inputtypes)
    -- this is the args to the predicate in QAL
    let argList2 = listE (map (\i -> [| var $(stringE i)|]) (args ++ rets))
    -- this is the list of ret vars
    let retList = listE (map (\i -> [| Var $(stringE i) |]) rets)
    case outputtypes of
                  [StringType] -> do
                      runIO $ putStrLn ("generating function " ++ show fn)
                      b <- [|getIntResult svcptr session $(retList) ( ($(pn) @@ $(argList2))) (Map.fromList $(argList))|]
                      sequence [funD fn [return (Clause ps (NormalB b) [])]]
                  [IntType] -> do
                      runIO $ putStrLn ("generating function " ++ show fn)
                      b <- [|getIntResult svcptr session $(retList) ( ($(pn) @@ $(argList2))) (Map.fromList $(argList))|]
                      sequence [funD fn [return (Clause ps (NormalB b) [])]]
                  _ -> do
                      runIO $ appendFile "/tmp/log" ("cannot generate function " ++ show fn ++ show outputtypes ++ "\n")
                      return []

querySomeFunction :: String -> [Type2] -> [Type2] -> DecsQ
querySomeFunction name inputtypes outputtypes = do
    let fn = mkName ("get_some_" ++ name)
    let pn = stringE("D_" ++ map toUpper name)
    s <- [p|svcptr|]
    p <- [p|session|]
    let n = mkName ("a")
    np <- [p|a|]
    let args = map (\i -> "arg" ++ show i) [1..length inputtypes]
    let argnames = map mkName args
    let rets = map (\i -> "ret" ++ show i) [1..length outputtypes]
    let ps = s : p : np : map VarP argnames
    -- this is the input map
    let argList = listE (zipWith (\i it -> [| (Var $(stringE i), $(param i it) ) |]) args inputtypes)
    -- this is the args to the predicate in QAL
    let argList2 = listE (map (\i -> [| var $(stringE i)|]) (args ++ rets))
    -- this is the list of ret vars
    let retList = listE (map (\i -> [| Var $(stringE i) |]) rets)
    let func = [|getSomeStringArrayResult|]
    runIO $ putStrLn ("generating function " ++ show fn)
    b <- [|$(func) svcptr session $(varE n) $(retList) ( ($(pn) @@ $(argList2))) (Map.fromList $(argList))|]
    sequence [funD fn [return (Clause ps (NormalB b) [])]]

queryAllFunction :: String -> [Type2] -> [Type2] -> DecsQ
queryAllFunction name inputtypes outputtypes = do
    let fn = mkName ("get_all_" ++ name)
    let pn = stringE("D_" ++ map toUpper name)
    s <- [p|svcptr|]
    p <- [p|session|]
    let args = map (\i -> "arg" ++ show i) [1..length inputtypes]
    let argnames = map mkName args
    let rets = map (\i -> "ret" ++ show i) [1..length outputtypes]
    let ps = s : p : map VarP argnames
    -- this is the input map
    let argList = listE (zipWith (\i it -> [| (Var $(stringE i), $(param i it) ) |]) args inputtypes)
    -- this is the args to the predicate in QAL
    let argList2 = listE (map (\i -> [| var $(stringE i)|]) (args ++ rets))
    -- this is the list of ret vars
    let retList = listE (map (\i -> [| Var $(stringE i) |]) rets)
    let func = [|getAllStringArrayResult|]
    runIO $ putStrLn ("generating function " ++ show fn)
    b <- [|$(func) svcptr session $(retList) ( ($(pn) @@ $(argList2))) (Map.fromList $(argList))|]
    sequence [funD fn [return (Clause ps (NormalB b) [])]]

{-|
    this function has the following format
    function(params, nparams, output, ncols, nrows)
-}
queryAll2Function :: String -> [Type2] -> [Type2] -> DecsQ
queryAll2Function name inputtypes outputtypes = do
    let fn = mkName ("get_all2_" ++ name)
    let pn = stringE("D_" ++ map toUpper name)
    let s = mkName "svcptr"
    let p = mkName "session"
    let args = map (\i -> "arg" ++ show i) [1..length inputtypes]
    let listarg = mkName "listarg"
    let rets = map (\i -> "ret" ++ show i) [1..length outputtypes]
    let ps = [VarP s, VarP p, VarP listarg]
    -- this is the input map
    let argList = listE (zipWith (\i it -> [| (Var $(stringE i), $(typeToConverter it) $(varE (mkName i)) ) |]) args inputtypes)
    -- this is the args to the predicate in QAL
    let argList2 = listE (map (\i -> [| var $(stringE i)|]) (args ++ rets))
    let argListPat = listP (map (\i -> (varP (mkName i))) args)
    -- this is the list of ret vars
    let retList = listE (map (\i -> [| Var $(stringE i) |]) rets)
    let func = [|getAllStringArrayResult|]
    runIO $ putStrLn ("generating function " ++ show fn)
    b <- [|let $(argListPat) = $(varE listarg) in
                      $(func) svcptr session $(retList) ( ($(pn) @@ $(argList2))) (Map.fromList $(argList))|]
    sequence [funD fn [return (Clause ps (NormalB b) [])]]

hsQueryForeign :: String -> [Type2] -> [Type2] -> DecsQ
hsQueryForeign name inputtypes outputtypes = do
  runIO $ putStrLn ("generating foreign " ++ ("hs_get_" ++ name))
  sequence [ForeignD <$> (ExportF CCall ("hs_get_" ++ name) (mkName ("hs_get_" ++ name)) <$> [t|forall a . StablePtr (QueryArrowService a) -> StablePtr a -> $(functype (length inputtypes) (case outputtypes of
    [StringType] -> [t|CString -> CInt -> IO Int|]
    [IntType] -> [t|CString -> CInt -> IO Int|]
    _ -> [t|Ptr CString -> CInt -> IO Int|]))|])]

hsQueryLongForeign :: String -> [Type2] -> [Type2] -> DecsQ
hsQueryLongForeign name inputtypes outputtypes = do
    let fn = "hs_get_int_" ++ name
    runIO $ putStrLn ("generating foreign " ++ fn)
    case outputtypes of
        [StringType] ->
            sequence [ForeignD <$> (ExportF CCall ("hs_get_int_" ++ name) (mkName fn) <$> [t|forall a . StablePtr (QueryArrowService a) -> StablePtr a -> $(functype (length inputtypes) [t|Ptr CLong -> IO Int|])|])]
        [IntType] ->
            sequence [ForeignD <$> (ExportF CCall ("hs_get_int_" ++ name) (mkName fn) <$> [t|forall a . StablePtr (QueryArrowService a) -> StablePtr a -> $(functype (length inputtypes) [t|Ptr CLong -> IO Int|])|])]
        _ -> do
            runIO $ appendFile "/tmp/log" ("cannot generate function " ++ show fn ++ show outputtypes ++ "\n")
            return []

hsQuerySomeForeign :: String -> [Type2] -> [Type2] -> DecsQ
hsQuerySomeForeign name inputtypes _ = do
    runIO $ putStrLn ("generating foreign " ++ ("hs_get_some_" ++ name))
    sequence [ForeignD <$> (ExportF CCall ("hs_get_some_" ++ name) (mkName ("hs_get_some_" ++ name)) <$> [t|forall a . StablePtr (QueryArrowService a) -> StablePtr a -> $(functype (length inputtypes) ([t| CString -> CInt -> CInt -> IO Int|]))|])]

hsQuerySome2Foreign :: String -> [Type2] -> [Type2] -> DecsQ
hsQuerySome2Foreign name inputtypes _ = do
    runIO $ putStrLn ("generating foreign " ++ ("hs_get_some2_" ++ name))
    sequence [ForeignD <$> (ExportF CCall ("hs_get_some2_" ++ name) (mkName ("hs_get_some2_" ++ name)) <$> [t|forall a . StablePtr (QueryArrowService a) -> StablePtr a -> $(functype (length inputtypes) ([t| Ptr CString -> Ptr CInt -> CInt -> IO Int|]))|])]

hsQueryAllForeign :: String -> [Type2] -> [Type2] -> DecsQ
hsQueryAllForeign name inputtypes _ = do
    runIO $ putStrLn ("generating foreign " ++ ("hs_get_all_" ++ name))
    sequence [ForeignD <$> (ExportF CCall ("hs_get_all_" ++ name) (mkName ("hs_get_all_" ++ name)) <$> [t|forall a . StablePtr (QueryArrowService a) -> StablePtr a -> $(functype (length inputtypes) ([t| Ptr (Ptr CString) -> Ptr CInt -> IO Int|]))|])]

hsQueryAll2Foreign :: String -> [Type2] -> [Type2] -> DecsQ
hsQueryAll2Foreign name _ _ = do
    runIO $ putStrLn ("generating foreign " ++ ("hs_get_all2_" ++ name))
    sequence [ForeignD <$> (ExportF CCall ("hs_get_all2_" ++ name) (mkName ("hs_get_all2_" ++ name)) <$> [t|forall a . StablePtr (QueryArrowService a) -> StablePtr a -> Ptr CString -> CInt -> Ptr (Ptr CString) -> Ptr CInt -> IO Int|])]

cparam :: Name -> Type2 -> ExpQ
cparam i StringType = [| liftIO $ cstringToText $(varE i) |]
cparam i IntType = [| liftIO $ cstringToInt64 $(varE i) |]
cparam i StringListType = [| liftIO $ cstringArrayToTextList $(varE i) |]

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
                  [IntType] -> [VarP retlen]
                  _ -> [VarP retlen]
    let argList = zipWith cparam argnames inputtypes
    let retp = case outputtypes of
                  [StringType] -> [|textToBuffer $(varE retn) (fromIntegral $(varE retlen))|]
                  [IntType] -> [|textToBuffer $(varE retn) (fromIntegral $(varE retlen))|]
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
      [a] | a == StringType || a == IntType -> do
          let fn2 = mkName ("hs_get_int_" ++ n)
          let retn = mkName ("retn")
          s <- [p|svcptr|]
          p <- [p|sessionptr|]
          let args = map (\i -> "arg" ++ show i) [1..length inputtypes]
          let argnames = map mkName args
          let ps = s : p : map VarP argnames ++ [VarP retn]
          let argList = zipWith cparam argnames inputtypes
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
hsQuerySomeFunction n inputtypes _ = do
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
    let argList = zipWith cparam argnames inputtypes
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
hsQuerySome2Function n inputtypes _ = do
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
    let argList = zipWith cparam argnames inputtypes
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
hsQueryAllFunction n inputtypes _ = do
    let fn = mkName ("get_all_" ++ n)
    let fn2 = mkName ("hs_get_all_" ++ n)
    let retn = mkName ("retn")
    let retlen = mkName ("retlen")
    s <- [p|svcptr|]
    p <- [p|sessionptr|]
    let args = map (\i -> "arg" ++ show i) [1..length inputtypes]
    let argnames = map mkName args
    let ps = s : p : map VarP argnames ++ [VarP retn, VarP retlen]
    let argList = zipWith cparam argnames inputtypes
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
hsQueryAll2Function n inputtypes _ = do
    let fn = mkName ("get_all2_" ++ n)
    let fn2 = mkName ("hs_get_all2_" ++ n)
    let retn = mkName "retn"
    let retlen = mkName "retlen"
    let s = mkName "svcptr"
    let p = mkName "sessionptr"
    let listarg = mkName "listarg"
    let cargs = mkName "cargs"
    let cnargs = mkName "cnargs"
    let ps = [VarP s, VarP p, VarP cargs, VarP cnargs, VarP retn, VarP retlen]
    let retp = [|arrayToAllocateBuffer $(varE retn) $(varE retlen)|]
    runIO $ putStrLn ("generating function " ++ show fn2)
    let app = [|$(varE fn) svc session $(varE listarg)|]
    let b0 = [|do
                  let nargs = fromIntegral $(varE cnargs)
                  $(varP listarg) <- liftIO $ peekArray nargs $(varE cargs) >>= mapM peekCString 
                  svc <- liftIO $ deRefStablePtr svcptr
                  session <- liftIO $ deRefStablePtr sessionptr
                  $(app)|]
    b <- [|processRes2 $(b0) $(retp)|]
    sequence [funD fn2 [return (Clause ps (NormalB b) [])]]

createFunction :: String -> [Type2] -> DecQ
createFunction n pts = trace ("creating create function " ++ n ++ " " ++ show pts) $ do
    let a = length pts
    let fn = mkName ("create_" ++ n)
    let pn = stringE("D_" ++ map toUpper n)
    s <- [p|svcptr|]
    p <- [p|session|]
    let args = map (\i -> "arg" ++ show i) [1..a]
    let argnames = map mkName args
    let ps = s : p : map VarP argnames
    let argList = listE (zipWith (\i it -> [| (Var $(stringE i), $(param i it)) |]) args pts)
    let argList2 = listE (map (\i -> [| var $(stringE i)|]) args)
    let func = [|execAbstract|]
    b <- [|$(func) svcptr session ( ($(pn) @@+ $(argList2))) (Map.fromList $(argList))|]
    funD fn [return (Clause ps (NormalB b) [])]

hsCreateForeign :: String -> [Type2] -> DecQ
hsCreateForeign n pts = do
    let a = length pts
    ForeignD <$> (ExportF CCall ("hs_create_" ++ n) (mkName ("hs_create_" ++ n)) <$> [t|forall a . StablePtr (QueryArrowService a) -> StablePtr a -> $(functype a [t|IO Int|])|])

hsUpdateForeign :: String -> [Type2] -> DecQ
hsUpdateForeign n pts = do
    let a = length pts
    ForeignD <$> (ExportF CCall ("hs_update_" ++ n) (mkName ("hs_create_" ++ n)) <$> [t|forall a . StablePtr (QueryArrowService a) -> StablePtr a -> $(functype a [t|IO Int|])|])

hsCreateFunction :: String -> [Type2] -> DecQ
hsCreateFunction n pts = do
    let a = length pts
    let fn = mkName ("create_" ++ n)
    let fn2 = mkName ("hs_create_" ++ n)
    s <- [p|svcptr|]
    p <- [p|sessionptr|]
    let args = map (\i -> "arg" ++ show i) [1..a]
    let argnames = map mkName args
    let ps = s : p : map VarP argnames
    let argList = zipWith cparam argnames pts
    let app = foldl (\expr name -> [|$(expr) $(varE name)|]) [|$(varE fn) svc session|] argnames
    let b0 = foldl (\expr (arg, name) -> [|do
                                        $(varP name) <- $(arg)
                                        $(expr)|]) [|do
                                                          svc <- liftIO $ deRefStablePtr svcptr
                                                          session <- liftIO $ deRefStablePtr sessionptr
                                                          $(app)|] (zip argList argnames)
    b <- [|processRes $(b0) (const (return ()))|]
    funD fn2 [return (Clause ps (NormalB b) [])]

typeToConverter :: Type2 -> ExpQ
typeToConverter StringType = [| \ x -> AbstractResultValue (StringValue (Text.pack x)) |] 
typeToConverter IntType = [| \ x -> AbstractResultValue (Int64Value (read x)) |]

createFunctionArray :: String -> [Type2] -> DecQ
createFunctionArray n pts = do
    let a = length pts
    let fn = mkName ("array_create_" ++ n)
    let pn = stringE("D_" ++ map toUpper n)
    s <- [p|svcptr|]
    p <- [p|session|]
    p2 <- [p|argarray|]
    let args = map (\i -> "arg" ++ show i) [1..a]
    let ps = [s, p , p2]
    let argList = listE (map (\i -> [| Var $(stringE i) |]) args)
    let argList2 = listE (map (\i -> [| var $(stringE i)|]) args)
    let func = [|execAbstract|]
    b <- [|$(func) svcptr session ( ($(pn) @@+ $(argList2))) (Map.fromList (zip $(argList) (zipWith ($) $(listE (map typeToConverter pts)) argarray)))|]
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
                  argarray2 <- liftIO $ peekArray (fromIntegral len) argarray >>= mapM peekCString
                  svc <- liftIO $ deRefStablePtr svcptr
                  session <- liftIO $ deRefStablePtr sessionptr
                  $(varE fn) svc session argarray2|]
    b <- [|processRes $(b0) (const (return ()))|]
    funD fn2 [return (Clause ps (NormalB b) [])]

deleteFunction :: String -> [Type2] -> DecQ
deleteFunction n pts = do
    let a = length pts
    let fn = mkName ("delete_" ++ n)
    let pn = stringE("D_" ++ map toUpper n)

    s <- [p|svcptr|]
    p <- [p|session|]
    let args = map (\i -> "arg" ++ show i) [1..a]
    let argnames = map mkName args
    let ps = s : p : map VarP argnames
    let argList = listE (zipWith (\i it -> [| (Var $(stringE i), $(param i it)) |]) args pts)
    let argList2 = listE (map (\i -> [| var $(stringE i)|]) args)
    let func = [|execAbstract|]
    b <- [|$(func) svcptr session ( ($(pn) @@- $(argList2))) (Map.fromList $(argList))|]
    funD fn [return (Clause ps (NormalB b) [])]

hsDeleteForeign :: String -> [Type2] -> DecQ
hsDeleteForeign n pts = do
    let a = length pts
    ForeignD <$> (ExportF CCall ("hs_delete_" ++ n) (mkName ("hs_delete_" ++ n)) <$> [t|forall a . StablePtr (QueryArrowService a) -> StablePtr a -> $(functype a [t|IO Int|])|])

hsDeleteFunction :: String -> [Type2] -> DecQ
hsDeleteFunction n pts = do
    let a = length pts
    let fn = mkName ("delete_" ++ n)
    let fn2 = mkName ("hs_delete_" ++ n)
    s <- [p|svcptr|]
    p <- [p|sessionptr|]
    let args = map (\i -> "arg" ++ show i) [1..a]
    let argnames = map mkName args
    let ps = s : p : map VarP argnames
    let argList = zipWith cparam argnames pts
    let app = foldl (\expr name -> [|$(expr) $(varE name)|]) [|$(varE fn) svc session|] argnames
    let b0 = foldl (\expr (arg, name) -> [|do
                                        $(varP name) <- $(arg)
                                        $(expr)|]) [|do
                                                          svc <- liftIO $ deRefStablePtr svcptr
                                                          session <- liftIO $ deRefStablePtr sessionptr
                                                          $(app)|] (zip argList argnames)
    b <- [|processRes $(b0) (const (return ()))|]
    funD fn2 [return (Clause ps (NormalB b) [])]

data TemplateConfig = TemplateConfig {
  template_translation :: ICATTranslationConnInfo,
  template_db :: PostgreSQLDBConfig
}

(deriveJSON defaultOptions ''TemplateConfig)

getRewritingRules :: String -> IO (([InsertRewritingRule], [InsertRewritingRule], [InsertRewritingRule]), [Pred])
getRewritingRules path = do
    transinfo <- getConfig path
    let fsconf = template_db transinfo
    db <- makeICATSQLDBAdapter (db_namespace fsconf) (db_predicates fsconf) (db_sql_mapping fsconf) (Just "nextid") ()
    let predmap0 = constructDBPredMap db
    putStrLn "predicates loaded "
    -- print predmap0
    -- trace ("preds:\n" ++ intercalate "\n" (map show (elems predmap0))) $ return ()
    let fsconf2 = template_translation transinfo
    (rewriting, workspace, _) <- getRewriting predmap0 fsconf2
    return (rewriting, Map.elems (allObjects workspace))

functions :: String -> DecsQ
functions path = do
    ((qr, ir, dr), preds) <- runIO (getRewritingRules path)
    let ptm = constructPredTypeMap preds
    let                     getOutputTypes [] = []
                            getOutputTypes (ParamType _ _ True Int64Type : t) = IntType : getOutputTypes t
                            getOutputTypes (ParamType _ _ True TextType : t) = StringType : getOutputTypes t
                            getOutputTypes t = error ("getTypes: error unsupported type " ++ show t)
    let                     getInputTypes [] = []
                            getInputTypes (ParamType _ True _ Int64Type : t) = IntType : getInputTypes t
                            getInputTypes (ParamType _ True _ TextType : t) = StringType : getInputTypes t
                            getInputTypes (ParamType _ True _ (ListType TextType) : t) = StringListType : getInputTypes t
                            getInputTypes t = error ("getTypes: error unsupported type " ++ show t)
    qr1 <- concat <$> mapM (\(InsertRewritingRule (Atom ( name@(ObjectPath _ n0)  ) _) _) ->
                        let n = map toLower (drop 2 n0)
                            (PredType _ ts) = fromMaybe (error "error") (lookup name ptm)
                            getInputOutputTypes [] = ([], [])
                            getInputOutputTypes (ParamType _ _ False Int64Type : t) = ((IntType :) *** id) (getInputOutputTypes t)
                            getInputOutputTypes (ParamType _ _ False TextType : t) = ((StringType :) *** id) (getInputOutputTypes t)
                            getInputOutputTypes (ParamType _ _ False (ListType TextType) : t) = ((StringListType :) *** id) (getInputOutputTypes t)
                            getInputOutputTypes (ParamType _ _ True Int64Type : t) = ([], IntType : getOutputTypes t)
                            getInputOutputTypes (ParamType _ _ True TextType : t) = ([], StringType : getOutputTypes t)
                            getInputOutputTypes t = error ("getInputOutputTypes: error unsupported type " ++ show t)
                            (inputtypes, outputtypes) = getInputOutputTypes ts in
                            concat <$> sequence [
                              queryFunction n inputtypes outputtypes, hsQueryFunction n inputtypes outputtypes, hsQueryForeign n inputtypes outputtypes,
                              queryLongFunction n inputtypes outputtypes, hsQueryLongFunction n inputtypes outputtypes, hsQueryLongForeign n inputtypes outputtypes,
                              querySomeFunction n inputtypes outputtypes, hsQuerySomeFunction n inputtypes outputtypes, hsQuerySomeForeign n inputtypes outputtypes,
                              hsQuerySome2Function n inputtypes outputtypes, hsQuerySome2Foreign n inputtypes outputtypes,
                              queryAllFunction n inputtypes outputtypes, hsQueryAllFunction n inputtypes outputtypes, hsQueryAllForeign n inputtypes outputtypes,
                              queryAll2Function n inputtypes outputtypes, hsQueryAll2Function n inputtypes outputtypes, hsQueryAll2Foreign n inputtypes outputtypes]
                    ) qr
    ir1 <- concat <$> mapM (\(InsertRewritingRule (Atom name@(ObjectPath _ n0) args) _) ->
                        let n = map toLower (drop 2 n0) 
                            (PredType _ ts) = fromMaybe (error "error") (lookup name ptm)
                            inputtypes = getInputTypes ts in
                            sequence [createFunction n inputtypes, hsCreateFunction n inputtypes, hsCreateForeign n inputtypes, hsUpdateForeign n inputtypes,
                              createFunctionArray n inputtypes, hsCreateFunctionArray n, hsCreateForeignArray n]
                    ) ir
    dr1 <- concat <$> mapM (\(InsertRewritingRule (Atom name@(ObjectPath _ n0) args) _) ->
                        let n = map toLower (drop 2 n0)
                            (PredType _ ts) = fromMaybe (error "error") (lookup name ptm)
                            inputtypes = getInputTypes ts in
                            sequence [deleteFunction n inputtypes, hsDeleteFunction n inputtypes, hsDeleteForeign n inputtypes]
                    ) dr
    return (qr1 ++ ir1 ++ dr1)
