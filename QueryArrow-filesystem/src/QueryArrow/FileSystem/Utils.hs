{-# LANGUAGE PatternSynonyms #-}

module QueryArrow.FileSystem.Utils where


import QueryArrow.FO.Data
import QueryArrow.DB.DB
import QueryArrow.Utils ()

import Prelude hiding (lookup)
import Data.Map.Strict (lookup)
import System.FilePath
import Control.Exception(throw)
import Control.Monad
import System.Directory

pattern PTKeyIO a = ParamType True True True a
pattern PTKeyI a = ParamType True False True a
pattern PTKeyO a = ParamType True True False a
pattern PTPropIO a = ParamType False True True a
pattern PTPropI a = ParamType False True False a
pattern PTPropO a = ParamType False False True a

evalExpr :: MapResultRow -> Expr -> ResultValue
evalExpr row (StringExpr s) = StringValue s
evalExpr row (IntExpr s) = IntValue s
evalExpr row (VarExpr v) = case lookup v row of
    Nothing -> Null
    Just r -> r
evalExpr row expr = error ("evalExpr: unsupported expr " ++ show expr)

copyDirectory ::  FilePath -> FilePath -> IO ()
copyDirectory src dst = do
  whenM (not <$> doesDirectoryExist src) $
    throw (userError "source does not exist")
  whenM (doesFileOrDirectoryExist dst) $
    throw (userError "destination already exists")

  createDirectory dst
  xs <- listDirectory src
  forM_ xs $ \name -> do
    let srcPath = src </> name
    let dstPath = dst </> name
    isDirectory <- doesDirectoryExist srcPath
    if isDirectory
      then copyDirectory srcPath dstPath
      else copyFile srcPath dstPath

  where
    doesFileOrDirectoryExist x = orM [doesDirectoryExist x, doesFileExist x]
    orM xs = or <$> sequence xs
    whenM s r = s >>= flip when r
