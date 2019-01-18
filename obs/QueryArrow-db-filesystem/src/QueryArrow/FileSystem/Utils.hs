{-# LANGUAGE PatternSynonyms #-}

module QueryArrow.FileSystem.Utils where


import QueryArrow.Utils ()

import System.FilePath
import Control.Exception(throw)
import Control.Monad
import System.Directory

copyDirectory ::  FilePath -> FilePath -> IO ()
copyDirectory src dst = do
  whenM (not <$> doesDirectoryExist src) $
    throw (userError "source does not exist")
  whenM (doesFileExist dst) $
    throw (userError "destination already exists as a file")

  whenM (not <$> doesDirectoryExist dst) $
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
    whenM s r = s >>= flip when r
