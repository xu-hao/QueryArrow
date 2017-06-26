-- https://rosettacode.org/wiki/Find_duplicate_files#Haskell

import Crypto.Hash        (hash, Digest)
import Crypto.Hash.Algorithms (MD5)
import Data.ByteString as BS  (readFile)
import System.Environment     (getArgs, getProgName)
import System.Directory       (doesDirectoryExist, getDirectoryContents)
import System.FilePath.Posix  ((</>), takeFileName)
import Control.Monad          (forM)
import Text.Printf            (printf)
import System.IO              (withFile, IOMode(ReadMode), hFileSize)


type File = (Digest MD5, -- md5hash
             FilePath)      -- filepath

type FileSize = Integer

getRecursiveContents :: FilePath -> FileSize -> IO [File]
getRecursiveContents curDir maxsize = do
  names <- getDirectoryContents curDir
  let dirs = filter (`notElem` [".", ".."]) names
  files <- forM dirs $ \path -> do
             let path' = curDir </> path
             exists <- doesDirectoryExist path'
             if exists
                then getRecursiveContents path' maxsize
                else genFileHash path' maxsize
  return $ concat files


genFileHash :: FilePath -> FileSize -> IO [File]
genFileHash path maxsize = do
  size <- withFile path ReadMode hFileSize
  if size <= maxsize
    then BS.readFile path >>= \bs -> return [(hash bs :: Digest MD5, path)]
    else return []

findDuplicates :: FilePath -> FileSize -> IO ()
findDuplicates dir bytes = do
  exists <- doesDirectoryExist dir
  if exists
    then getRecursiveContents dir bytes >>= findSameHashes
    else printf "Sorry, the directory \"%s\" does not exist...\n" dir

findSameHashes :: [File] -> IO ()
findSameHashes []     = return ()
findSameHashes ((hash, fp):xs) = do
  case lookup hash xs of
    (Just dupFile) -> do
      printf "===========================\n\
                            \Found duplicate:\n\
                            \=> %s \n\
                            \=> %s \n\n" fp dupFile
      writeFile fp ("skip\n" ++ takeFileName dupFile)
      findSameHashes xs
    (_)            -> findSameHashes xs

main :: IO ()
main = do
  args <- getArgs
  case args of
    [dir] -> findDuplicates dir 0
    _ -> do
      name <- getProgName
      printf "Something went wrong - please use ./%s <dir> <bytes>\n" name
