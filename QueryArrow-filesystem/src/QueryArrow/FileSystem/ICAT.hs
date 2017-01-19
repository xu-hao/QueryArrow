module QueryArrow.FileSystem.ICAT where

import Data.Text

import QueryArrow.FileSystem.Query

import QueryArrow.DB.GenericDatabase
import QueryArrow.DB.NoConnection

makeFileSystemDBAdapter :: String -> [String] -> FileSystemConnInfo -> IO (NoConnectionDatabase (GenericDatabase FileSystemTrans FileSystemConnInfo))
makeFileSystemDBAdapter ns [rootDir] conn = return (NoConnectionDatabase (GenericDatabase (FileSystemTrans  rootDir ns) conn ns
  [FileContentPred ns, DirContentPred ns, FilePred ns, DirPred ns, FileContentPred ns]))
