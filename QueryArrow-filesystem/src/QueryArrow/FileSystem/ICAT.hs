module QueryArrow.FileSystem.ICAT where

import QueryArrow.FileSystem.Query
import QueryArrow.FileSystem.Builtin

import QueryArrow.DB.GenericDatabase
import QueryArrow.DB.NoConnection

makeFileSystemDBAdapter :: String -> [String] -> FileSystemConnInfo -> IO (NoConnectionDatabase (GenericDatabase FileSystemTrans FileSystemConnInfo))
makeFileSystemDBAdapter ns [rootDir] conn = return (NoConnectionDatabase (GenericDatabase (FileSystemTrans rootDir ns) conn ns
  [
    FileContentPred ns, DirContentPred ns,
    FilePathPred ns, DirPathPred ns,
    FileNamePred ns, DirNamePred ns,
    FileObjectPred ns, DirObjectPred ns,
    NewFileObjectPred ns, NewDirObjectPred ns,
    FileDirPred ns, DirDirPred ns,
    FileContentRangePred ns]))
