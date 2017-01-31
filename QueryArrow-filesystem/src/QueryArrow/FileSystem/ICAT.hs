module QueryArrow.FileSystem.ICAT where

import QueryArrow.FileSystem.Query
import QueryArrow.FileSystem.Builtin
import QueryArrow.FileSystem.Commands

import QueryArrow.DB.GenericDatabase
import QueryArrow.DB.NoConnection

import Control.Monad

makeFileSystemDBAdapter :: String -> [String] -> String -> Int -> FileSystemConnInfo -> IO (NoConnectionDatabase (GenericDatabase FileSystemTrans FileSystemConn))
makeFileSystemDBAdapter ns (rootDir : hostmap0) host port conninfo = do
  let hostmap1 = distribute hostmap0
  unless ((host, port, rootDir) `elem` hostmap1) (error "root dir not found for local host")
  let hostmap = map (\(host', port', root') -> ((host', root'), (if host == host'
                                                            then makeLocalInterpreter
                                                            else makeRemoteInterpreter) host' port' root')) hostmap1
  let hostmap2 = [((hosta, roota, hostb, rootb), (if host == hosta
                                                      then if host == hostb
                                                        then makeLocalInterpreter2
                                                        else makeLocalToRemoteInterpreter2
                                                      else if host == hostb
                                                        then makeRemoteToLocalInterpreter2
                                                        else makeRemoteToRemoteInterpreter2) hosta porta roota hostb portb rootb) | (hosta, porta, roota) <- hostmap1, (hostb, portb, rootb) <- hostmap1]
  return (NoConnectionDatabase (GenericDatabase (FileSystemTrans host rootDir ns) (FileSystemConn hostmap hostmap2) ns
    [
      FileContentPred ns, DirContentPred ns,
      FilePathPred ns, DirPathPred ns,
      FileNamePred ns, DirNamePred ns,
      FileHostPred ns, DirHostPred ns,
      FileSizePred ns,
      FileModifyTimePred ns, DirModifyTimePred ns,
      FileObjectPred ns, DirObjectPred ns,
      NewFileObjectPred ns, NewDirObjectPred ns,
      FileDirPred ns, DirDirPred ns,
      FileContentRangePred ns])) where
        distribute [] = []
        distribute [_] = error "cannot distribute one element"
        distribute [_, _] = error "cannot distribute two elements"
        distribute (a:b:c:t) = (a,read b,c) : distribute t
