{-# LANGUAGE MultiParamTypeClasses, TypeFamilies #-}
module QueryArrow.FileSystem.Query where

import QueryArrow.FO.Data
import QueryArrow.DB.GenericDatabase
import QueryArrow.FileSystem.Builtin
import QueryArrow.FileSystem.Commands

import Control.Monad
import Data.Convertible
import Data.Text (Text)
import qualified Data.Text as T
import Data.Set (Set)
import qualified Data.Set as Set
import System.FilePath
import Data.Time.Clock.POSIX
import System.FilePath.Find
import qualified Data.ByteString as BS

data FileSystemConnInfo = FileSystemConnInfo

data FileSystemTrans = FileSystemTrans {rootDir :: String, predNS :: String}

fsSupported :: String -> Formula -> Set Var -> Bool
fsSupported _ (FAtomic (Atom (FilePathPredName _) _)) env = True
fsSupported _ (FAtomic (Atom (DirPathPredName _) _)) env = True
fsSupported _ (FAtomic (Atom (FileNamePredName _) _)) env = True
fsSupported _ (FAtomic (Atom (DirNamePredName _) _)) env = True
fsSupported _ (FAtomic (Atom (FileSizePredName _) _)) env = True
-- fsSupported _ (FAtomic (Atom (FileCreateTimePredName _) _)) env = True
-- fsSupported _ (FAtomic (Atom (DirCreateTimePredName _) _)) env = True
fsSupported _ (FAtomic (Atom (FileModifyTimePredName _) _)) env = True
fsSupported _ (FAtomic (Atom (DirModifyTimePredName _) _)) env = True
fsSupported _ (FAtomic (Atom (NewFileObjectPredName _) _)) env = True
fsSupported _ (FAtomic (Atom (NewDirObjectPredName _) _)) env = True
fsSupported _ (FAtomic (Atom (DirContentPredName _) _)) env = True
fsSupported _ (FAtomic (Atom (FileContentPredName _) _)) env = True
fsSupported _ (FAtomic (Atom (DirDirPredName _) [arg1, arg2])) env = True
fsSupported _ (FAtomic (Atom (FileDirPredName _) [arg1, arg2])) env = True
fsSupported _ (FAtomic (Atom (FileContentRangePredName _) _)) env = True
fsSupported _ (FInsert (Lit Pos (Atom (FileNamePredName _) _))) env = True
fsSupported _ (FInsert (Lit Pos (Atom (DirNamePredName _) _))) env = True
fsSupported _ (FInsert (Lit Pos (Atom (FileContentPredName _) _))) env = True
fsSupported _ (FInsert (Lit Pos (Atom (DirContentPredName _) _))) env = True
fsSupported _ (FInsert (Lit Pos (Atom (DirDirPredName _) _))) env = True
fsSupported _ (FInsert (Lit Pos (Atom (FileDirPredName _) _))) env = True
fsSupported _ (FInsert (Lit Pos (Atom (FileNamePredName _) _))) env = True
fsSupported _ (FInsert (Lit Pos (Atom (DirNamePredName _) _))) env = True
fsSupported _ (FInsert (Lit Pos (Atom (FileContentRangePredName _) _))) env = True
fsSupported _ (FInsert (Lit Neg (Atom (FileObjectPredName _) _))) env = True
fsSupported _ (FInsert (Lit Neg (Atom (DirObjectPredName _) _))) env = True
fsSupported _ _ _ = False

data FileObject = NewFileObject String | ExistingFileObject String

data DirObject = NewDirObject String | ExistingDirObject String

newtype FileContent = FileContent String

newtype DirContent = DirContent String

instance Convertible FileObject Text where
  safeConvert (NewFileObject  n) = Right (T.pack ("*" ++ n))
  safeConvert (ExistingFileObject n) = Right (T.pack n)

instance Convertible Text FileObject where
  safeConvert t =
    let s = T.unpack t in
        Right (case s of
          '*' : n -> NewFileObject  n
          n -> ExistingFileObject n)

instance Convertible DirObject Text where
  safeConvert (NewDirObject  n) = Right (T.pack ("*" ++ n))
  safeConvert (ExistingDirObject n) = Right (T.pack n)

instance Convertible Text DirObject where
  safeConvert t =
    let s = T.unpack t in
        Right (case s of
          '*' : n -> NewDirObject  n
          n -> ExistingDirObject n)

instance Convertible FileContent Text where
  safeConvert (FileContent n) = Right (T.pack n)

instance Convertible Text FileContent where
  safeConvert t =
    Right (FileContent (T.unpack t))

instance Convertible DirContent Text where
  safeConvert (DirContent n) = Right (T.pack n)

instance Convertible Text DirContent where
  safeConvert t =
    Right (DirContent (T.unpack t))

dirObjectPath :: DirObject -> FSProgram String
dirObjectPath o =
  case o of
    NewDirObject _ ->
      stop
    ExistingDirObject absp ->
      return absp

fileObjectPath :: FileObject -> FSProgram String
fileObjectPath o =
  case o of
    NewFileObject _ ->
      stop
    ExistingFileObject absp ->
      return absp

toAP :: String -> String -> String
toAP root p =
    let ps = splitDirectories p in
        case ps of
            ["/"] -> root
            "/" : p2 | not (("." `elem` p2) || (".." `elem` p2)) -> root </> joinPath p2
            _ -> error ("fsTranslateQuery: malformatted path " ++ p)

toRelP :: String -> String -> String
toRelP root absp =
  let roots = splitDirectories root
      absps = splitDirectories absp
  in
      if take (length roots) absps /= roots
        then error "fsTranslateQuery: path error"
        else joinPath ("/" : drop (length roots) absps)


fsTranslateQuery :: FileSystemTrans -> Set Var -> Formula -> Set Var -> FSProgram ()
fsTranslateQuery  _ ret (FAtomic (Atom (NewFileObjectPredName _) [arg1, VarExpr var])) env = do
  n <- T.unpack <$> evalText arg1
  setText var (convert (NewFileObject n))

fsTranslateQuery  _ ret (FAtomic (Atom (NewDirObjectPredName _) [arg1, VarExpr var])) env = do
  n <- T.unpack <$> evalText arg1
  setText var (convert (NewDirObject n))

fsTranslateQuery  (FileSystemTrans root _) ret (FAtomic (Atom (FileNamePredName _) [VarExpr var1, VarExpr var2])) env | not (var1 `Set.member` env || var2 `Set.member` env) = do
  files <- fsfind (return True) ((statusType <$> fileStatus) ==? RegularFile) root
  absp <- foreach files
  let n = takeFileName absp
  setText var1 (convert (ExistingFileObject absp))
  setText var2 (T.pack n)

fsTranslateQuery  (FileSystemTrans root _) ret (FAtomic (Atom (FileNamePredName _) [VarExpr var, arg1])) env | not (var `Set.member` env) = do
  n <- T.unpack <$> evalText arg1
  files <- fsfind (return True) ((statusType <$> fileStatus) ==? RegularFile &&? fileName ==? n) root
  absp <- foreach files
  setText var (convert (ExistingFileObject absp))

fsTranslateQuery  (FileSystemTrans root _) ret (FAtomic (Atom (FileNamePredName _) [arg1, VarExpr var])) env | not (var `Set.member` env) = do
  o <- convert <$> evalText arg1
  absp <- fileObjectPath o
  let n = takeFileName absp
  setText var (T.pack n)

fsTranslateQuery  (FileSystemTrans root _) ret (FAtomic (Atom (FileNamePredName _) [arg1, arg2])) env = do
  o <- convert <$> evalText arg1
  n2 <- T.unpack <$> evalText arg2
  absp <- fileObjectPath o
  let n1 = takeFileName absp
  unless (n1 == n2) stop

fsTranslateQuery  (FileSystemTrans root _) ret (FAtomic (Atom (DirNamePredName _) [VarExpr var1, VarExpr var2])) env | not (var1 `Set.member` env || var2 `Set.member` env) = do
  files <- fsfind (return True) ((statusType <$> fileStatus) ==? Directory) root
  absp <- foreach files
  let n = takeFileName absp
  setText var1 (convert (ExistingFileObject absp))
  setText var2 (T.pack n)

fsTranslateQuery  (FileSystemTrans root _) ret (FAtomic (Atom (DirNamePredName _) [VarExpr var, arg1])) env | not (var `Set.member` env) = do
  n <- T.unpack <$> evalText arg1
  files <- fsfind (return True) ((statusType <$> fileStatus) ==? Directory &&? fileName ==? n) root
  absp <- foreach files
  setText var (convert (ExistingFileObject absp))

fsTranslateQuery  (FileSystemTrans root _) ret (FAtomic (Atom (DirNamePredName _) [arg1, VarExpr var])) env | not (var `Set.member` env) = do
  o <- convert <$> evalText arg1
  absp <- dirObjectPath o
  let n = takeFileName absp
  setText var (T.pack n)

fsTranslateQuery  (FileSystemTrans root _) ret (FAtomic (Atom (DirNamePredName _) [arg1, arg2])) env = do
  o <- convert <$> evalText arg1
  n2 <- T.unpack <$> evalText arg2
  absp <- dirObjectPath o
  let n1 = takeFileName absp
  unless (n1 == n2) stop

fsTranslateQuery  (FileSystemTrans root _) ret (FAtomic (Atom (FileSizePredName _) [VarExpr var1, VarExpr var2])) env | not (var1 `Set.member` env || var2 `Set.member` env) = do
  files <- fsfind (return True) ((statusType <$> fileStatus) ==? Directory) root
  absp <- foreach files
  s <- size absp
  setText var1 (convert (ExistingFileObject absp))
  setInteger var2 s

fsTranslateQuery  (FileSystemTrans root _) ret (FAtomic (Atom (FileSizePredName _) [arg1, VarExpr var2])) env | not (var2 `Set.member` env) = do
  o <- convert <$> evalText arg1
  absp <- fileObjectPath o
  s <- size absp
  setInteger var2 s

fsTranslateQuery  (FileSystemTrans root _) ret (FAtomic (Atom (FileSizePredName _) [VarExpr var1, arg2])) env | not (var1 `Set.member` env) = do
  a <- evalInteger arg2
  files <- fsfind (return True) ((statusType <$> fileStatus) ==? RegularFile &&? fileSize ==? fromInteger a) root
  absp <- foreach files
  setText var1 (convert (ExistingFileObject absp))

fsTranslateQuery  (FileSystemTrans root _) ret (FAtomic (Atom (FileSizePredName _) [arg1, arg2])) env = do
  o <- convert <$> evalText arg1
  absp <- fileObjectPath o
  s <- size absp
  a <- evalInteger arg2
  unless (s == a) stop

fsTranslateQuery  (FileSystemTrans root _) ret (FAtomic (Atom (FileModifyTimePredName _) [VarExpr var1, VarExpr var2])) env | not (var1 `Set.member` env || var2 `Set.member` env) = do
  files <- fsfind (return True) ((statusType <$> fileStatus) ==? RegularFile) root
  absp <- foreach files
  mt <- fsmodificationTime absp
  setText var1 (convert (ExistingFileObject absp))
  setInteger var2 (floor (utcTimeToPOSIXSeconds mt))

fsTranslateQuery  (FileSystemTrans root _) ret (FAtomic (Atom (FileModifyTimePredName _) [VarExpr var, arg1])) env | not (var `Set.member` env) = do
  mt <- evalInteger arg1
  files <- fsfind (return True) ((statusType <$> fileStatus) ==? RegularFile &&? modificationTime ==? fromInteger mt) root
  absp <- foreach files
  setText var (convert (ExistingFileObject absp))

fsTranslateQuery  (FileSystemTrans root _) ret (FAtomic (Atom (FileModifyTimePredName _) [arg1, VarExpr var])) env | not (var `Set.member` env) = do
  o <- convert <$> evalText arg1
  absp <- fileObjectPath o
  mt <- fsmodificationTime absp
  setInteger var (floor (utcTimeToPOSIXSeconds mt))

fsTranslateQuery  (FileSystemTrans root _) ret (FAtomic (Atom (FileModifyTimePredName _) [arg1, arg2])) env = do
  o <- convert <$> evalText arg1
  absp <- fileObjectPath o
  mt1 <- fsmodificationTime absp
  mt2 <- evalInteger arg1
  unless (floor (utcTimeToPOSIXSeconds mt1) == mt2) stop

fsTranslateQuery  (FileSystemTrans root _) ret (FAtomic (Atom (DirModifyTimePredName _) [VarExpr var1, VarExpr var2])) env | not (var1 `Set.member` env || var2 `Set.member` env) = do
  files <- fsfind (return True) ((statusType <$> fileStatus) ==? Directory) root
  absp <- foreach files
  mt <- fsmodificationTime absp
  setText var1 (convert (ExistingDirObject absp))
  setInteger var2 (floor (utcTimeToPOSIXSeconds mt))

fsTranslateQuery  (FileSystemTrans root _) ret (FAtomic (Atom (DirModifyTimePredName _) [VarExpr var, arg1])) env | not (var `Set.member` env) = do
  mt <- evalInteger arg1
  files <- fsfind (return True) ((statusType <$> fileStatus) ==? Directory &&? modificationTime ==? fromInteger mt) root
  absp <- foreach files
  setText var (convert (ExistingDirObject absp))

fsTranslateQuery  (FileSystemTrans root _) ret (FAtomic (Atom (DirModifyTimePredName _) [arg1, VarExpr var])) env | not (var `Set.member` env) = do
  o <- convert <$> evalText arg1
  absp <- fileObjectPath o
  mt <- fsmodificationTime absp
  setInteger var (floor (utcTimeToPOSIXSeconds mt))

fsTranslateQuery  (FileSystemTrans root _) ret (FAtomic (Atom (DirModifyTimePredName _) [arg1, arg2])) env = do
  o <- convert <$> evalText arg1
  absp <- dirObjectPath o
  mt1 <- fsmodificationTime absp
  mt2 <- evalInteger arg1
  unless (floor (utcTimeToPOSIXSeconds mt1) == mt2) stop

fsTranslateQuery  (FileSystemTrans root _) ret (FAtomic (Atom (FilePathPredName _) [VarExpr var1, VarExpr var2])) env | not (var1 `Set.member` env || var2 `Set.member` env) = do
  files <- fsfind (return True) ((statusType <$> fileStatus) ==? RegularFile) root
  absp <- foreach files
  let p = toRelP root absp
  setText var1 (convert (ExistingFileObject absp))
  setText var2 (T.pack p)

fsTranslateQuery  (FileSystemTrans root _) ret (FAtomic (Atom (FilePathPredName _) [VarExpr var, arg1])) env | not (var `Set.member` env) = do
  p <- T.unpack <$> evalText arg1
  let absp = toAP root p
  b <- fileExists absp
  if b
    then
      setText var (convert (ExistingFileObject absp))
    else
      stop

fsTranslateQuery  (FileSystemTrans root _) ret (FAtomic (Atom (FilePathPredName _) [arg1, VarExpr var])) env | not (var `Set.member` env) = do
  o <- convert <$> evalText arg1
  absp <- fileObjectPath o
  let p = toRelP root absp
  setText var (T.pack p)

fsTranslateQuery  (FileSystemTrans root _) ret (FAtomic (Atom (FilePathPredName _) [arg1, arg2])) env = do
  o <- convert <$> evalText arg1
  p <- T.unpack <$> evalText arg2
  absp1 <- fileObjectPath o
  let absp2 = toAP root p
  unless (equalFilePath absp1 absp2) stop

fsTranslateQuery  (FileSystemTrans root _) ret (FAtomic (Atom (DirPathPredName _) [VarExpr var1, VarExpr var2])) env | not (var1 `Set.member` env || var2 `Set.member` env) = do
  files <- fsfind (return True) ((statusType <$> fileStatus) ==? Directory) root
  absp <- foreach files
  let p = toRelP root absp
  setText var1 (convert (ExistingDirObject absp))
  setText var2 (T.pack p)

fsTranslateQuery  (FileSystemTrans root _) ret (FAtomic (Atom (DirPathPredName _) [VarExpr var, arg1])) env | not (var `Set.member` env) = do
  p <- T.unpack <$> evalText arg1
  let absp = toAP root p
  b <- dirExists absp
  if b
    then
      setText var (convert (ExistingDirObject absp))
    else
      stop

fsTranslateQuery  (FileSystemTrans root _) ret (FAtomic (Atom (DirPathPredName _) [arg1, VarExpr var])) env | not (var `Set.member` env) = do
  o <- convert <$> evalText arg1
  absp <- dirObjectPath o
  let p = toRelP root absp
  setText var (T.pack p)

fsTranslateQuery  (FileSystemTrans root _) ret (FAtomic (Atom (DirPathPredName _) [arg1, arg2])) env = do
  o <- convert <$> evalText arg1
  p <- T.unpack <$> evalText arg2
  absp1 <- dirObjectPath o
  let absp2 = toAP root p
  unless (equalFilePath absp1 absp2) stop

fsTranslateQuery  (FileSystemTrans root _) ret (FAtomic (Atom (FileContentPredName _) [VarExpr var1, VarExpr var2])) env | not (var1 `Set.member` env || (var2 `Set.member` env))= do
  files <- fsfind (return True) ((statusType <$> fileStatus) ==? RegularFile) root
  absp <- foreach files
  setText var1 (convert (ExistingFileObject absp))
  setText var2 (convert (FileContent absp))

fsTranslateQuery  _ ret (FAtomic (Atom (FileContentPredName _) [arg1, VarExpr var])) env | not (var `Set.member` env) = do
  o <- convert <$> evalText arg1
  absp <- fileObjectPath o
  setText var (convert (FileContent absp))

fsTranslateQuery  _ ret (FAtomic (Atom (FileContentPredName _) [arg1, VarExpr var])) env | not (var `Set.member` env) = do
  FileContent absp <- convert <$> evalText arg1
  setText var (convert (ExistingFileObject absp))

fsTranslateQuery  _ ret (FAtomic (Atom (FileContentPredName _) [arg1, arg2])) env = do
  o <- convert <$> evalText arg1
  absp1 <- fileObjectPath o
  FileContent absp2 <- convert <$> evalText arg1
  unless (equalFilePath absp1 absp2) stop

fsTranslateQuery  (FileSystemTrans root _) ret (FAtomic (Atom (DirContentPredName _) [VarExpr var1, VarExpr var2])) env | not (var1 `Set.member` env || (var2 `Set.member` env))= do
  files <- fsfind (return True) ((statusType <$> fileStatus) ==? RegularFile) root
  absp <- foreach files
  setText var1 (convert (ExistingDirObject absp))
  setText var2 (convert (DirContent absp))

fsTranslateQuery  _ ret (FAtomic (Atom (DirContentPredName _) [arg1, VarExpr var])) env | not (var `Set.member` env) = do
  o <- convert <$> evalText arg1
  absp <- dirObjectPath o
  setText var (convert (DirContent absp))

fsTranslateQuery  _ ret (FAtomic (Atom (DirContentPredName _) [arg1, VarExpr var])) env | not (var `Set.member` env) = do
  FileContent absp <- convert <$> evalText arg1
  setText var (convert (DirContent absp))

fsTranslateQuery  _ ret (FAtomic (Atom (DirContentPredName _) [arg1, arg2])) env = do
  o <- convert <$> evalText arg1
  absp1 <- dirObjectPath o
  FileContent absp2 <- convert <$> evalText arg1
  unless (equalFilePath absp1 absp2) stop

fsTranslateQuery  (FileSystemTrans root _) ret (FAtomic (Atom (DirDirPredName _) [VarExpr var1, VarExpr var2])) env | not (var1 `Set.member` env || var2 `Set.member` env) = do
  files <- fsfind (return True) ((statusType <$> fileStatus) ==? Directory &&? filePath /=? root) root
  absp <- foreach files
  let absp2 = takeDirectory absp
  setText var1 (convert (ExistingDirObject absp))
  setText var2 (convert (ExistingDirObject absp2))

fsTranslateQuery  (FileSystemTrans root _) ret (FAtomic (Atom (DirDirPredName _) [arg1, VarExpr var])) env | not (var `Set.member` env) = do
  o <- convert <$> evalText arg1
  absp <- dirObjectPath o
  when (equalFilePath absp root) stop
  let absp2 = takeDirectory absp
  setText var (convert (ExistingDirObject absp2))

fsTranslateQuery  _ ret (FAtomic (Atom (DirDirPredName _) [VarExpr var, arg1])) env | not (var `Set.member` env) = do
  o <- convert <$> evalText arg1
  absp <- dirObjectPath o
  absp2s <- listDirDir absp
  absp2 <- foreach absp2s
  setText var (convert (ExistingDirObject absp2))

fsTranslateQuery  _ ret (FAtomic (Atom (DirDirPredName _) [arg1, arg2])) env = do
  o <- convert <$>  evalText arg2
  absp <- dirObjectPath o
  o2 <- convert <$> evalText arg1
  absp2 <- dirObjectPath o2
  let seg = splitDirectories absp
  let seg2 = splitDirectories absp2
  unless (length seg2 == length seg + 1 &&
          take (length seg) seg2 == seg) stop

fsTranslateQuery  (FileSystemTrans root _) ret (FAtomic (Atom (FileDirPredName _) [VarExpr var1, VarExpr var2])) env | not (var1 `Set.member` env || var2 `Set.member` env) = do
  files <- fsfind (return True) ((statusType <$> fileStatus) ==? RegularFile &&? filePath /=? root) root
  absp <- foreach files
  let absp2 = takeDirectory absp
  setText var1 (convert (ExistingFileObject absp))
  setText var2 (convert (ExistingDirObject absp2))

fsTranslateQuery  (FileSystemTrans root _) ret (FAtomic (Atom (FileDirPredName _) [arg1, VarExpr var])) env | not (var `Set.member` env) = do
  o <- convert <$> evalText arg1
  absp <- fileObjectPath o
  when (equalFilePath absp root) stop
  let absp2 = takeDirectory absp
  setText var (convert (ExistingDirObject absp2))

fsTranslateQuery  _ ret (FAtomic (Atom (FileDirPredName _) [VarExpr var, arg1])) env | not (var `Set.member` env) = do
  o <- convert <$> evalText arg1
  absp <- fileObjectPath o
  absp2s <- listDirFile absp
  absp2 <- foreach absp2s
  setText var (convert (ExistingFileObject absp2))

fsTranslateQuery  _ ret (FAtomic (Atom (FileDirPredName _) [arg1, arg2])) env = do
  o <- convert <$>  evalText arg2
  absp <- fileObjectPath o
  o2 <- convert <$> evalText arg1
  absp2 <- fileObjectPath o2
  let seg = splitDirectories absp
  let seg2 = splitDirectories absp2
  unless (length seg2 == length seg + 1 &&
          take (length seg) seg2 == seg) stop

fsTranslateQuery  (FileSystemTrans root _) ret (FAtomic (Atom (FileContentRangePredName _) [VarExpr var1, VarExpr var2, VarExpr var3, VarExpr var4])) env | not (var1 `Set.member` env || var2 `Set.member` env || var3 `Set.member` env || var4 `Set.member` env) = do
  files <- fsfind (return True) ((statusType <$> fileStatus) ==? RegularFile) root
  absp <- foreach files
  s <- size absp
  a <- foreachInteger [0..s]
  b <- foreachInteger [a..s]
  c <- fsread absp a b
  setText var1 (convert (FileContent absp))
  setInteger var2 a
  setInteger var3 b
  setByteString var4 c

fsTranslateQuery  (FileSystemTrans root _) ret (FAtomic (Atom (FileContentRangePredName _) [VarExpr var1, arg2, VarExpr var3, VarExpr var4])) env | not (var1 `Set.member` env || var3 `Set.member` env || var4 `Set.member` env) = do
  files <- fsfind (return True) ((statusType <$> fileStatus) ==? RegularFile) root
  absp <- foreach files
  s <- size absp
  a <- evalInteger arg2
  unless (a >= 0 && a <= s) stop
  b <- foreachInteger [a..s]
  c <- fsread absp a b
  setText var1 (convert (FileContent absp))
  setInteger var3 b
  setByteString var4 c

fsTranslateQuery  (FileSystemTrans root _) ret (FAtomic (Atom (FileContentRangePredName _) [VarExpr var1, VarExpr var2, arg3, VarExpr var4])) env | not (var1 `Set.member` env || var2 `Set.member` env || var4 `Set.member` env) = do
  files <- fsfind (return True) ((statusType <$> fileStatus) ==? RegularFile) root
  absp <- foreach files
  s <- size absp
  b <- evalInteger arg3
  unless (b >= 0 && b <= s) stop
  a <- foreachInteger [0..b]
  c <- fsread absp a b
  setText var1 (convert (FileContent absp))
  setInteger var2 a
  setByteString var4 c

fsTranslateQuery  (FileSystemTrans root _) ret (FAtomic (Atom (FileContentRangePredName _) [VarExpr var1, arg2, arg3, VarExpr var4])) env | not (var1 `Set.member` env || var4 `Set.member` env) = do
  files <- fsfind (return True) ((statusType <$> fileStatus) ==? RegularFile) root
  absp <- foreach files
  s <- size absp
  a <- evalInteger arg2
  b <- evalInteger arg3
  unless (a >= 0 && b <= s && a <= b) stop
  c <- fsread absp a b
  setText var1 (convert (FileContent absp))
  setByteString var4 c

fsTranslateQuery  (FileSystemTrans root _) ret (FAtomic (Atom (FileContentRangePredName _) [VarExpr var1, VarExpr var2, VarExpr var3, arg4])) env | not (var1 `Set.member` env || var2 `Set.member` env || var3 `Set.member` env) = do
  files <- fsfind (return True) ((statusType <$> fileStatus) ==? RegularFile) root
  absp <- foreach files
  s <- size absp
  d <- evalByteString arg4
  let l = BS.length d
  a <- foreachInteger [0..s-fromIntegral l]
  let b = a + fromIntegral l
  c <- fsread absp a b
  unless (c == d) stop
  setText var1 (convert (FileContent absp))
  setInteger var2 a
  setInteger var3 b

fsTranslateQuery  (FileSystemTrans root _) ret (FAtomic (Atom (FileContentRangePredName _) [VarExpr var1, arg2, VarExpr var3, arg4])) env | not (var1 `Set.member` env || var3 `Set.member` env) = do
  files <- fsfind (return True) ((statusType <$> fileStatus) ==? RegularFile) root
  absp <- foreach files
  s <- size absp
  d <- evalByteString arg4
  let l = BS.length d
  a <- evalInteger arg2
  let b = a + fromIntegral l
  unless (a >= 0 && b <= s) stop
  c <- fsread absp a b
  unless (c == d) stop
  setText var1 (convert (FileContent absp))
  setInteger var3 b

fsTranslateQuery  (FileSystemTrans root _) ret (FAtomic (Atom (FileContentRangePredName _) [VarExpr var1, VarExpr var2, arg3, arg4])) env | not (var1 `Set.member` env || var2 `Set.member` env) = do
  files <- fsfind (return True) ((statusType <$> fileStatus) ==? RegularFile) root
  absp <- foreach files
  s <- size absp
  d <- evalByteString arg4
  let l = BS.length d
  b <- evalInteger arg3
  let a = b - fromIntegral l
  unless (a >= 0 && b <= s) stop
  c <- fsread absp a b
  unless (c == d) stop
  setText var1 (convert (FileContent absp))
  setInteger var2 a

fsTranslateQuery  (FileSystemTrans root _) ret (FAtomic (Atom (FileContentRangePredName _) [VarExpr var1, arg2, arg3, arg4])) env | not (var1 `Set.member` env) = do
  files <- fsfind (return True) ((statusType <$> fileStatus) ==? RegularFile) root
  absp <- foreach files
  s <- size absp
  a <- evalInteger arg2
  b <- evalInteger arg3
  d <- evalByteString arg4
  unless (a >= 0 && b <= s && a <= b) stop
  c <- fsread absp a b
  unless (c == d) stop
  setText var1 (convert (FileContent absp))

fsTranslateQuery  (FileSystemTrans root _) ret (FAtomic (Atom (FileContentRangePredName _) [arg1, VarExpr var2, VarExpr var3, VarExpr var4])) env | not (var2 `Set.member` env || var3 `Set.member` env || var4 `Set.member` env) = do
  FileContent absp <- convert <$> evalText arg1
  s <- size absp
  a <- foreachInteger [0..s]
  b <- foreachInteger [a..s]
  c <- fsread absp a b
  setInteger var2 a
  setInteger var3 b
  setByteString var4 c

fsTranslateQuery  (FileSystemTrans root _) ret (FAtomic (Atom (FileContentRangePredName _) [arg1, arg2, VarExpr var3, VarExpr var4])) env | not (var3 `Set.member` env || var4 `Set.member` env) = do
  FileContent absp <- convert <$> evalText arg1
  s <- size absp
  a <- evalInteger arg2
  unless (a >= 0 && a <= s) stop
  b <- foreachInteger [a..s]
  c <- fsread absp a b
  setInteger var3 b
  setByteString var4 c

fsTranslateQuery  (FileSystemTrans root _) ret (FAtomic (Atom (FileContentRangePredName _) [arg1, VarExpr var2, arg3, VarExpr var4])) env | not (var2 `Set.member` env || var4 `Set.member` env) = do
  FileContent absp <- convert <$> evalText arg1
  s <- size absp
  b <- evalInteger arg3
  unless (b >= 0 && b <= s) stop
  a <- foreachInteger [0..b]
  c <- fsread absp a b
  setInteger var2 a
  setByteString var4 c

fsTranslateQuery  (FileSystemTrans root _) ret (FAtomic (Atom (FileContentRangePredName _) [arg1, arg2, arg3, VarExpr var4])) env | not (var4 `Set.member` env) = do
  FileContent absp <- convert <$> evalText arg1
  s <- size absp
  a <- evalInteger arg2
  b <- evalInteger arg3
  unless (a >= 0 && b <= s && a <= b) stop
  c <- fsread absp a b
  setByteString var4 c

fsTranslateQuery  (FileSystemTrans root _) ret (FAtomic (Atom (FileContentRangePredName _) [arg1, VarExpr var2, VarExpr var3, arg4])) env | not (var2 `Set.member` env || var3 `Set.member` env) = do
  FileContent absp <- convert <$> evalText arg1
  s <- size absp
  d <- evalByteString arg4
  let l = BS.length d
  a <- foreachInteger [0..s - fromIntegral l]
  let b = a + fromIntegral l
  c <- fsread absp a b
  unless (c == d) stop
  setInteger var2 a
  setInteger var3 b

fsTranslateQuery  (FileSystemTrans root _) ret (FAtomic (Atom (FileContentRangePredName _) [arg1, arg2, VarExpr var3, arg4])) env | not (var3 `Set.member` env) = do
  FileContent absp <- convert <$> evalText arg1
  s <- size absp
  a <- evalInteger arg2
  d <- evalByteString arg4
  let l = BS.length d
  let b = a + fromIntegral l
  unless (a >= 0 && b <= s) stop
  c <- fsread absp a b
  unless (c == d) stop
  setInteger var3 b

fsTranslateQuery  (FileSystemTrans root _) ret (FAtomic (Atom (FileContentRangePredName _) [arg1, VarExpr var2, arg3, arg4])) env | not (var2 `Set.member` env) = do
  FileContent absp <- convert <$> evalText arg1
  s <- size absp
  b <- evalInteger arg3
  d <- evalByteString arg4
  let l = BS.length d
  let a = b - fromIntegral l
  unless (a >= 0 && b <= s) stop
  c <- fsread absp a b
  unless (c == d) stop
  setInteger var2 a

fsTranslateQuery  (FileSystemTrans root _) ret (FAtomic (Atom (FileContentRangePredName _) [arg1, arg2, arg3, arg4])) env = do
  FileContent absp <- convert <$> evalText arg1
  s <- size absp
  a <- evalInteger arg2
  b <- evalInteger arg3
  d <- evalByteString arg4
  let l = BS.length d
  unless (a >= 0 && b <= s && a + fromIntegral l == b) stop
  c <- fsread absp a b
  unless (c == d) stop

fsTranslateQuery  _ ret (FInsert (Lit Pos (Atom (FileContentPredName _) [arg1, arg2]))) env = do
  o <- convert <$> evalText arg1
  FileContent absp2 <- convert <$> evalText arg2
  case o of
    NewFileObject _ -> error "cannot add content to a new file object"
    ExistingFileObject absp -> fscopyFile absp2 absp

fsTranslateQuery  _ ret (FInsert (Lit Pos (Atom (DirContentPredName _) [arg1, arg2]))) env = do
  o <- convert <$> evalText arg1
  DirContent absp2 <- convert <$> evalText arg2
  case o of
    NewDirObject _ -> error "cannot add content to a new dir object"
    ExistingDirObject absp -> fscopyDir absp2 absp

fsTranslateQuery  _ ret (FInsert (Lit Pos (Atom (FileNamePredName _) [arg1, arg2]))) env = do
  o <- convert <$> evalText arg1
  n <- T.unpack <$> evalText arg2
  case o of
    NewFileObject _ -> error "cannot change name of new file object"
    ExistingFileObject absp -> do
      let absp2 = takeDirectory absp </> n
      unless (equalFilePath absp2 absp) $ moveFile absp absp2

fsTranslateQuery  _ ret (FInsert (Lit Pos (Atom (DirNamePredName _) [arg1, arg2]))) env = do
  o <- convert <$> evalText arg1
  n <- T.unpack <$> evalText arg2
  case o of
    NewFileObject _ -> error "cannot change name of new dir object"
    ExistingFileObject absp -> do
      let absp2 = takeDirectory absp </> n
      unless (equalFilePath absp2 absp) $ moveDir absp absp2

fsTranslateQuery  _ ret (FInsert (Lit Pos (Atom (DirDirPredName _) [arg1, arg2]))) env = do
  o <- convert <$> evalText arg2
  o2 <- convert <$> evalText arg1
  case o of
    NewDirObject _ -> error "cannot add content to a new dir object"
    ExistingDirObject absp ->
      case o2 of
        NewDirObject n2 ->
          makeDir (absp </> n2)
        ExistingDirObject absp2 -> do
          let n2 = takeFileName absp2
          moveDir absp2 (absp </> n2)

fsTranslateQuery  _ ret (FInsert (Lit Pos (Atom (FileDirPredName _) [arg1, arg2]))) env = do
  o <- convert <$> evalText arg2
  o2 <- convert <$> evalText arg1
  case o of
    NewDirObject _ -> error "cannot add content to a new dir object"
    ExistingDirObject absp ->
      case o2 of
        NewFileObject n2 ->
          makeFile (absp </> n2)
        ExistingFileObject absp2 -> do
          let n2 = takeFileName absp2
          moveFile absp2 (absp </> n2)

fsTranslateQuery  _ ret (FInsert (Lit Pos (Atom (FileContentRangePredName _) [arg1, arg2, arg3, arg4]))) env = do
  o <- convert <$> evalText arg1
  a <- evalInteger arg2
  b <- evalInteger arg3
  c <- evalText arg4
  case o of
    NewFileObject _ -> error "cannot modify content of a new file object"
    ExistingFileObject absp -> do
      unless (b - a == fromIntegral (T.length c)) $ error "content length error"
      fswrite absp a c


fsTranslateQuery  _ ret (FInsert (Lit Neg (Atom (FileObjectPredName _) [arg]))) env = do
  o <- convert <$> evalText arg
  case o of
    NewFileObject _ -> error "cannot delete a new file object"
    ExistingFileObject absp ->
      unlinkFile absp

fsTranslateQuery  _ ret (FInsert (Lit Neg (Atom (DirObjectPredName _) [arg]))) env = do
  o <- convert <$> evalText arg
  case o of
    NewDirObject _ -> error "cannot delete a new dir object"
    ExistingDirObject absp ->
      removeDir absp

fsTranslateQuery _ _ query _ = error ("fsTranslateQuery: cannot translate query" ++ show query)

instance IGenericDatabase01 FileSystemTrans where
    type GDBQueryType FileSystemTrans = FSProgram ()
    type GDBFormulaType FileSystemTrans = Formula
    gTranslateQuery trans ret query env = return (fsTranslateQuery trans ret query env)
    gCheckQuery _ _ _ _ = return (Right ())
    gSupported (FileSystemTrans _ ns) ret form env = fsSupported ns form env
