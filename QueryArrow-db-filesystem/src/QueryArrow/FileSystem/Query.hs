{-# LANGUAGE MultiParamTypeClasses, TypeFamilies, TypeSynonymInstances, FlexibleInstances, FlexibleContexts #-}
module QueryArrow.FileSystem.Query where

import QueryArrow.FO.Data
import QueryArrow.FO.Types
import QueryArrow.DB.DB
import QueryArrow.DB.GenericDatabase
import QueryArrow.FileSystem.Builtin
import QueryArrow.FileSystem.Commands

import Control.Monad
import Data.Convertible
import Data.Text (Text)
import qualified Data.Text as T
import Data.Set (Set)
import qualified Data.Set as Set
import Data.Time.Clock.POSIX
import qualified Data.ByteString as BS

data FileSystemConnInfo = FileSystemConnInfo

data FileSystemConn = FileSystemConn {hostMap :: [((String, String), Interpreter)], hostMap2 :: [((String, String, String, String), Interpreter2)]}

data FileSystemTrans = FileSystemTrans {fsHost :: String, rootDir :: String, predNS :: String}

fsSupported :: String -> FormulaT -> Set Var -> Bool
fsSupported _ (FAtomic2 _ (Atom (FilePathPredName _) _)) env = True
fsSupported _ (FAtomic2 _ (Atom (DirPathPredName _) _)) env = True
fsSupported _ (FAtomic2 _ (Atom (FileModePredName _) _)) env = True
fsSupported _ (FAtomic2 _ (Atom (DirModePredName _) _)) env = True
fsSupported _ (FAtomic2 _ (Atom (FileIdPredName _) _)) env = True
fsSupported _ (FAtomic2 _ (Atom (DirIdPredName _) _)) env = True
fsSupported _ (FAtomic2 _ (Atom (FileNamePredName _) _)) env = True
fsSupported _ (FAtomic2 _ (Atom (DirNamePredName _) _)) env = True
fsSupported _ (FAtomic2 _ (Atom (FileHostPredName _) _)) env = True
fsSupported _ (FAtomic2 _ (Atom (DirHostPredName _) _)) env = True
fsSupported _ (FAtomic2 _ (Atom (FileSizePredName _) _)) env = True
-- fsSupported _2 _ (FAtomic2 _ (Atom (FileCreateTimePredName _) _)) env = True
-- fsSupported _2 _ (FAtomic2 _ (Atom (DirCreateTimePredName _) _)) env = True
fsSupported _ (FAtomic2 _ (Atom (FileModifyTimePredName _) _)) env = True
fsSupported _ (FAtomic2 _ (Atom (DirModifyTimePredName _) _)) env = True
fsSupported _ (FAtomic2 _ (Atom (NewFileObjectPredName _) _)) env = True
fsSupported _ (FAtomic2 _ (Atom (NewDirObjectPredName _) _)) env = True
fsSupported _ (FAtomic2 _ (Atom (DirContentPredName _) _)) env = True
fsSupported _ (FAtomic2 _ (Atom (FileContentPredName _) _)) env = True
fsSupported _ (FAtomic2 _ (Atom (DirDirPredName _) _)) env = True
fsSupported _ (FAtomic2 _ (Atom (FileDirPredName _) _)) env = True
fsSupported _ (FAtomic2 _ (Atom (FileContentRangePredName _) _)) env = True
fsSupported _ (FInsert2 _ (Lit Pos (Atom (FileSizePredName _) _))) env = True
fsSupported _ (FInsert2 _ (Lit Pos (Atom (FileNamePredName _) _))) env = True
fsSupported _ (FInsert2 _ (Lit Pos (Atom (DirNamePredName _) _))) env = True
fsSupported _ (FInsert2 _ (Lit Pos (Atom (FileHostPredName _) _))) env = True
fsSupported _ (FInsert2 _ (Lit Pos (Atom (DirHostPredName _) _))) env = True
fsSupported _ (FInsert2 _ (Lit Pos (Atom (FileContentPredName _) _))) env = True
fsSupported _ (FInsert2 _ (Lit Pos (Atom (DirContentPredName _) _))) env = True
fsSupported _ (FInsert2 _ (Lit Pos (Atom (DirDirPredName _) _))) env = True
fsSupported _ (FInsert2 _ (Lit Pos (Atom (FileDirPredName _) _))) env = True
fsSupported _ (FInsert2 _ (Lit Pos (Atom (FileNamePredName _) _))) env = True
fsSupported _ (FInsert2 _ (Lit Pos (Atom (DirNamePredName _) _))) env = True
fsSupported _ (FInsert2 _ (Lit Pos (Atom (FileContentRangePredName _) _))) env = True
fsSupported _ (FInsert2 _ (Lit Neg (Atom (FileContentRangePredName _) _))) env = True
fsSupported _ (FInsert2 _ (Lit Neg (Atom (FileObjectPredName _) _))) env = True
fsSupported _ (FInsert2 _ (Lit Neg (Atom (DirObjectPredName _) _))) env = True
fsSupported _ _ _ = False

data FileObject = NewFileObject String | ExistingFileObject File

data DirObject = NewDirObject String | ExistingDirObject File

data FileContent = FileContent File

data DirContent = DirContent File

instance Convertible FileObject ConcreteResultValue where
  safeConvert (NewFileObject  n) = Right (RefValue "FileObject" ["*"] n)
  safeConvert (ExistingFileObject file) = Right (fileToResultValue "FileObject" file)

instance Convertible ConcreteResultValue FileObject where
  safeConvert (RefValue "FileObject" ["*"] s) =
    Right (NewFileObject  s)
  safeConvert file =
    Right (ExistingFileObject (resultValueToFile file))

instance Convertible DirObject ConcreteResultValue where
  safeConvert (NewDirObject  n) = Right (RefValue "DirObject" ["*"] n)
  safeConvert (ExistingDirObject file) = Right (fileToResultValue "DirObject" file)

instance Convertible ConcreteResultValue DirObject where
  safeConvert (RefValue "DirObject" ["*"] s) =
    Right (NewDirObject  s)
  safeConvert file =
    Right (ExistingDirObject (resultValueToFile file))

instance Convertible FileContent ConcreteResultValue where
  safeConvert (FileContent file) = Right (fileToResultValue "FileContent" file)

instance Convertible ConcreteResultValue FileContent where
  safeConvert file =
    Right (FileContent (resultValueToFile file))

instance Convertible DirContent ConcreteResultValue where
  safeConvert (DirContent file) = Right (fileToResultValue "DirContent" file)

instance Convertible ConcreteResultValue DirContent where
  safeConvert file =
    Right (DirContent (resultValueToFile file))

dirObjectPath :: DirObject -> FSProgram File
dirObjectPath o =
  case o of
    NewDirObject _ ->
      stop
    ExistingDirObject file ->
      return file

fileObjectPath :: FileObject -> FSProgram File
fileObjectPath o =
  case o of
    NewFileObject _ ->
      stop
    ExistingFileObject file ->
      return file

allFiles :: FileSystemTrans -> Expr -> Set Var -> FSProgram File
allFiles (FileSystemTrans host root _) (VarExpr var1) env | not (var1 `Set.member` env) = do
  files <- fsallFiles
  file <- foreach files
  setRef var1 (ExistingFileObject file)
  return file

allFiles _ arg1 env = do
  o <- evalRef arg1
  fileObjectPath o

allFileContents :: FileSystemTrans -> Expr -> Set Var -> FSProgram File
allFileContents (FileSystemTrans host root _) (VarExpr var1) env | not (var1 `Set.member` env) = do
  files <- fsallFiles
  file <- foreach files
  setRef var1 (FileContent file)
  return file

allFileContents _ arg1 env = do
  FileContent file <- evalRef arg1
  return file

allDirs :: FileSystemTrans -> Expr -> Set Var -> FSProgram File
allDirs (FileSystemTrans host root _) (VarExpr var1) env | not (var1 `Set.member` env) = do
  files <- fsallDirs
  file <- foreach files
  setRef var1 (ExistingFileObject file)
  return file

allDirs _ arg1 env = do
  o <- evalRef arg1
  fileObjectPath o


ranges :: File -> Expr -> Expr -> Set Var -> FSProgram (Integer, Integer)
ranges absp (VarExpr var2) (VarExpr var3) env | not (var2 `Set.member` env || var3 `Set.member` env) = do
  s <- fssize absp
  a <- foreach [0..s]
  b <- foreach [a..s]
  setInteger var2 a
  setInteger var3 b
  return (a, b)

ranges absp arg2 (VarExpr var3) env | not (var3 `Set.member` env) = do
  s <- fssize absp
  a <- evalInteger arg2
  unless (a >= 0 && a <= s) stop
  b <- foreach [a..s]
  setInteger var3 b
  return (a, b)

ranges  absp (VarExpr var2) arg3 env | not (var2 `Set.member` env) = do
  s <- fssize absp
  b <- evalInteger arg3
  unless (b >= 0 && b <= s) stop
  a <- foreach [0..b]
  setInteger var2 a
  return (a, b)

ranges  absp arg2 arg3 _ = do
  s <- fssize absp
  a <- evalInteger arg2
  b <- evalInteger arg3
  unless (a >= 0 && b <= s && a <= b) stop
  return (a, b)

rangesl :: File -> Expr -> Expr -> Int -> Set Var -> FSProgram (Integer, Integer)
rangesl absp (VarExpr var2) (VarExpr var3) l env | not (var2 `Set.member` env || var3 `Set.member` env) = do
  s <- fssize absp
  a <- foreach [0..s - fromIntegral l]
  let b = a + fromIntegral l
  setInteger var2 a
  setInteger var3 b
  return (a, b)

rangesl absp arg2 (VarExpr var3) l env | not (var3 `Set.member` env) = do
  s <- fssize absp
  a <- evalInteger arg2
  let b = a + fromIntegral l
  unless (a >= 0 && b <= s) stop
  setInteger var3 b
  return (a, b)

rangesl  absp (VarExpr var2) arg3 l env | not (var2 `Set.member` env) = do
  s <- fssize absp
  b <- evalInteger arg3
  let a = b - fromIntegral l
  unless (a >= 0 && b <= s) stop
  setInteger var2 a
  return (a, b)

rangesl  absp arg2 arg3 l _ = do
  s <- fssize absp
  a <- evalInteger arg2
  b <- evalInteger arg3
  unless (a >= 0 && b <= s && a + fromIntegral l == b) stop
  return (a, b)

extractText :: ConcreteResultValue -> Text
extractText a =  case a of
    StringValue s ->  s
    _ -> error ("FileSystem: not a string")

extractInteger :: ConcreteResultValue -> Integer
extractInteger a = case a of
    Int64Value s -> fromIntegral s
    _ -> error ("FileSystem: not an integer")

extractByteString :: ConcreteResultValue -> BS.ByteString
extractByteString a = case a of
    ByteStringValue s -> s
    _ -> error ("FileSystem: not a byte string")

injectText :: Text -> ConcreteResultValue
injectText a = StringValue a

injectInteger :: Integer -> ConcreteResultValue
injectInteger a = Int64Value (fromIntegral a)

injectByteString :: BS.ByteString -> ConcreteResultValue
injectByteString a = ByteStringValue a

evalText :: Expr -> FSProgram Text
evalText = (extractText <$>) . evalResultValue

evalInteger :: Expr -> FSProgram Integer
evalInteger = (extractInteger <$>) . evalResultValue

evalByteString :: Expr -> FSProgram BS.ByteString
evalByteString = (extractByteString <$>) . evalResultValue

setText :: Var -> Text -> FSProgram ()
setText var txt = setResultValue var (injectText txt)

setInteger :: Var -> Integer -> FSProgram ()
setInteger var int = setResultValue var (injectInteger int)

setByteString :: Var -> BS.ByteString -> FSProgram ()
setByteString var bs = setResultValue var (injectByteString bs)

evalRef :: (Convertible ConcreteResultValue a) => Expr -> FSProgram a
evalRef arg1 =
  convert <$> evalResultValue arg1

setRef :: (Convertible a ConcreteResultValue) => Var -> a -> FSProgram ()
setRef var o =
  setResultValue var (convert o)

fsTranslateQuery :: FileSystemTrans -> Set Var -> FormulaT -> Set Var -> FSProgram ()
fsTranslateQuery  _ ret (FAtomic2 _ (Atom (NewFileObjectPredName _) [arg1, VarExpr var])) env = do
  n <- T.unpack <$> evalText arg1
  setRef var (NewFileObject n)

fsTranslateQuery  _ ret (FAtomic2 _ (Atom (NewDirObjectPredName _) [arg1, VarExpr var])) env = do
  n <- T.unpack <$> evalText arg1
  setRef var (NewDirObject n)

fsTranslateQuery  trans ret (FAtomic2 _ (Atom (FileNamePredName _) [arg1, VarExpr var2])) env | not (var2 `Set.member` env) = do
  file <- allFiles trans arg1 env
  let n = fsfileName file
  setText var2 (T.pack n)

fsTranslateQuery  _ ret (FAtomic2 _ (Atom (FileNamePredName _) [VarExpr var, arg1])) env | not (var `Set.member` env) = do
  n <- T.unpack <$> evalText arg1
  files <- fsfindFilesByName n
  file <- foreach files
  setRef var (ExistingFileObject file)

fsTranslateQuery _ ret (FAtomic2 _ (Atom (FileNamePredName _) [arg1, arg2])) env = do
  o <- evalRef arg1
  n2 <- T.unpack <$> evalText arg2
  file <- fileObjectPath o
  let n1 = fsfileName file
  unless (n1 == n2) stop

fsTranslateQuery  trans ret (FAtomic2 _ (Atom (DirNamePredName _) [arg1, VarExpr var2])) env | not (var2 `Set.member` env) = do
  file <- allDirs trans arg1 env
  let n = fsfileName file
  setText var2 (T.pack n)

fsTranslateQuery  _ ret (FAtomic2 _ (Atom (DirNamePredName _) [VarExpr var, arg1])) env | not (var `Set.member` env) = do
  n <- T.unpack <$> evalText arg1
  files <- fsfindDirsByName n
  file <- foreach files
  setRef var (ExistingFileObject file)

fsTranslateQuery  _ ret (FAtomic2 _ (Atom (DirNamePredName _) [arg1, arg2])) env = do
  o <- evalRef arg1
  n2 <- T.unpack <$> evalText arg2
  file <- dirObjectPath o
  let n1 = fsfileName file
  unless (n1 == n2) stop

fsTranslateQuery  trans ret (FAtomic2 _ (Atom (FileHostPredName _) [arg1, VarExpr var2])) env | not (var2 `Set.member` env) = do
  file <- allFiles trans arg1 env
  setText var2 (T.pack (fHost file))

fsTranslateQuery  (FileSystemTrans host root _) ret (FAtomic2 _ (Atom (FileHostPredName _) [VarExpr var, arg1])) env | not (var `Set.member` env) = do
  host <- T.unpack <$> evalText arg1
  files <- fsfindFilesByHost host
  file <- foreach files
  setRef var (ExistingFileObject file)

fsTranslateQuery _ ret (FAtomic2 _ (Atom (FileHostPredName _) [arg1, arg2])) env = do
  o <- evalRef arg1
  host2 <- T.unpack <$> evalText arg2
  file1 <- fileObjectPath o
  unless (fHost file1 == host2) stop

fsTranslateQuery  trans ret (FAtomic2 _ (Atom (DirHostPredName _) [arg1, VarExpr var2])) env | not (var2 `Set.member` env) = do
  file <- allDirs trans arg1 env
  setText var2 (T.pack (fHost file))

fsTranslateQuery  _ ret (FAtomic2 _ (Atom (DirHostPredName _) [VarExpr var, arg1])) env | not (var `Set.member` env) = do
  host <- T.unpack <$> evalText arg1
  files <- fsfindDirsByHost host
  file <- foreach files
  setRef var (ExistingFileObject file)

fsTranslateQuery  _ ret (FAtomic2 _ (Atom (DirHostPredName _) [arg1, arg2])) env = do
  o <- evalRef arg1
  host2 <- T.unpack <$> evalText arg2
  file <- dirObjectPath o
  unless (fHost file == host2) stop

fsTranslateQuery  trans ret (FAtomic2 _ (Atom (FileSizePredName _) [arg1, VarExpr var2])) env | not (var2 `Set.member` env) = do
  absp <- allFiles trans arg1 env
  s <- fssize absp
  setInteger var2 s

fsTranslateQuery (FileSystemTrans host root _ ) ret (FAtomic2 _ (Atom (FileSizePredName _) [VarExpr var1, arg2])) env | not (var1 `Set.member` env) = do
  a <- evalInteger arg2
  files <- fsfindFilesBySize (fromInteger a)
  file <- foreach files
  setRef var1 (ExistingFileObject file)

fsTranslateQuery _  ret (FAtomic2 _ (Atom (FileSizePredName _) [arg1, arg2])) env = do
  o <- evalRef arg1
  absp <- fileObjectPath o
  s <- fssize absp
  a <- evalInteger arg2
  unless (s == a) stop

fsTranslateQuery  trans ret (FAtomic2 _ (Atom (FileModifyTimePredName _) [arg1, VarExpr var2])) env | not (var2 `Set.member` env) = do
  absp <- allFiles trans arg1 env
  mt <- fsmodificationTime absp
  setInteger var2 (floor (utcTimeToPOSIXSeconds mt))

fsTranslateQuery  (FileSystemTrans host root _ ) ret (FAtomic2 _ (Atom (FileModifyTimePredName _) [VarExpr var, arg1])) env | not (var `Set.member` env) = do
  mt <- evalInteger arg1
  files <- fsfindFilesByModificationTime (fromInteger mt)
  file <- foreach files
  setRef var (ExistingFileObject file)

fsTranslateQuery  _ ret (FAtomic2 _ (Atom (FileModifyTimePredName _) [arg1, arg2])) env = do
  o <- evalRef arg1
  file <- fileObjectPath o
  mt1 <- fsmodificationTime file
  mt2 <- evalInteger arg1
  unless (floor (utcTimeToPOSIXSeconds mt1) == mt2) stop

fsTranslateQuery trans ret (FAtomic2 _ (Atom (DirModifyTimePredName _) [arg1, VarExpr var2])) env | not (var2 `Set.member` env) = do
  absp <- allDirs trans arg1 env
  mt <- fsmodificationTime absp
  setInteger var2 (floor (utcTimeToPOSIXSeconds mt))

fsTranslateQuery  (FileSystemTrans host root _ ) ret (FAtomic2 _ (Atom (DirModifyTimePredName _) [VarExpr var, arg1])) env | not (var `Set.member` env) = do
  mt <- evalInteger arg1
  files <- fsfindDirsByModificationTime (fromInteger mt)
  file <- foreach files
  setRef var (ExistingDirObject file)

fsTranslateQuery _ ret (FAtomic2 _ (Atom (DirModifyTimePredName _) [arg1, arg2])) env = do
  o <- evalRef arg1
  absp <- dirObjectPath o
  mt1 <- fsmodificationTime absp
  mt2 <- evalInteger arg1
  unless (floor (utcTimeToPOSIXSeconds mt1) == mt2) stop

fsTranslateQuery trans ret (FAtomic2 _ (Atom (FilePathPredName _) [arg1, VarExpr var2])) env | not (var2 `Set.member` env) = do
  file <- allFiles trans arg1 env
  let p = fsRelP file
  setText var2 (T.pack p)

fsTranslateQuery  (FileSystemTrans _ _ _ ) ret (FAtomic2 _ (Atom (FilePathPredName _) [VarExpr var, arg1])) env | not (var `Set.member` env) = do
  p <- T.unpack <$> evalText arg1
  files <- fsfindFilesByPath p
  file <- foreach files
  setRef var (ExistingFileObject file)

fsTranslateQuery _ ret (FAtomic2 _ (Atom (FilePathPredName _) [arg1, arg2])) env = do
  o <- evalRef arg1
  p <- T.unpack <$> evalText arg2
  file <- fileObjectPath o
  unless (file == fsreplaceRelP p file) stop

fsTranslateQuery trans ret (FAtomic2 _ (Atom (DirPathPredName _) [arg1, VarExpr var2])) env | not (var2 `Set.member` env) = do
  file <- allDirs trans arg1 env
  let p = fsRelP file
  setText var2 (T.pack p)

fsTranslateQuery  (FileSystemTrans _ _ _ ) ret (FAtomic2 _ (Atom (DirPathPredName _) [VarExpr var, arg1])) env | not (var `Set.member` env) = do
  p <- T.unpack <$> evalText arg1
  files <- fsfindDirsByPath p
  file <- foreach files
  setRef var (ExistingDirObject file)

fsTranslateQuery _ ret (FAtomic2 _ (Atom (DirPathPredName _) [arg1, arg2])) env = do
  o <- evalRef arg1
  p <- T.unpack <$> evalText arg2
  file <- dirObjectPath o
  unless (file == fsreplaceRelP p file) stop

-- file mode
fsTranslateQuery trans ret (FAtomic2 _ (Atom (FileModePredName _) [arg1, VarExpr var2])) env | not (var2 `Set.member` env) = do
  file <- allFiles trans arg1 env
  p <- fsmode file
  setInteger var2 p

fsTranslateQuery  (FileSystemTrans _ _ _ ) ret (FAtomic2 _ (Atom (FileModePredName _) [VarExpr var, arg1])) env | not (var `Set.member` env) = do
  p <- evalInteger arg1
  files <- fsfindFilesByMode p
  file <- foreach files
  setRef var (ExistingFileObject file)

fsTranslateQuery _ ret (FAtomic2 _ (Atom (FileModePredName _) [arg1, arg2])) env = do
  o <- evalRef arg1
  p <- evalInteger arg2
  file <- fileObjectPath o
  i <- fsmode file
  unless (i == p) stop

-- dir mode
fsTranslateQuery trans ret (FAtomic2 _ (Atom (DirModePredName _) [arg1, VarExpr var2])) env | not (var2 `Set.member` env) = do
  file <- allDirs trans arg1 env
  p <- fsid file
  setInteger var2 p

fsTranslateQuery  (FileSystemTrans _ _ _ ) ret (FAtomic2 _ (Atom (DirModePredName _) [VarExpr var, arg1])) env | not (var `Set.member` env) = do
  p <- evalInteger arg1
  files <- fsfindDirsByMode p
  file <- foreach files
  setRef var (ExistingDirObject file)

fsTranslateQuery _ ret (FAtomic2 _ (Atom (DirModePredName _) [arg1, arg2])) env = do
  o <- evalRef arg1
  p <- evalInteger arg2
  file <- dirObjectPath o
  i <- fsmode file
  unless (i == p) stop

-- file id
fsTranslateQuery trans ret (FAtomic2 _ (Atom (FileIdPredName _) [arg1, VarExpr var2])) env | not (var2 `Set.member` env) = do
  file <- allFiles trans arg1 env
  p <- fsid file
  setInteger var2 p

fsTranslateQuery  (FileSystemTrans _ _ _ ) ret (FAtomic2 _ (Atom (FileIdPredName _) [VarExpr var, arg1])) env | not (var `Set.member` env) = do
  p <- evalInteger arg1
  file <- fsfindFileById p
  case file of
    Nothing -> stop
    Just file ->
      setRef var (ExistingFileObject file)

fsTranslateQuery _ ret (FAtomic2 _ (Atom (FileIdPredName _) [arg1, arg2])) env = do
  o <- evalRef arg1
  p <- evalInteger arg2
  file <- fileObjectPath o
  i <- fsid file
  unless (i == p) stop

-- dir id
fsTranslateQuery trans ret (FAtomic2 _ (Atom (DirIdPredName _) [arg1, VarExpr var2])) env | not (var2 `Set.member` env) = do
  file <- allDirs trans arg1 env
  p <- fsid file
  setInteger var2 p

fsTranslateQuery  (FileSystemTrans _ _ _ ) ret (FAtomic2 _ (Atom (DirIdPredName _) [VarExpr var, arg1])) env | not (var `Set.member` env) = do
  p <- evalInteger arg1
  file <- fsfindDirById p
  case file of
    Nothing -> stop
    Just file ->
      setRef var (ExistingDirObject file)

fsTranslateQuery _ ret (FAtomic2 _ (Atom (DirIdPredName _) [arg1, arg2])) env = do
  o <- evalRef arg1
  p <- evalInteger arg2
  file <- dirObjectPath o
  i <- fsid file
  unless (i == p) stop

fsTranslateQuery  trans ret (FAtomic2 _ (Atom (FileContentPredName _) [arg1, VarExpr var2])) env | not (var2 `Set.member` env) = do
  file <- allFiles trans arg1 env
  setRef var2 (FileContent file)

fsTranslateQuery  _ ret (FAtomic2 _ (Atom (FileContentPredName _) [VarExpr var, arg1])) env | not (var `Set.member` env) = do
  FileContent file <- evalRef arg1
  setRef var (ExistingFileObject file)

fsTranslateQuery  _ ret (FAtomic2 _ (Atom (FileContentPredName _) [arg1, arg2])) env = do
  o <- evalRef arg1
  file1 <- fileObjectPath o
  FileContent file2 <- evalRef arg1
  unless (file1 == file2) stop

fsTranslateQuery trans ret (FAtomic2 _ (Atom (DirContentPredName _) [arg1, VarExpr var2])) env | not (var2 `Set.member` env)= do
  file <- allDirs trans arg1 env
  setRef var2 (DirContent file)

fsTranslateQuery  _ ret (FAtomic2 _ (Atom (DirContentPredName _) [VarExpr var, arg1])) env | not (var `Set.member` env) = do
  DirContent file <- evalRef arg1
  setRef var (ExistingDirObject file)

fsTranslateQuery  _ ret (FAtomic2 _ (Atom (DirContentPredName _) [arg1, arg2])) env = do
  o <- evalRef arg1
  file1 <- dirObjectPath o
  FileContent file2 <- evalRef arg1
  unless (file1 == file2) stop

fsTranslateQuery  (FileSystemTrans host root _ ) ret (FAtomic2 _ (Atom (DirDirPredName _) [VarExpr var1, VarExpr var2])) env | not (var1 `Set.member` env || var2 `Set.member` env) = do
  files <- fsallNonRootDirs
  file <- foreach files
  let absp2 = fsDir file
  case absp2 of
    Nothing -> stop
    Just absp2 -> do
      setRef var1 (ExistingDirObject file)
      setRef var2 (ExistingDirObject absp2)

fsTranslateQuery _ ret (FAtomic2 _ (Atom (DirDirPredName _) [arg1, VarExpr var])) env | not (var `Set.member` env) = do
  o <- evalRef arg1
  file <- dirObjectPath o
  let absp2 = fsDir file
  case absp2 of
    Nothing -> stop
    Just absp2 ->
      setRef var (ExistingDirObject absp2)

fsTranslateQuery  _ ret (FAtomic2 _ (Atom (DirDirPredName _) [VarExpr var, arg1])) env | not (var `Set.member` env) = do
  o <- evalRef arg1
  file <- dirObjectPath o
  absp2s <- listDirDir file
  absp2 <- foreach absp2s
  setRef var (ExistingDirObject absp2)

fsTranslateQuery  _ ret (FAtomic2 _ (Atom (DirDirPredName _) [arg1, arg2])) env = do
  o <- evalRef arg2
  file <- dirObjectPath o
  o2 <- evalRef arg1
  file2 <- dirObjectPath o2
  unless (Just file == fsDir file2) stop

fsTranslateQuery _ ret (FAtomic2 _ (Atom (FileDirPredName _) [VarExpr var1, VarExpr var2])) env | not (var1 `Set.member` env || var2 `Set.member` env) = do
  files <- fsallNonRootFiles
  file <- foreach files
  let absp2 = fsDir file
  case absp2 of
    Nothing -> stop
    Just absp2 -> do
      setRef var1 (ExistingFileObject file)
      setRef var2 (ExistingDirObject absp2)

fsTranslateQuery _ ret (FAtomic2 _ (Atom (FileDirPredName _) [arg1, VarExpr var])) env | not (var `Set.member` env) = do
  o <- evalRef arg1
  file <- fileObjectPath o
  let absp2 = fsDir file
  case absp2 of
    Nothing -> stop
    Just absp2 ->
      setRef var (ExistingDirObject absp2)

fsTranslateQuery  _ ret (FAtomic2 _ (Atom (FileDirPredName _) [VarExpr var, arg1])) env | not (var `Set.member` env) = do
  o <- evalRef arg1
  file <- fileObjectPath o
  absp2s <- listDirFile file
  absp2 <- foreach absp2s
  setRef var (ExistingFileObject absp2)

fsTranslateQuery  _ ret (FAtomic2 _ (Atom (FileDirPredName _) [arg1, arg2])) env = do
  o <- evalRef arg2
  file <- fileObjectPath o
  o2 <- evalRef arg1
  file2 <- fileObjectPath o2
  unless (Just file  == fsDir file2) stop

fsTranslateQuery trans ret (FAtomic2 _ (Atom (FileContentRangePredName _) [arg1, arg2, arg3, VarExpr var4])) env | not (var4 `Set.member` env) = do
  absp <- allFileContents trans arg1 env
  (a, b) <- ranges absp arg2 arg3 env
  c <- fsread absp a b
  setByteString var4 c

fsTranslateQuery trans ret (FAtomic2 _ (Atom (FileContentRangePredName _) [arg1, arg2, arg3, arg4])) env = do
  absp <- allFileContents trans arg1 env
  d <- evalByteString arg4
  let l = BS.length d
  (a, b) <- rangesl absp arg2 arg3 l env
  c <- fsread absp a b
  unless (c == d) stop

fsTranslateQuery  _ ret (FInsert2 _ (Lit Pos (Atom (FileContentPredName _) [arg1, arg2]))) env = do
  o <- evalRef arg1
  FileContent absp2 <- evalRef arg2
  case o of
    NewFileObject _ -> error "cannot add content to a new file object"
    ExistingFileObject absp -> fscopyFile absp2 absp

fsTranslateQuery  _ ret (FInsert2 _ (Lit Pos (Atom (DirContentPredName _) [arg1, arg2]))) env = do
  o <- evalRef arg1
  DirContent absp2 <- evalRef arg2
  case o of
    NewDirObject _ -> error "cannot add content to a new dir object"
    ExistingDirObject absp -> fscopyDir absp2 absp

fsTranslateQuery  _ ret (FInsert2 _ (Lit Pos (Atom (FileNamePredName _) [arg1, arg2]))) env = do
  o <- evalRef arg1
  n <- T.unpack <$> evalText arg2
  case o of
    NewFileObject _ -> error "cannot change name of new file object"
    ExistingFileObject file -> do
      let file2 = fsreplaceFileName n file
      unless (file2 == file) $ moveFile file file2

fsTranslateQuery _ ret (FInsert2 _ (Lit Pos (Atom (FileSizePredName _) [arg1, arg2]))) env = do
  o <- evalRef arg1
  s <- evalInteger arg2
  case o of
    NewFileObject _ -> error "cannot change size of new file object"
    ExistingFileObject absp ->
      fstruncate absp s

fsTranslateQuery  _ ret (FInsert2 _ (Lit Pos (Atom (DirNamePredName _) [arg1, arg2]))) env = do
  o <- evalRef arg1
  n <- T.unpack <$> evalText arg2
  case o of
    NewDirObject _ -> error "cannot change name of new dir object"
    ExistingDirObject file -> do
      let file2 = fsreplaceFileName n file
      unless (file2 == file) $ moveDir file file2

fsTranslateQuery  _ ret (FInsert2 _ (Lit Pos (Atom (FileHostPredName _) [arg1, arg2]))) env = do
  o <- evalRef arg1
  n <- T.unpack <$> evalText arg2
  case o of
    NewFileObject _ -> error "cannot change name of new file object"
    ExistingFileObject file -> do
      let file2 = fsreplaceFileName n file
      unless (file2 == file) $ moveFile file file2

fsTranslateQuery  _ ret (FInsert2 _ (Lit Pos (Atom (DirHostPredName _) [arg1, arg2]))) env = do
  o <- evalRef arg1
  n <- T.unpack <$> evalText arg2
  case o of
    NewDirObject _ -> error "cannot change name of new dir object"
    ExistingDirObject file -> do
      let file2 = fsreplaceFileName n file
      unless (file2 == file) $ moveDir file file2

fsTranslateQuery  _ ret (FInsert2 _ (Lit Pos (Atom (DirDirPredName _) [arg1, arg2]))) env = do
  o <- evalRef arg2
  o2 <- evalRef arg1
  case o of
    NewDirObject _ -> error "cannot add content to a new dir object"
    ExistingDirObject file ->
      case o2 of
        NewDirObject n2 ->
          makeDir (fsreplaceFileName n2 file)
        ExistingDirObject file2 -> do
          let n2 = fsfileName file2
          moveDir file2 (fsreplaceFileName n2 file)

fsTranslateQuery  _ ret (FInsert2 _ (Lit Pos (Atom (FileDirPredName _) [arg1, arg2]))) env = do
  o <- evalRef arg2
  o2 <- evalRef arg1
  case o of
    NewDirObject _ -> error "cannot add content to a new dir object"
    ExistingDirObject file ->
      case o2 of
        NewFileObject n2 ->
          makeFile (fsreplaceFileName n2 file)
        ExistingFileObject file2 -> do
          let n2 = fsfileName file2
          moveFile file2 (fsreplaceFileName n2 file)

fsTranslateQuery  _ ret (FInsert2 _ (Lit Pos (Atom (FileContentRangePredName _) [arg1, arg2, arg3, arg4]))) env = do
  FileContent absp <- evalRef arg1
  a <- evalInteger arg2
  b <- evalInteger arg3
  c <- evalByteString arg4
  unless (b - a == fromIntegral (BS.length c)) $ error "content length error"
  fswrite absp a c

fsTranslateQuery  _ ret (FInsert2 _ (Lit Neg (Atom (FileContentRangePredName _) [arg1, arg2, arg3, arg4]))) env = do
  FileContent absp <- evalRef arg1
  a <- evalInteger arg2
  b <- evalInteger arg3
  c <- evalByteString arg4
  l <- fssize absp
  unless (b - a == fromIntegral (BS.length c) && a >= 0 && b == l) $ error "content length error"
  buf <- fsread absp a b
  unless (c == buf) stop
  fstruncate absp a

fsTranslateQuery  _ ret (FInsert2 _ (Lit Neg (Atom (FileObjectPredName _) [arg]))) env = do
  o <- evalRef arg
  case o of
    NewFileObject _ -> error "cannot delete a new file object"
    ExistingFileObject absp ->
      unlinkFile absp

fsTranslateQuery  _ ret (FInsert2 _ (Lit Neg (Atom (DirObjectPredName _) [arg]))) env = do
  o <- evalRef arg
  case o of
    NewDirObject _ -> error "cannot delete a new dir object"
    ExistingDirObject absp ->
      removeDir absp

fsTranslateQuery _ _ query _ = error ("fsTranslateQuery: cannot translate query" ++ show query)

instance IGenericDatabase01 FileSystemTrans where
    type GDBQueryType FileSystemTrans = FSProgram ()
    type GDBFormulaType FileSystemTrans = FormulaT
    gTranslateQuery trans ret query env = return (fsTranslateQuery trans ret query env)
    gSupported (FileSystemTrans _ _ ns) ret form env = fsSupported ns form env
