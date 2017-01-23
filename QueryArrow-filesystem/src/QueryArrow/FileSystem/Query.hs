{-# LANGUAGE MultiParamTypeClasses, TypeFamilies #-}
module QueryArrow.FileSystem.Query where

import QueryArrow.FO.Data
import QueryArrow.DB.GenericDatabase
import QueryArrow.FileSystem.Builtin
import QueryArrow.FileSystem.Commands
import QueryArrow.FO.Utils

import Control.Monad
import Data.Convertible
import Data.List (find)
import Data.Maybe
import Data.Text (Text)
import qualified Data.Text as T
import Data.Set (Set)
import qualified Data.Set as Set
import System.FilePath
import Data.Time.Clock.POSIX

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
fsSupported _ (FAtomic (Atom (DirContentPredName _) [_, VarExpr v])) env | not (v `Set.member` env) = True
fsSupported _ (FAtomic (Atom (FileContentPredName _) [_, VarExpr v])) env | not (v `Set.member` env) = True
fsSupported _ (FAtomic (Atom (DirDirPredName _) [arg1, arg2])) env | freeVars arg1 `Set.isSubsetOf` env || freeVars arg2 `Set.isSubsetOf` env = True
fsSupported _ (FAtomic (Atom (FileDirPredName _) [arg1, arg2])) env | freeVars arg1 `Set.isSubsetOf` env || freeVars arg2 `Set.isSubsetOf` env = True
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
    ExistingDirObject ap ->
      return ap

fileObjectPath :: FileObject -> FSProgram String
fileObjectPath o =
  case o of
    NewFileObject _ ->
      stop
    ExistingFileObject ap ->
      return ap

toAP :: String -> String -> String
toAP root p = case p of
            ['/'] -> root
            '/' : p2 -> root </> p2
            _ -> error ("fsTranslateQuery: malformatted path " ++ p)

fsTranslateQuery :: FileSystemTrans -> Set Var -> Formula -> Set Var -> FSProgram ()
fsTranslateQuery  _ ret (FAtomic (Atom (NewFileObjectPredName _) [arg1, VarExpr var])) env = do
  n <- T.unpack <$> evalText arg1
  setText var (convert (NewFileObject n))

fsTranslateQuery  _ ret (FAtomic (Atom (NewDirObjectPredName _) [arg1, VarExpr var])) env = do
  n <- T.unpack <$> evalText arg1
  setText var (convert (NewDirObject n))

fsTranslateQuery  _ ret (FAtomic (Atom (FileNamePredName _) [arg1, arg2])) env = do
  o <- convert <$> evalText arg1
  ap <- fileObjectPath o
  let fn = takeFileName ap
  case arg2 of
    VarExpr var ->
      setText var (T.pack fn)
    _ -> do
      n2 <- T.unpack <$> evalText arg2
      unless (fn == n2) stop

fsTranslateQuery  _ ret (FAtomic (Atom (DirNamePredName _) [arg1, arg2])) env = do
  o <- convert <$> evalText arg1
  ap <- dirObjectPath o
  let fn = takeFileName ap
  case arg2 of
    VarExpr var ->
      setText var (T.pack fn)
    _ -> do
      n2 <- T.unpack <$> evalText arg2
      unless (fn == n2) stop

fsTranslateQuery  _ ret (FAtomic (Atom (FileSizePredName _) [arg1, VarExpr var])) env = do
  o <- convert <$> evalText arg1
  ap <- fileObjectPath o
  s <- size ap
  setInteger var s

fsTranslateQuery  _ ret (FAtomic (Atom (FileModifyTimePredName _) [arg1, VarExpr var])) env = do
  o <- convert <$> evalText arg1
  ap <- fileObjectPath o
  mt <- modificationTime ap
  setInteger var (floor (utcTimeToPOSIXSeconds mt))

fsTranslateQuery  _ ret (FAtomic (Atom (DirModifyTimePredName _) [arg1, VarExpr var])) env = do
  o <- convert <$> evalText arg1
  ap <- dirObjectPath o
  mt <- modificationTime ap
  setInteger var (floor (utcTimeToPOSIXSeconds mt))

fsTranslateQuery  (FileSystemTrans root _) ret (FAtomic (Atom (FilePathPredName _) [VarExpr var, arg1])) env = do
  p <- T.unpack <$> evalText arg1
  let ap = toAP root p
  b <- fileExists ap
  if b
    then
      setText var (convert (ExistingFileObject ap))
    else
      stop

fsTranslateQuery  (FileSystemTrans root _) ret (FAtomic (Atom (DirPathPredName _) [VarExpr var, arg1])) env = do
  p <- T.unpack <$> evalText arg1
  let ap = toAP root p
  b <- dirExists ap
  if b
    then
      setText var (convert (ExistingDirObject ap))
    else
      stop

fsTranslateQuery  _ ret (FAtomic (Atom (FileContentPredName _) [arg1, VarExpr var])) env = do
  o <- convert <$> evalText arg1
  case o of
    NewFileObject _ ->
      stop
    ExistingFileObject ap ->
      setText var (convert (FileContent ap))

fsTranslateQuery  _ ret (FAtomic (Atom (DirContentPredName _) [arg1, VarExpr var])) env = do
  o <- convert <$> evalText arg1
  case o of
    NewDirObject _ ->
      stop
    ExistingDirObject ap ->
      setText var (convert (DirContent ap))

fsTranslateQuery  _ ret (FAtomic (Atom (DirDirPredName _) [arg1, VarExpr var])) env | not (var `Set.member` env) = do
  o <- convert <$> evalText arg1
  ap <- dirObjectPath o
  let ap2 = takeDirectory ap
  if ap2 == ap
    then
      stop
    else
        setText var (convert (ExistingDirObject ap2))

fsTranslateQuery  _ ret (FAtomic (Atom (DirDirPredName _) [VarExpr var, arg1])) env = do
  o <- convert <$> evalText arg1
  case o of
    NewDirObject _ ->
      stop
    ExistingDirObject ap -> do
      ap2 <- listDirDir ap
      setText var (convert (ExistingDirObject ap2))

fsTranslateQuery  _ ret (FAtomic (Atom (DirDirPredName _) [arg1, arg2])) env = do
  o <- convert <$>  evalText arg2
  case o of
    NewDirObject _ ->
      stop
    ExistingDirObject ap -> do
      o2 <- convert <$> evalText arg1
      case o2 of
        NewDirObject _ ->
          stop
        ExistingDirObject ap2 -> do
          let seg = splitDirectories ap
          let seg2 = splitDirectories ap2
          unless (length seg2 == length seg + 1 &&
                  take (length seg) seg2 == seg) stop

fsTranslateQuery  _ ret (FAtomic (Atom (FileDirPredName _) [arg1, VarExpr var])) env | not (var `Set.member` env) = do
  o <- convert <$> evalText arg1
  case o of
    NewDirObject _ ->
      stop
    ExistingDirObject ap -> do
      let ap2 = takeDirectory ap
      if ap2 == ap
        then
          stop
        else
          setText var (convert (ExistingDirObject ap2))

fsTranslateQuery  _ ret (FAtomic (Atom (FileDirPredName _) [VarExpr var, arg1])) env = do
  o <- convert <$> evalText arg1
  case o of
    NewDirObject _ ->
      stop
    ExistingDirObject ap -> do
      ap2 <- listDirFile ap
      setText var (convert (ExistingDirObject ap2))

fsTranslateQuery  _ ret (FAtomic (Atom (FileDirPredName _) [arg1, arg2])) env = do
  o <- convert <$>  evalText arg2
  case o of
    NewDirObject _ ->
      stop
    ExistingDirObject ap -> do
      o2 <- convert <$> evalText arg1
      case o2 of
        NewDirObject _ ->
          stop
        ExistingDirObject ap2 -> do
          let seg = splitDirectories ap
          let seg2 = splitDirectories ap2
          unless (length seg2 == length seg + 1 &&
                  take (length seg) seg2 == seg) stop

fsTranslateQuery  _ ret (FAtomic (Atom (FileContentRangePredName _) [arg1, arg2, arg3, VarExpr var])) env | not (var `Set.member` env) = do
  FileContent ap <- convert <$> evalText arg1
  a <- evalInteger arg2
  b <- evalInteger arg3
  c <- fsread ap a b
  setByteString var c

fsTranslateQuery  _ ret (FAtomic (Atom (FileContentRangePredName _) [arg1, arg2, arg3, arg4])) env = do
  FileContent ap <- convert <$> evalText arg1
  a <- evalInteger arg2
  b <- evalInteger arg3
  d <- evalByteString arg4
  c <- fsread ap a b
  unless (c == d) stop

fsTranslateQuery  _ ret (FInsert (Lit Pos (Atom (FileContentPredName _) [arg1, arg2]))) env = do
  o <- convert <$> evalText arg1
  FileContent ap2 <- convert <$> evalText arg2
  case o of
    NewFileObject _ -> error "cannot add content to a new file object"
    ExistingFileObject ap -> fscopyFile ap2 ap

fsTranslateQuery  _ ret (FInsert (Lit Pos (Atom (DirContentPredName _) [arg1, arg2]))) env = do
  o <- convert <$> evalText arg1
  DirContent ap2 <- convert <$> evalText arg2
  case o of
    NewDirObject _ -> error "cannot add content to a new dir object"
    ExistingDirObject ap -> fscopyDir ap2 ap

fsTranslateQuery  _ ret (FInsert (Lit Pos (Atom (FileNamePredName _) [arg1, arg2]))) env = do
  o <- convert <$> evalText arg1
  n <- T.unpack <$> evalText arg2
  case o of
    NewFileObject _ -> error "cannot change name of new file object"
    ExistingFileObject ap -> do
      let ap2 = takeDirectory ap </> n
      unless (equalFilePath ap2 ap) $ moveFile ap ap2

fsTranslateQuery  _ ret (FInsert (Lit Pos (Atom (DirNamePredName _) [arg1, arg2]))) env = do
  o <- convert <$> evalText arg1
  n <- T.unpack <$> evalText arg2
  case o of
    NewFileObject _ -> error "cannot change name of new dir object"
    ExistingFileObject ap -> do
      let ap2 = takeDirectory ap </> n
      unless (equalFilePath ap2 ap) $ moveDir ap ap2

fsTranslateQuery  _ ret (FInsert (Lit Pos (Atom (DirDirPredName _) [arg1, arg2]))) env = do
  o <- convert <$> evalText arg2
  o2 <- convert <$> evalText arg1
  case o of
    NewDirObject _ -> error "cannot add content to a new dir object"
    ExistingDirObject ap ->
      case o2 of
        NewDirObject n2 ->
          makeDir (ap </> n2)
        ExistingDirObject ap2 -> do
          let n2 = takeFileName ap2
          moveDir ap2 (ap </> n2)

fsTranslateQuery  _ ret (FInsert (Lit Pos (Atom (FileDirPredName _) [arg1, arg2]))) env = do
  o <- convert <$> evalText arg2
  o2 <- convert <$> evalText arg1
  case o of
    NewDirObject _ -> error "cannot add content to a new dir object"
    ExistingDirObject ap ->
      case o2 of
        NewFileObject n2 ->
          makeFile (ap </> n2)
        ExistingFileObject ap2 -> do
          let n2 = takeFileName ap2
          moveFile ap2 (ap </> n2)

fsTranslateQuery  _ ret (FInsert (Lit Pos (Atom (FileContentRangePredName _) [arg1, arg2, arg3, arg4]))) env = do
  o <- convert <$> evalText arg1
  a <- evalInteger arg2
  b <- evalInteger arg3
  c <- evalText arg4
  case o of
    NewFileObject _ -> error "cannot modify content of a new file object"
    ExistingFileObject ap -> do
      unless (b - a == fromIntegral (T.length c)) $ error "content length error"
      fswrite ap a c


fsTranslateQuery  _ ret (FInsert (Lit Neg (Atom (FileObjectPredName _) [arg]))) env = do
  o <- convert <$> evalText arg
  case o of
    NewFileObject _ -> error "cannot delete a new file object"
    ExistingFileObject ap ->
      unlinkFile ap

fsTranslateQuery  _ ret (FInsert (Lit Neg (Atom (DirObjectPredName _) [arg]))) env = do
  o <- convert <$> evalText arg
  case o of
    NewDirObject _ -> error "cannot delete a new dir object"
    ExistingDirObject ap ->
      removeDir ap

fsTranslateQuery _ _ query _ = error ("fsTranslateQuery: cannot translate query" ++ show query)

instance IGenericDatabase01 FileSystemTrans where
    type GDBQueryType FileSystemTrans = FSProgram ()
    type GDBFormulaType FileSystemTrans = Formula
    gTranslateQuery trans ret query env = return (fsTranslateQuery trans ret query env)
    gCheckQuery _ _ _ _ = return (Right ())
    gSupported (FileSystemTrans _ ns) ret form env = fsSupported ns form env
