{-# LANGUAGE TypeSynonymInstances, FlexibleInstances, MultiParamTypeClasses, FlexibleContexts, RankNTypes, GADTs, PatternSynonyms, TypeFamilies, DeriveFunctor, OverloadedStrings #-}
module QueryArrow.FileSystem.Query where

import QueryArrow.FO.Data hiding (Subst, subst)
import QueryArrow.DB.GenericDatabase
import QueryArrow.ListUtils
import QueryArrow.Utils
import QueryArrow.DB.DB
import QueryArrow.FO.Domain

import Prelude hiding (lookup)
import Control.Monad.Free
import Control.Monad
import Data.Convertible
import Data.List (intercalate, (\\),union, nub, find)
import Control.Monad.Trans.State.Strict (StateT, get, put, evalStateT, runStateT, modify)
import Control.Monad.Trans.Class (lift)
import Data.Map.Strict (empty, Map, insert, member, singleton, lookup, fromList, keys, alter, toList, elems, size, delete)
import Data.Monoid ((<>))
import Control.Applicative
import Data.Maybe
import Debug.Trace
import Data.Text (Text)
import qualified Data.Text as T
import Data.Set (toAscList, Set)
import qualified Data.Set as Set
import System.FilePath
import Algebra.Lattice
import Algebra.Lattice.Dropped
import Algebra.Lattice.Ordered

pattern PTKeyIO a = ParamType True True True a
pattern PTKeyI a = ParamType True False True a
pattern PTKeyO a = ParamType True True False a
pattern PTPropIO a = ParamType False True True a
pattern PTPropI a = ParamType False True False a
pattern PTPropO a = ParamType False False True a

data FileSystemConnInfo = FileSystemConnInfo

pattern FilePathPredName ns = QPredName ns [] "FILE_PATH"

pattern DirPathPredName ns = QPredName ns [] "DIR_PATH"

pattern FileNamePredName ns = QPredName ns [] "FILE_NAME"

pattern DirNamePredName ns = QPredName ns [] "DIR_NAME"

pattern FileObjectPredName ns = QPredName ns [] "FILE_OBJ"

pattern DirObjectPredName ns = QPredName ns [] "DIR_OBJ"

pattern FileContentPredName ns = QPredName ns [] "FILE_CONTENT"

pattern DirContentPredName ns = QPredName ns [] "DIR_CONTENT"

pattern FileDirPredName ns = QPredName ns [] "DIR_DIR"

pattern DirDirPredName ns = QPredName ns [] "FILE_DIR"

pattern NewFileObjectPredName ns = QPredName ns [] "NEW_FILE_OBJ"

pattern NewDirObjectPredName ns = QPredName ns [] "NEW_DIR_OBJ"

pattern FileContentRangePredName ns = QPredName ns [] "FILE_CONTENT_RANGE"

pattern FilePathPred ns = Pred (FilePathPredName ns) (PredType PropertyPred [PTKeyO (RefType "FileObject"), PTPropI TextType])

pattern DirPathPred ns = Pred (DirPathPredName ns) (PredType PropertyPred [PTKeyO (RefType "DirObject"), PTPropI TextType])

pattern FileNamePred ns = Pred (FileNamePredName ns) (PredType PropertyPred [PTKeyI (RefType "FileObject"), PTPropIO TextType])

pattern DirNamePred ns = Pred (DirNamePredName ns) (PredType PropertyPred [PTKeyI (RefType "DirObject"), PTPropIO TextType])

pattern FileObjectPred ns = Pred (FileObjectPredName ns) (PredType ObjectPred [PTKeyI (RefType "FileObject")])

pattern DirObjectPred ns = Pred (DirObjectPredName ns) (PredType ObjectPred [PTKeyI (RefType "DirObject")])

pattern NewFileObjectPred ns = Pred (NewFileObjectPredName ns) (PredType PropertyPred [PTKeyI TextType, PTPropO (RefType "DirObject")])

pattern NewDirObjectPred ns = Pred (NewDirObjectPredName ns) (PredType PropertyPred [PTKeyI TextType, PTPropO (RefType "FileObject")])

pattern FileContentPred ns = Pred (FileContentPredName ns) (PredType PropertyPred [PTKeyI (RefType "FileObject"), PTPropIO (RefType "FileContnet")])

pattern DirContentPred ns = Pred (DirContentPredName ns) (PredType PropertyPred [PTKeyI (RefType "DirObject"), PTKeyIO (RefType "DirContent")])

pattern DirDirPred ns = Pred (DirDirPredName ns) (PredType PropertyPred [PTKeyIO (RefType "DirObject"), PTPropIO (RefType "DirObject")])

pattern FileDirPred ns = Pred (FileDirPredName ns) (PredType PropertyPred [PTPropIO (RefType "FileObject"), PTPropIO (RefType "DirObject")])

pattern FileContentRangePred ns = Pred (FileContentPredName ns) (PredType PropertyPred [PTKeyI (RefType "FileContent"), PTPropI NumberType, PTPropI NumberType, PTPropIO TextType])

data Stats = Stats {isDir :: Bool}


data FSCommand x = DirExists String (Bool -> x)
                 | FileExists String (Bool -> x)
                 | CopyFile String String x
                 | CopyDir String String x
                 | UnlinkFile String x
                 | RemoveDir String x
                 | MakeFile String x
                 | MakeDir String x
                 | Write String Integer Text x
                 | Read String Integer Integer (Text -> x)
                 | ListDirDir String (String -> x)
                 | ListDirFile String (String -> x)
                 | Stat String (Maybe Stats -> x)
                 | MoveFile String String x
                 | MoveDir String String x
                 | EvalText Expr (Text -> x)
                 | EvalInteger Expr (Integer -> x)
                 | SetText Var Text x
                 | SetInteger Var Integer x
                 | Stop deriving Functor


type FSProgram = Free FSCommand

dirExists :: String -> FSProgram Bool
dirExists a = liftF (DirExists a id)

fileExists :: String -> FSProgram Bool
fileExists a = liftF (FileExists a id)

fscopyFile :: String -> String -> FSProgram ()
fscopyFile a b = liftF (CopyFile a b ())

fscopyDir :: String -> String -> FSProgram ()
fscopyDir a b = liftF (CopyDir a b ())

unlinkFile :: String -> FSProgram ()
unlinkFile a = liftF (UnlinkFile a ())

removeDir :: String -> FSProgram ()
removeDir a = liftF (RemoveDir a ())

makeFile :: String -> FSProgram ()
makeFile a = liftF (MakeFile a ())

makeDir :: String -> FSProgram ()
makeDir a = liftF (MakeDir a ())

fswrite :: String -> Integer -> Text -> FSProgram ()
fswrite a b d = liftF (Write a b d ())

fsread :: String -> Integer -> Integer -> FSProgram Text
fsread a b c = liftF (Read a b c id)

listDirDir :: String -> FSProgram String
listDirDir a = liftF (ListDirDir a id)

listDirFile :: String -> FSProgram String
listDirFile a = liftF (ListDirFile a id)

stat :: String -> FSProgram (Maybe Stats)
stat a = liftF (Stat a id)

moveFile :: String -> String -> FSProgram ()
moveFile a b = liftF (MoveFile a b ())

moveDir :: String -> String -> FSProgram ()
moveDir a b = liftF (MoveDir a b ())

stop :: FSProgram ()
stop = liftF Stop

setText :: Var -> Text -> FSProgram ()
setText var expr = liftF (SetText var expr ())

setInteger :: Var -> Integer -> FSProgram ()
setInteger var expr = liftF (SetInteger var expr ())

evalText :: Expr -> FSProgram Text
evalText expr = liftF (EvalText expr id)

evalInteger :: Expr -> FSProgram Integer
evalInteger expr = liftF (EvalInteger expr id)

data FileSystemTrans = FileSystemTrans {rootDir :: String, predNS :: String}

fsSupported :: String -> Formula -> Set Var -> Bool
fsSupported _ (FAtomic (Atom (FilePathPredName _) _)) env = True
fsSupported _ (FAtomic (Atom (DirPathPredName _) _)) env = True
fsSupported _ (FAtomic (Atom (FileNamePredName _) _)) env = True
fsSupported _ (FAtomic (Atom (DirNamePredName _) _)) env = True
fsSupported _ (FAtomic (Atom (NewFileObjectPredName _) _)) env = True
fsSupported _ (FAtomic (Atom (NewDirObjectPredName _) _)) env = True
fsSupported _ (FAtomic (Atom (DirContentPredName _) _)) env = True
fsSupported _ (FAtomic (Atom (FileContentPredName _) [_, VarExpr v])) env | not (v `Set.member` env) = True
fsSupported _ (FAtomic (Atom (DirDirPredName _) [arg1, arg2])) env | freeVars arg1 `Set.isSubsetOf` env || freeVars arg2 `Set.isSubsetOf` env = True
fsSupported _ (FAtomic (Atom (FileDirPredName _) [arg1, arg2])) env | freeVars arg1 `Set.isSubsetOf` env || freeVars arg2 `Set.isSubsetOf` env = True
fsSupported _ (FAtomic (Atom (FileContentRangePredName _) _)) env = True
fsSupported _ (FInsert (Lit Pos (Atom (FileNamePredName _) _))) env = True
fsSupported _ (FInsert (Lit Pos (Atom (DirNamePredName _) _))) env = True
fsSupported _ (FInsert (Lit Pos (Atom (FileContentPredName _) _))) env = True
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

fsTranslateQuery :: FileSystemTrans -> Set Var -> Formula -> Set Var -> FSProgram ()
fsTranslateQuery trans@(FileSystemTrans root ns) ret (FAtomic (Atom (NewFileObjectPredName _) [arg1, VarExpr var])) env = do
  n <- T.unpack <$> evalText arg1
  setText var (convert (NewFileObject n))

fsTranslateQuery trans@(FileSystemTrans root ns) ret (FAtomic (Atom (NewDirObjectPredName _) [arg1, VarExpr var])) env = do
  n <- T.unpack <$> evalText arg1
  setText var (convert (NewDirObject n))

fsTranslateQuery trans@(FileSystemTrans root ns) ret (FAtomic (Atom (FilePathPredName _) [VarExpr var, arg1])) env = do
  p <- T.unpack <$> evalText arg1
  let ap = root </> p
  b <- fileExists ap
  if b
    then
      setText var (convert (ExistingFileObject ap))
    else
      stop

fsTranslateQuery trans@(FileSystemTrans root ns) ret (FAtomic (Atom (DirPathPredName _) [VarExpr var, arg1])) env = do
  p <- T.unpack <$> evalText arg1
  let ap = root </> p
  b <- dirExists ap
  if b
    then
      setText var (convert (ExistingDirObject ap))
    else
      stop

fsTranslateQuery trans@(FileSystemTrans root ns) ret (FAtomic (Atom (FileContentPredName _) [arg1, VarExpr var])) env = do
  o <- convert <$> evalText arg1
  case o of
    NewFileObject _ ->
      stop
    ExistingFileObject ap ->
      setText var (convert (FileContent ap))

fsTranslateQuery trans@(FileSystemTrans root ns) ret (FAtomic (Atom (DirContentPredName _) [arg1, VarExpr var])) env = do
  o <- convert <$> evalText arg1
  case o of
    NewDirObject _ ->
      stop
    ExistingDirObject ap ->
      setText var (convert (DirContent ap))

fsTranslateQuery trans@(FileSystemTrans root ns) ret (FAtomic (Atom (DirDirPredName _) [arg1, VarExpr var])) env | not (var `Set.member` env) = do
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

fsTranslateQuery trans@(FileSystemTrans root ns) ret (FAtomic (Atom (DirDirPredName _) [VarExpr var, arg1])) env = do
  o <- convert <$> evalText arg1
  case o of
    NewDirObject _ ->
      stop
    ExistingDirObject ap -> do
      ap2 <- listDirDir ap
      setText var (convert (ExistingDirObject ap2))

fsTranslateQuery trans@(FileSystemTrans root ns) ret (FAtomic (Atom (DirDirPredName _) [arg1, arg2])) env = do
  o <- convert <$>  evalText arg2
  case o of
    NewDirObject _ ->
      stop
    ExistingDirObject ap -> do
      o2 <- convert <$> evalText arg1
      case o2 of
        NewDirObject _ ->
          stop
        ExistingDirObject ap2 ->
          unless (length ap2 > length ap &&
                  take (length ap) ap2 == ap &&
                  ap2 !! length ap == pathSeparator &&
                  isNothing (find (== pathSeparator) (drop (length ap + 1) ap2))) stop

fsTranslateQuery trans@(FileSystemTrans root ns) ret (FAtomic (Atom (FileDirPredName _) [arg1, VarExpr var])) env | not (var `Set.member` env) = do
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

fsTranslateQuery trans@(FileSystemTrans root ns) ret (FAtomic (Atom (FileDirPredName _) [VarExpr var, arg1])) env = do
  o <- convert <$> evalText arg1
  case o of
    NewDirObject _ ->
      stop
    ExistingDirObject ap -> do
      ap2 <- listDirFile ap
      setText var (convert (ExistingDirObject ap2))

fsTranslateQuery trans@(FileSystemTrans root ns) ret (FAtomic (Atom (FileDirPredName _) [arg1, arg2])) env = do
  o <- convert <$>  evalText arg2
  case o of
    NewDirObject _ ->
      stop
    ExistingDirObject ap -> do
      o2 <- convert <$> evalText arg1
      case o2 of
        NewDirObject _ ->
          stop
        ExistingDirObject ap2 ->
          unless (length ap2 > length ap &&
                  take (length ap) ap2 == ap &&
                  ap2 !! length ap == pathSeparator &&
                  isNothing (find (== pathSeparator) (drop (length ap + 1) ap2))) stop

fsTranslateQuery trans@(FileSystemTrans root ns) ret (FAtomic (Atom (FileContentRangePredName _) [arg1, arg2, arg3, VarExpr var])) env | not (var `Set.member` env) = do
  FileContent ap <- convert <$> evalText arg1
  a <- evalInteger arg2
  b <- evalInteger arg3
  c <- fsread ap a b
  setText var c

fsTranslateQuery trans@(FileSystemTrans root ns) ret (FAtomic (Atom (FileContentRangePredName _) [arg1, arg2, arg3, arg4])) env = do
  FileContent ap <- convert <$> evalText arg1
  a <- evalInteger arg2
  b <- evalInteger arg3
  d <- evalText arg4
  c <- fsread ap a b
  unless (c == d) stop

fsTranslateQuery trans@(FileSystemTrans root ns) ret (FInsert (Lit Pos (Atom (FileContentPredName _) [arg1, arg2]))) env = do
  o <- convert <$> evalText arg1
  FileContent ap2 <- convert <$> evalText arg2
  case o of
    NewFileObject _ -> error "cannot add content to a new file object"
    ExistingFileObject ap -> fscopyFile ap ap2

fsTranslateQuery trans@(FileSystemTrans root ns) ret (FInsert (Lit Pos (Atom (DirContentPredName _) [arg1, arg2]))) env = do
  o <- convert <$> evalText arg1
  DirContent ap2 <- convert <$> evalText arg2
  case o of
    NewDirObject _ -> error "cannot add content to a new dir object"
    ExistingDirObject ap -> fscopyDir ap ap2

fsTranslateQuery trans@(FileSystemTrans root ns) ret (FInsert (Lit Pos (Atom (FileNamePredName _) [arg1, arg2]))) env = do
  o <- convert <$> evalText arg1
  n <- T.unpack <$> evalText arg2
  case o of
    NewFileObject _ -> error "cannot change name of new file object"
    ExistingFileObject ap -> do
      let ap2 = takeDirectory ap </> n
      unless (ap2 == ap) $ moveFile ap ap2

fsTranslateQuery trans@(FileSystemTrans root ns) ret (FInsert (Lit Pos (Atom (DirNamePredName _) [arg1, arg2]))) env = do
  o <- convert <$> evalText arg1
  n <- T.unpack <$> evalText arg2
  case o of
    NewFileObject _ -> error "cannot change name of new dir object"
    ExistingFileObject ap -> do
      let ap2 = takeDirectory ap </> n
      unless (ap2 == ap) $ moveDir ap ap2

fsTranslateQuery trans@(FileSystemTrans root ns) ret (FInsert (Lit Pos (Atom (DirDirPredName _) [arg1, arg2]))) env = do
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

fsTranslateQuery trans@(FileSystemTrans root ns) ret (FInsert (Lit Pos (Atom (FileDirPredName _) [arg1, arg2]))) env = do
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

fsTranslateQuery trans@(FileSystemTrans root ns) ret (FInsert (Lit Pos (Atom (FileContentRangePredName _) [arg1, arg2, arg3, arg4]))) env = do
  o <- convert <$> evalText arg1
  a <- evalInteger arg2
  b <- evalInteger arg3
  c <- evalText arg4
  case o of
    NewFileObject _ -> error "cannot modify content of a new file object"
    ExistingFileObject ap -> do
      unless (b - a == fromIntegral (T.length c)) $ error "content length error"
      fswrite ap a c


fsTranslateQuery trans@(FileSystemTrans root ns) ret (FInsert (Lit Neg (Atom (FileObjectPredName _) [arg]))) env = do
  o <- convert <$> evalText arg
  case o of
    NewFileObject _ -> error "cannot delete a new file object"
    ExistingFileObject ap ->
      unlinkFile ap

fsTranslateQuery trans@(FileSystemTrans root ns) ret (FInsert (Lit Neg (Atom (DirObjectPredName _) [arg]))) env = do
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
    gSupported trans@(FileSystemTrans _ ns) ret form env = fsSupported ns form env
