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

pattern FilePredName ns = QPredName ns [] "FILE_OBJ"

pattern DirPredName ns = QPredName ns [] "DIR_OBJ"

pattern FileContentPredName ns = QPredName ns [] "FILE_CONTENT"

pattern DirContentPredName ns = QPredName ns [] "DIR_CONTENT"

pattern FileContentRangePredName ns = QPredName ns [] "FILE_CONTENT_RANGE"

pattern FilePred ns = Pred (FilePredName ns) (PredType ObjectPred [PTKeyI TextType])

pattern DirPred ns = Pred (DirPredName ns) (PredType ObjectPred [PTKeyI TextType])

pattern FileContentPred ns = Pred (FileContentPredName ns) (PredType ObjectPred [PTKeyI TextType, PTPropO RefType])

pattern DirContentPred ns = Pred (DirContentPredName ns) (PredType ObjectPred [PTKeyI TextType, PTPropIO TextType])

pattern FileContentRangePred ns = Pred (FileContentPredName ns) (PredType ObjectPred [PTKeyIO TextType, PTPropI NumberType, PTPropI NumberType, PTPropO TextType])

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
               | ListDir String (String -> x)
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

listDir :: String -> FSProgram String
listDir a = liftF (ListDir a id)

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

fsSupported :: String -> Formula -> Bool
fsSupported _ (FAtomic (Atom (FilePredName _) _)) = True
fsSupported _ (FAtomic (Atom (DirPredName _) _)) = True
fsSupported _ (FAtomic (Atom (FileContentPredName _) _)) = True
fsSupported _ (FAtomic (Atom (DirContentPredName _) _)) = True
fsSupported _ (FAtomic (Atom (FileContentRangePredName _) _)) = True
fsSupported _ (FInsert (Lit Pos (Atom (FilePredName _) _))) = True
fsSupported _ (FInsert (Lit Pos (Atom (DirPredName _) _))) = True
fsSupported _ (FInsert (Lit Pos (Atom (FileContentPredName _) _))) = True
fsSupported _ (FInsert (Lit Pos (Atom (DirContentPredName _) _))) = True
fsSupported _ (FInsert (Lit Pos (Atom (FileContentRangePredName _) _))) = True
fsSupported _ (FInsert (Lit Neg (Atom (FilePredName _) _))) = True
fsSupported _ (FInsert (Lit Neg (Atom (DirPredName _) _))) = True
fsSupported _ (FInsert (Lit Neg (Atom (DirContentPredName _) _))) = True
fsSupported _ _ = False

fsTranslateQuery :: FileSystemTrans -> Set Var -> Formula -> Set Var -> FSProgram ()
fsTranslateQuery trans@(FileSystemTrans root ns) ret (FAtomic (Atom (FilePredName _) [arg])) env = do
  p <- T.unpack <$> evalText arg
  b <- fileExists (root </>  p)
  unless b stop

fsTranslateQuery trans@(FileSystemTrans root ns) ret (FAtomic (Atom (DirPredName _) [arg])) env = do
  p <- T.unpack <$> evalText arg
  b <- dirExists (root </>  p)
  unless b stop

fsTranslateQuery trans@(FileSystemTrans root ns) ret (FAtomic (Atom (FileContentPredName _) [arg1, VarExpr var])) env = do
  p <- evalText arg1
  setText var p

fsTranslateQuery trans@(FileSystemTrans root ns) ret (FAtomic (Atom (DirContentPredName _) [arg1, VarExpr var])) env = do
  p <- T.unpack <$> evalText arg1
  p2 <- listDir (root </>  p)
  setText var (T.pack (drop (length root + 1) p2))

fsTranslateQuery trans@(FileSystemTrans root ns) ret (FAtomic (Atom (DirContentPredName _) [arg1, arg2])) env = do
  p <- T.unpack <$>  evalText arg1
  p2 <- T.unpack <$>  evalText arg2
  unless (length p2 > length p &&
          take (length p) p2 == p &&
          p2 !! length p == pathSeparator &&
          isNothing (find (== pathSeparator) (drop (length p + 1) p2))) stop

fsTranslateQuery trans@(FileSystemTrans root ns) ret (FAtomic (Atom (FileContentRangePredName _) [arg1, arg2, arg3, VarExpr var])) env = do
  p <- T.unpack <$> evalText arg1
  a <- evalInteger arg2
  b <- evalInteger arg3
  c <- fsread p a b
  setText var c

fsTranslateQuery trans@(FileSystemTrans root ns) ret (FInsert (Lit Pos (Atom (FilePredName _) [arg]))) env = do
  p <- T.unpack <$> evalText arg
  makeFile (root </> p)

fsTranslateQuery trans@(FileSystemTrans root ns) ret (FInsert (Lit Pos (Atom (DirPredName _) [arg]))) env = do
  p <- T.unpack <$> evalText arg
  makeDir (root </> p)

fsTranslateQuery trans@(FileSystemTrans root ns) ret (FInsert (Lit Pos (Atom (FileContentPredName _) [arg1, arg2]))) env = do
  p <- T.unpack <$> evalText arg1
  p2 <- T.unpack <$> evalText arg2
  fscopyFile (root </> p) (root </> p2)

fsTranslateQuery trans@(FileSystemTrans root ns) ret (FInsert (Lit Pos (Atom (DirContentPredName _) [arg1, arg2]))) env = do
  p <- T.unpack <$> evalText arg1
  p2 <- T.unpack <$> evalText arg2
  let n = takeFileName p2
  let ap2 = root </> p2
  stats <- stat ap2
  case stats of
    Nothing -> error ("stat error: does not exist " ++ p2)
    Just stats ->
      if isDir stats
        then
          fscopyFile ap2 (root </> p)
        else
          fscopyDir ap2 (root </> p)
fsTranslateQuery trans@(FileSystemTrans root ns) ret (FInsert (Lit Pos (Atom (FileContentRangePredName _) [arg1, arg2, arg3, arg4]))) env = do
  p <- T.unpack <$> evalText arg1
  a <- evalInteger arg2
  b <- evalInteger arg3
  c <- evalText arg4
  unless (b - a == fromIntegral (T.length c)) $ error "content length error"
  fswrite (root </> p) a c


fsTranslateQuery trans@(FileSystemTrans root ns) ret (FInsert (Lit Neg (Atom (FilePredName _) [arg]))) env = do
  p <- T.unpack <$> evalText arg
  unlinkFile (root </> p)

fsTranslateQuery trans@(FileSystemTrans root ns) ret (FInsert (Lit Neg (Atom (DirPredName _) [arg]))) env = do
  p <- T.unpack <$> evalText arg
  removeDir (root </> p)

fsTranslateQuery trans@(FileSystemTrans root ns) ret (FInsert (Lit Neg (Atom (DirContentPredName _) [arg1, arg2]))) env = do
  p <- T.unpack <$> evalText arg1
  p2 <- T.unpack <$> evalText arg2
  let n = takeFileName p2
  let ap2 = root </> p2
  stats <- stat ap2
  case stats of
    Nothing -> return ()
    Just stats ->
      if isDir stats
        then
          unlinkFile ap2
        else
          removeDir ap2


fsTranslateQuery _ _ query _ = error ("fsTranslateQuery: cannot translate query" ++ show query)


instance IGenericDatabase01 FileSystemTrans where
    type GDBQueryType FileSystemTrans = FSProgram ()
    type GDBFormulaType FileSystemTrans = Formula
    gTranslateQuery trans ret query env = return (fsTranslateQuery trans ret query env)
    gCheckQuery _ _ _ _ = return (Right ())
    gSupported trans@(FileSystemTrans _ ns) ret form env = fsSupported ns form
