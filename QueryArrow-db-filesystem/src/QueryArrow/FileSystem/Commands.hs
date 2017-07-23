{-# LANGUAGE DeriveFunctor, RankNTypes, GADTs, StandaloneDeriving, MultiParamTypeClasses #-}
module QueryArrow.FileSystem.Commands (
    File(fHost), fsreplaceFileName, fsfileName, fsRelP, fsDir, fsreplaceRelP,
    fileToResultValue, resultValueToFile,
    Stats(..), FSProgram,
    fsid, fsmode, stat, fswrite, fsread, fileExists, dirExists, fsfindFilesByPath, fsfindDirsByPath,
    fstruncate, unlinkFile, removeDir, makeFile, makeDir, moveFile, moveDir, fsmodificationTime, fssize, evalResultValue, setResultValue, foreach,
    fscopyFile, fscopyDir, listDirDir, listDirFile, fsallFiles, fsallDirs, fsallNonRootFiles, fsallNonRootDirs, fsfindFilesByName, fsfindDirsByName, fsfindFileById, fsfindDirById, fsfindFilesByMode, fsfindDirsByMode,
    fsfindFilesByHost, fsfindDirsByHost, fsfindFilesBySize, fsfindFilesByModificationTime, fsfindDirsByModificationTime, stop,
    interpret,
    makeLocalInterpreter, makeRemoteInterpreter,
    makeLocalInterpreter2, makeRemoteToLocalInterpreter2, makeRemoteToRemoteInterpreter2, makeLocalToRemoteInterpreter2,
    Interpreter(..), Interpreter2(..), LIO(..)
          ) where

import QueryArrow.Syntax.Data
import QueryArrow.DB.DB
import QueryArrow.Semantics.ResultStream
import QueryArrow.Semantics.ResultValue
import QueryArrow.Semantics.ResultValue.AbstractResultValue
import QueryArrow.Semantics.ResultRow.VectorResultRow
import QueryArrow.Semantics.ResultHeader.VectorResultHeader
import QueryArrow.Data.Some
import Data.ByteString (ByteString)
import QueryArrow.Utils
import QueryArrow.FileSystem.LocalCommands

import Control.Monad.Free
import Control.Monad.Trans.State (StateT, get, modify)
import Control.Monad.Trans.Reader (ReaderT, ask)
import Control.Monad.Trans.Class (lift)
import Control.Monad.IO.Class (MonadIO(..))
import Data.Map.Strict (insert)
import Data.Time.Clock
import Data.Maybe
import qualified Data.Vector as V
import Control.Arrow ((***))
import QueryArrow.FileSystem.Interpreter
import Data.MessagePack hiding (Some)

data FSCommand x where
   DirExists :: File -> (Bool -> x) -> FSCommand x
   FileExists :: File -> (Bool -> x) -> FSCommand x
   CopyFile :: File -> File -> x -> FSCommand x
   CopyDir :: File -> File -> x -> FSCommand x
   UnlinkFile :: File -> x -> FSCommand x
   RemoveDir :: File -> x -> FSCommand x
   MakeFile :: File -> x -> FSCommand x
   MakeDir :: File -> x -> FSCommand x
   Write :: File -> Integer -> ByteString -> x -> FSCommand x
   Read :: File -> Integer -> Integer -> (ByteString -> x) -> FSCommand x
   ListDirDir :: File -> ([File] -> x) -> FSCommand x
   ListDirFile :: File -> ([File] -> x) -> FSCommand x
   Foreach :: forall x a. [a] -> (a -> x) -> FSCommand x
   AllFiles :: ([File] -> x) -> FSCommand x
   AllDirs :: ([File] -> x) -> FSCommand x
   AllNonRootFiles :: ([File] -> x) -> FSCommand x
   AllNonRootDirs :: ([File] -> x) -> FSCommand x
   FindFilesByName :: String -> ([File] -> x) -> FSCommand x
   FindDirsByName :: String -> ([File] -> x) -> FSCommand x
   FindFilesByPath :: String -> ([File] -> x) -> FSCommand x
   FindDirsByPath :: String -> ([File] -> x) -> FSCommand x
   FindFileById :: Integer -> (Maybe File -> x) -> FSCommand x
   FindDirById :: Integer -> (Maybe File -> x) -> FSCommand x
   FindFilesByMode :: Integer -> ([File] -> x) -> FSCommand x
   FindDirsByMode :: Integer -> ([File] -> x) -> FSCommand x
   FindFilesByHost :: String -> ([File] -> x) -> FSCommand x
   FindDirsByHost :: String -> ([File] -> x) -> FSCommand x
   FindFilesBySize :: Integer -> ([File] -> x) -> FSCommand x
   FindFilesByModificationTime :: Integer -> ([File] -> x) -> FSCommand x
   FindDirsByModficationTime :: Integer -> ([File] -> x) -> FSCommand x
   Stat :: File -> (Maybe Stats -> x) -> FSCommand x
   Mode :: File -> (Integer -> x) -> FSCommand x
   Id :: File -> (Integer -> x) -> FSCommand x
   Truncate :: File -> Integer -> x -> FSCommand x
   Size :: File -> (Integer -> x) -> FSCommand x
   ModificationTime :: File -> (UTCTime -> x) -> FSCommand x
   MoveFile :: File -> File -> x -> FSCommand x
   MoveDir :: File -> File -> x -> FSCommand x
   EvalResultValue :: Expr -> (ConcreteResultValue -> x) -> FSCommand x
   SetResultValue :: Var -> ConcreteResultValue -> x -> FSCommand x
   Stop :: FSCommand x

deriving instance Functor FSCommand

type FSProgram = Free FSCommand


dirExists :: File -> FSProgram Bool
dirExists a = liftF (DirExists a id)

fileExists :: File -> FSProgram Bool
fileExists a = liftF (FileExists a id)

fscopyFile :: File -> File -> FSProgram ()
fscopyFile a b = liftF (CopyFile a b ())

fscopyDir :: File -> File -> FSProgram ()
fscopyDir a b = liftF (CopyDir a b ())

unlinkFile :: File -> FSProgram ()
unlinkFile a = liftF (UnlinkFile a ())

removeDir :: File -> FSProgram ()
removeDir a = liftF (RemoveDir a ())

makeFile :: File -> FSProgram ()
makeFile a = liftF (MakeFile a ())

makeDir :: File -> FSProgram ()
makeDir a = liftF (MakeDir a ())

fswrite :: File -> Integer -> ByteString -> FSProgram ()
fswrite a b d = liftF (Write a b d ())

fsread :: File -> Integer -> Integer -> FSProgram ByteString
fsread a b c = liftF (Read a b c id)

fsallFiles :: FSProgram [File]
fsallFiles = liftF (AllFiles id)

fsallDirs :: FSProgram [File]
fsallDirs = liftF (AllDirs id)

fsallNonRootFiles :: FSProgram [File]
fsallNonRootFiles = liftF (AllNonRootFiles id)

fsallNonRootDirs :: FSProgram [File]
fsallNonRootDirs = liftF (AllNonRootDirs id)

fsfindFilesByName :: String -> FSProgram [File]
fsfindFilesByName n = liftF (FindFilesByName n id)

fsfindDirsByName :: String -> FSProgram [File]
fsfindDirsByName n = liftF (FindDirsByName n id)

fsfindFilesByPath :: String -> FSProgram [File]
fsfindFilesByPath n = liftF (FindFilesByPath n id)

fsfindDirsByPath :: String -> FSProgram [File]
fsfindDirsByPath n = liftF (FindDirsByPath n id)

fsfindFileById :: Integer -> FSProgram (Maybe File)
fsfindFileById n = liftF (FindFileById n id)

fsfindDirById :: Integer -> FSProgram (Maybe File)
fsfindDirById n = liftF (FindDirById n id)

fsfindFilesByMode :: Integer -> FSProgram [File]
fsfindFilesByMode n = liftF (FindFilesByMode n id)

fsfindDirsByMode :: Integer -> FSProgram [File]
fsfindDirsByMode n = liftF (FindDirsByMode n id)

fsfindFilesByHost :: String -> FSProgram [File]
fsfindFilesByHost n = liftF (FindFilesByHost n id)

fsfindDirsByHost :: String -> FSProgram [File]
fsfindDirsByHost n = liftF (FindDirsByHost n id)

fsfindFilesBySize :: Integer -> FSProgram [File]
fsfindFilesBySize n = liftF (FindFilesBySize n id)

fsfindFilesByModificationTime :: Integer -> FSProgram [File]
fsfindFilesByModificationTime n = liftF (FindFilesByModificationTime n id)

fsfindDirsByModificationTime :: Integer -> FSProgram [File]
fsfindDirsByModificationTime n = liftF (FindDirsByModficationTime n id)

listDirDir :: File -> FSProgram [File]
listDirDir a = liftF (ListDirDir a id)

listDirFile :: File -> FSProgram [File]
listDirFile a = liftF (ListDirFile a id)

foreach :: [a] -> FSProgram a
foreach as = liftF (Foreach as id)

stat :: File -> FSProgram (Maybe Stats)
stat a = liftF (Stat a id)

fsmode :: File -> FSProgram Integer
fsmode a = liftF (Mode a id)

fsid :: File -> FSProgram Integer
fsid a = liftF (Id a id)

fssize :: File -> FSProgram Integer
fssize a = liftF (Size a id)

fstruncate :: File -> Integer -> FSProgram ()
fstruncate a i = liftF (Truncate a i ())

fsmodificationTime :: File -> FSProgram UTCTime
fsmodificationTime a = liftF (ModificationTime a id)

moveFile :: File -> File -> FSProgram ()
moveFile a b = liftF (MoveFile a b ())

moveDir :: File -> File -> FSProgram ()
moveDir a b = liftF (MoveDir a b ())

stop :: FSProgram a
stop = liftF Stop

setResultValue :: Var -> ConcreteResultValue -> FSProgram ()
setResultValue var expr = liftF (SetResultValue var expr ())

evalResultValue :: Expr -> FSProgram ConcreteResultValue
evalResultValue expr = liftF (EvalResultValue expr id)

newtype LIO a = LIO {runLIO :: IO [a]}

instance Functor LIO where
  fmap f (LIO a) = LIO (fmap (map f) a)

instance Applicative LIO where
  f <*> a = do
    a2 <- a
    f2 <- f
    return (f2 a2)

instance Monad LIO where
  return a = LIO (return [a])
  LIO a >>= f = LIO (do
    as <- a
    concat <$> mapM (runLIO . f) as)

instance MonadIO LIO where
  liftIO a = LIO (do
    a2 <- a
    return [a2])

type InterMonad = StateT (ResultHeader, VectorResultRow AbstractResultValue) (ReaderT ([((String, String), Interpreter)], [((String, String, String, String), Interpreter2)]) LIO) -- (DBResultStream (VectorResultRow AbstractResultValue))

redirect :: (MessagePack a) => LocalizedFSCommand a -> String -> String -> InterMonad a
redirect cmd hosta roota = do
  (hostmap, _) <- lift ask
  let i = fromMaybe (error "cannot find host or root") (lookup (hosta, roota) hostmap)
  interpreter i cmd

redirect2 :: LocalizedFSCommand2  -> String -> String -> String -> String -> InterMonad ()
redirect2 cmd hosta roota hostb rootb = do
  (_, hostmap2) <- lift ask
  let i = fromMaybe (error "cannot find host or root") (lookup (hosta, roota, hostb, rootb) hostmap2)
  interpreter2 i cmd

runPredicate :: (MessagePack a, Monoid a) => LocalizedFSCommand a -> InterMonad a
runPredicate predicate = do
  (hostmap, _) <- lift ask
  mconcat <$> mapM (\(_, ia) -> interpreter ia predicate) hostmap

runPredicateMaybe :: (MessagePack a) => LocalizedFSCommand (Maybe a) -> InterMonad (Maybe a)
runPredicateMaybe predicate = do
  (hostmap, _) <- lift ask
  rpm hostmap where
    rpm [] = return Nothing
    rpm ((_, ia) : hm2) = do
      a <- interpreter ia predicate
      case a of
        Nothing -> rpm hm2
        Just a -> return (Just a)

interpret :: FSCommand ( InterMonad ()) ->  InterMonad ()
interpret (DirExists (File hosta roota a) next) = do
  b <- redirect (LDirExists a) hosta roota
  next b

interpret (FileExists (File hosta roota a) next) = do
  b <- redirect (LFileExists a) hosta roota
  next b

interpret (UnlinkFile (File hosta roota a) next) = do
  redirect (LUnlinkFile a) hosta roota
  next

interpret (RemoveDir (File hosta roota a) next) = do
  redirect (LRemoveDir a) hosta roota
  next

interpret (MakeFile (File hosta roota a) next) = do
  redirect (LMakeFile a ) hosta roota
  next

interpret (MakeDir (File hosta roota a) next) = do
  redirect (LMakeDir a ) hosta roota
  next

interpret (Write (File hosta roota a) b c next) = do
  redirect (LWrite a b c ) hosta roota
  next

interpret (Read (File hosta roota a) b d next) = do
  bs <- redirect (LRead a b d ) hosta roota
  next bs

interpret (ListDirDir (File hosta roota a) next) = do
  fs <- redirect (LListDirDir a ) hosta roota
  next fs

interpret (ListDirFile (File hosta roota a) next) = do
  fs <- redirect (LListDirFile a ) hosta roota
  next fs

interpret (Stat (File hosta roota a) next) = do
  st <- redirect (LStat a ) hosta roota
  next st

interpret (Mode (File hosta roota a) next) = do
  st <- redirect (LMode a ) hosta roota
  next st

interpret (Id (File hosta roota a) next) = do
  st <- redirect (LId a ) hosta roota
  next st

interpret (Size (File hosta roota fn) next) = do
  s <- redirect (LSize fn ) hosta roota
  next s

interpret (Truncate (File hosta roota fn) i next) = do
  redirect (LTruncate fn i ) hosta roota
  next

interpret (ModificationTime (File hosta roota fn) next) = do
  mt <- redirect (LModificationTime fn ) hosta roota
  next mt

interpret (CopyFile (File hosta roota a) (File hostb rootb b) next) = do
  redirect2 (L2CopyFile a b ) hosta roota hostb rootb
  next

interpret (CopyDir (File hosta roota a) (File hostb rootb b) next) = do
  redirect2 (L2CopyDir a b ) hosta roota hostb rootb
  next

interpret (MoveFile (File hosta roota a) (File hostb rootb b) next) = do
  redirect2 (L2MoveFile a b ) hosta roota hostb rootb
  next

interpret (MoveDir (File hosta roota a) (File hostb rootb b) next) = do
  redirect2 (L2MoveDir a b ) hosta roota hostb rootb
  next

interpret (AllFiles next) = do
  fs <- runPredicate LAllFiles
  next fs

interpret (AllDirs next) = do
  fs <- runPredicate LAllDirs
  next fs

interpret (AllNonRootFiles next) = do
  fs <- runPredicate LAllNonRootFiles
  next fs

interpret (AllNonRootDirs next) = do
  fs <- runPredicate LAllNonRootDirs
  next fs

interpret (FindFilesByPath p next) = do
  fs <- runPredicate (LFindFilesByPath p)
  next fs

interpret (FindDirsByPath p next) = do
  fs <- runPredicate (LFindDirsByPath p)
  next fs

interpret (FindFilesByMode p next) = do
  fs <- runPredicate (LFindFilesByMode p)
  next fs

interpret (FindDirsByMode p next) = do
  fs <- runPredicate (LFindDirsByMode p)
  next fs

interpret (FindFileById p next) = do
  fs <- runPredicateMaybe (LFindFileById p)
  next fs

interpret (FindDirById p next) = do
  fs <- runPredicateMaybe (LFindDirById p)
  next fs

interpret (FindFilesByName n next) = do
  fs <- runPredicate (LFindFilesByName n)
  next fs

interpret (FindDirsByName n next) = do
  fs <- runPredicate (LFindDirsByName n)
  next fs

interpret (FindFilesByHost n next) = do
  (hostmap, _) <- lift ask
  let hostmap' = filter (\((host2, _), _) -> n == host2) hostmap
  fs <- concat <$> mapM (\(_, ia)  -> interpreter ia LAllFiles) hostmap'
  next fs

interpret (FindDirsByHost n next) = do
  (hostmap, _) <- lift ask
  let hostmap' = filter (\((host2, _), _) -> n == host2) hostmap
  fs <- concat <$> mapM (\(_, ia)  -> interpreter ia LAllDirs) hostmap'
  next fs

interpret (FindFilesBySize n next) = do
  fs <- runPredicate (LFindFilesBySize n)
  next fs

interpret (FindFilesByModificationTime n next) = do
  fs <- runPredicate (LFindFilesByModificationTime n)
  next fs

interpret (FindDirsByModficationTime n next) = do
  fs <- runPredicate (LFindDirsByModficationTime n)
  next fs

interpret (Foreach as next) =
  mapM_ next as

interpret (EvalResultValue a next) = do
  (hdr, row) <- get
  next (case evalExpr hdr row a of Some arv -> toConcreteResultValue arv)

interpret (SetResultValue a b next) = do
  modify ((`V.snoc` a) *** (`V.snoc` (Some b)))
  next

interpret Stop =
  lift . lift $ LIO (return [])
