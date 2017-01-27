{-# LANGUAGE DeriveFunctor #-}
module QueryArrow.FileSystem.Commands where

import QueryArrow.FO.Data
import QueryArrow.DB.DB
import QueryArrow.FileSystem.Utils
import Control.Monad.IO.Class (liftIO)
import System.Directory
import Control.Exception(throw)
import System.IO
import Data.Text (Text, unpack)
import QueryArrow.DB.ResultStream
import Data.ByteString (ByteString, hGet)
import QueryArrow.Utils

import Control.Monad.Free
import Control.Monad
import Control.Monad.Trans.State (StateT, get, modify)
import Control.Monad.Trans.Class (lift)
import Data.Map.Strict (insert)
import System.FilePath ((</>))
import Data.Time.Clock
import System.FilePath.Find

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
                 | Read String Integer Integer (ByteString -> x)
                 | ListDirDir String ([String] -> x)
                 | ListDirFile String ([String] -> x)
                 | Foreach [String] (String -> x)
                 | ForeachInteger [Integer] (Integer -> x)
                 | Find RecursionPredicate FilterPredicate String ([String] -> x)
                 | Stat String (Maybe Stats -> x)
                 | Size String (Integer -> x)
                 | ModificationTime String (UTCTime -> x)
                 | MoveFile String String x
                 | MoveDir String String x
                 | EvalByteString Expr (ByteString -> x)
                 | EvalText Expr (Text -> x)
                 | EvalInteger Expr (Integer -> x)
                 | SetByteString Var ByteString x
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

fsread :: String -> Integer -> Integer -> FSProgram ByteString
fsread a b c = liftF (Read a b c id)

fsfind :: RecursionPredicate -> FilterPredicate -> String -> FSProgram [String]
fsfind p q a = liftF (Find p q a id)

listDirDir :: String -> FSProgram [String]
listDirDir a = liftF (ListDirDir a id)

listDirFile :: String -> FSProgram [String]
listDirFile a = liftF (ListDirFile a id)

foreach :: [String] -> FSProgram String
foreach as = liftF (Foreach as id)

foreachInteger :: [Integer] -> FSProgram Integer
foreachInteger as = liftF (ForeachInteger as id)

stat :: String -> FSProgram (Maybe Stats)
stat a = liftF (Stat a id)

size :: String -> FSProgram Integer
size a = liftF (Size a id)

fsmodificationTime :: String -> FSProgram UTCTime
fsmodificationTime a = liftF (ModificationTime a id)

moveFile :: String -> String -> FSProgram ()
moveFile a b = liftF (MoveFile a b ())

moveDir :: String -> String -> FSProgram ()
moveDir a b = liftF (MoveDir a b ())

stop :: FSProgram a
stop = liftF Stop

setText :: Var -> Text -> FSProgram ()
setText var expr = liftF (SetText var expr ())

setInteger :: Var -> Integer -> FSProgram ()
setInteger var expr = liftF (SetInteger var expr ())

setByteString :: Var -> ByteString -> FSProgram ()
setByteString var expr = liftF (SetByteString var expr ())

evalText :: Expr -> FSProgram Text
evalText expr = liftF (EvalText expr id)

evalInteger :: Expr -> FSProgram Integer
evalInteger expr = liftF (EvalInteger expr id)

evalByteString :: Expr -> FSProgram ByteString
evalByteString expr = liftF (EvalByteString expr id)

interpret :: FSCommand (StateT MapResultRow DBResultStream ()) ->  StateT MapResultRow DBResultStream ()
interpret (DirExists a next) = do
  b <- liftIO $ doesDirectoryExist a
  next b
interpret (FileExists a next) = do
  b <- liftIO $ doesFileExist a
  next b
interpret (CopyFile a b next) = do
  liftIO $ copyFile a b
  next
interpret (CopyDir a b next) = do
  liftIO $ copyDirectory a b
  next
interpret (UnlinkFile a next) = do
  liftIO $ removeFile a
  next
interpret (RemoveDir a next) = do
  liftIO $ removeDirectoryRecursive a
  next
interpret (MakeFile a next) = do
  liftIO $ do
    b <- doesPathExist a
    if b
      then
        throw (userError "file already exists")
      else
        appendFile a ""
  next
interpret (MakeDir a next) = do
  liftIO $ createDirectory a
  next
interpret (Write a b c next) = do
  liftIO $ withFile a WriteMode $ \h -> do
      hSeek h AbsoluteSeek b
      hPutStr h (unpack c)
  next

interpret (Read a b d next) = do
  c <- liftIO $ withFile a ReadMode $ \h -> do
      hSeek h AbsoluteSeek b
      hGet h (fromInteger (d - b))
  next c

interpret (Find p q a next) = do
  ps <- liftIO $ find p q a
  next ps

interpret (ListDirDir a next) = do
  ps <- liftIO $ listDirectory a
  let ps5 = map (a </>) ps
  ps2 <- liftIO $ filterM doesDirectoryExist ps5
  next ps2

interpret (ListDirFile a next) = do
  ps <- liftIO $ listDirectory a
  let ps5 = map (a </>) ps
  ps2 <- liftIO $ filterM doesFileExist ps5
  next ps2

interpret (Foreach as next) = do
  a <- lift $ listResultStream as
  next a

interpret (ForeachInteger as next) = do
  a <- lift $ listResultStream as
  next a

interpret (Stat a next) = do
  b <- liftIO $ doesFileExist a
  stats <- if b
    then return (Just (Stats False))
    else do
      b2 <- liftIO $ doesDirectoryExist a
      if b2
        then return (Just (Stats True))
        else return Nothing
  next stats

interpret (Size fn next) = do
  s <- liftIO $ getFileSize fn
  next s

interpret (ModificationTime fn next) = do
  mt <- liftIO $ getModificationTime fn
  next mt

interpret (MoveFile a b next) = do
  liftIO $ renameFile b a
  next

interpret (MoveDir a b next) = do
  liftIO $ renameDirectory b a
  next

interpret (EvalText a next) = do
  row <- get
  case evalExpr row a of
    StringValue s -> next s
    _ -> error ("FileSystem: not a string")

interpret (EvalInteger a next) = do
  row <- get
  case evalExpr row a of
    IntValue s -> next (fromIntegral s)
    _ -> error ("FileSystem: not an integer")

interpret (EvalByteString a next) = do
  row <- get
  case evalExpr row a of
    ByteStringValue s -> next s
    _ -> error ("FileSystem: not a byte string")

interpret (SetText a b next) = do
  modify (insert a (StringValue b))
  next

interpret (SetInteger a b next) = do
  modify (insert a (IntValue (fromIntegral b)))
  next

interpret (SetByteString a b next) = do
  modify (insert a (ByteStringValue b))
  next

interpret Stop =
  lift $ emptyResultStream
