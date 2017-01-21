{-# LANGUAGE TypeSynonymInstances, FlexibleInstances, MultiParamTypeClasses, FlexibleContexts, RankNTypes, GADTs, PatternSynonyms, TypeFamilies, DeriveFunctor, OverloadedStrings #-}
module QueryArrow.FileSystem.Commands where

import QueryArrow.FO.Data hiding (Subst, subst)
import QueryArrow.DB.DB
import QueryArrow.FileSystem.Utils
import Control.Monad.IO.Class (liftIO)
import qualified Data.ByteString.Char8 as BL
import System.Directory
import Control.Exception(throw)
import System.IO
import Data.Text (Text, unpack, pack)
import QueryArrow.DB.ResultStream

import Prelude hiding (lookup)
import Control.Monad.Free
import Control.Monad
import Control.Monad.Trans.State (StateT, get, modify)
import Control.Monad.Trans.Class (lift)
import Data.Map.Strict (insert)


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
      BL.hGet h (fromInteger (d - b))
  next (pack (BL.unpack c))
interpret (ListDirDir a next) = do
  ps <- liftIO $ listDirectory a
  ps2 <- liftIO $ filterM doesDirectoryExist ps
  p <- lift $ listResultStream ps2
  next p
interpret (ListDirFile a next) = do
  ps <- liftIO $ listDirectory a
  ps2 <- liftIO $ filterM doesFileExist ps
  p <- lift $ listResultStream ps2
  next p
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

interpret (SetText a b next) = do
  modify (insert a (StringValue b))
  next

interpret (SetInteger a b next) = do
  modify (insert a (IntValue (fromIntegral b)))
  next

interpret Stop =
  lift $ emptyResultStream
