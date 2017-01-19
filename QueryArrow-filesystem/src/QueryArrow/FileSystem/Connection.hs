{-# LANGUAGE MultiParamTypeClasses, FlexibleInstances, TypeFamilies, OverloadedStrings #-}

module QueryArrow.FileSystem.Connection where

import Prelude hiding (lookup)
import Data.Map.Strict (Map, fromList, empty, foldrWithKey, insert, lookup)
import Data.Text (unpack, Text, pack)
import Data.Convertible
import Control.Monad.IO.Class (liftIO, MonadIO)
import qualified Data.ByteString.Char8 as BL
import Control.Monad.Free
import Control.Monad.Trans.Class
import System.FilePath((</>))
import Control.Applicative((<$>), (<|>))
import Control.Exception(throw)
import Control.Monad

import QueryArrow.FO.Data
import QueryArrow.DB.DB
import QueryArrow.DB.GenericDatabase
import QueryArrow.DB.NoConnection
import QueryArrow.DB.ResultStream
import QueryArrow.Utils ()
import System.IO
import System.Directory
import QueryArrow.FileSystem.Query
import Control.Monad.Trans.State

instance INoConnectionDatabase2 (GenericDatabase FileSystemTrans FileSystemConnInfo) where
    type NoConnectionQueryType (GenericDatabase FileSystemTrans FileSystemConnInfo) = FSProgram ()
    type NoConnectionRowType (GenericDatabase FileSystemTrans FileSystemConnInfo) = MapResultRow
    noConnectionDBStmtExec _ qu rs = do
      row <- rs
      interpret row qu

interpret :: MapResultRow -> FSProgram () ->  DBResultStream MapResultRow
interpret row (Free (DirExists a next)) = do
  b <- liftIO $ doesDirectoryExist a
  interpret row $ next b
interpret row (Free (FileExists a next)) = do
  b <- liftIO $ doesFileExist a
  interpret row $ next b
interpret row (Free (CopyFile a b next)) = do
  liftIO $ copyFile a b
  interpret row $ next
interpret row (Free (CopyDir a b next)) = do
  liftIO $ copyDirectory a b
  interpret row $ next
interpret row (Free (UnlinkFile a next)) = do
  liftIO $ removeFile a
  interpret row $ next
interpret row (Free (RemoveDir a next)) = do
  liftIO $ removeDirectoryRecursive a
  interpret row $ next
interpret row (Free (MakeFile a next)) = do
  liftIO $ appendFile a ""
  interpret row $ next
interpret row (Free (MakeDir a next)) = do
  liftIO $ createDirectory a
  interpret row $ next
interpret row (Free (Write a b c next)) = do
  liftIO $ withFile a WriteMode $ \h -> do
      hSeek h AbsoluteSeek b
      hPutStr h (unpack c)
  interpret row $ next

interpret row (Free (Read a b d next)) = do
  c <- liftIO $ withFile a ReadMode $ \h -> do
      hSeek h AbsoluteSeek b
      BL.hGet h (fromInteger (d - b))
  interpret row $ next (pack (BL.unpack c))
interpret row (Free (ListDir a next)) = do
  ps <- liftIO $ listDirectory a
  p <- listResultStream ps
  interpret row $ next p
interpret row (Free (Stat a next)) = do
  b <- liftIO $ doesFileExist a
  stats <- if b
    then return (Just (Stats False))
    else do
      b2 <- liftIO $ doesDirectoryExist a
      if b2
        then return (Just (Stats True))
        else return Nothing
  interpret row $ next stats

interpret row (Free (MoveFile a b next)) = do
  liftIO $ renameFile b a
  interpret row $ next

interpret row (Free (MoveDir a b next)) = do
  liftIO $ renameDirectory b a
  interpret row $ next

interpret row (Free (EvalText a next)) =
  case evalExpr row a of
    StringValue s -> interpret row $ next s
    _ -> error ("FileSystem: not a string")

interpret row (Free (EvalInteger a next)) =
  case evalExpr row a of
    IntValue s -> interpret row $ next (fromIntegral s)
    _ -> error ("FileSystem: not an integer")

interpret row (Free (SetText a b next)) =
  interpret (insert a (StringValue b) row) $ next

interpret row (Free (SetInteger a b next)) =
  interpret (insert a (IntValue (fromIntegral b)) row) $ next

interpret row (Free Stop) =
  emptyResultStream
interpret row (Pure a) = return row

evalExpr :: MapResultRow -> Expr -> ResultValue
evalExpr row (StringExpr s) = StringValue s
evalExpr row (IntExpr s) = IntValue s
evalExpr row (VarExpr v) = case lookup v row of
    Nothing -> Null
    Just r -> r
evalExpr row expr = error ("evalExpr: unsupported expr " ++ show expr)



copyDirectory ::  FilePath -> FilePath -> IO ()
copyDirectory src dst = do
  whenM (not <$> doesDirectoryExist src) $
    throw (userError "source does not exist")
  whenM (doesFileOrDirectoryExist dst) $
    throw (userError "destination already exists")

  createDirectory dst
  xs <- listDirectory src
  forM_ xs $ \name -> do
    let srcPath = src </> name
    let dstPath = dst </> name
    isDirectory <- doesDirectoryExist srcPath
    if isDirectory
      then copyDirectory srcPath dstPath
      else copyFile srcPath dstPath

  where
    doesFileOrDirectoryExist x = orM [doesDirectoryExist x, doesFileExist x]
    orM xs = or <$> sequence xs
    whenM s r = s >>= flip when r
