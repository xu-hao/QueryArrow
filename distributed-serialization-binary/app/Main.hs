{-# LANGUAGE StaticPointers, ExistentialQuantification, ScopedTypeVariables, GADTs, ConstraintKinds, FlexibleInstances, TypeApplications #-}

module Main where

import QueryArrow.Distributed.Serializable
import QueryArrow.Distributed.Serializable.Binary.Serialization
import Data.Binary.Put
import Data.Binary.Get
import Data.Int
import Network
import Data.ByteString.Lazy
import GHC.IO.Handle (hClose)
import System.Environment
import Data.Constraint.Trivial
import QueryArrow.Data.Some
import QueryArrow.Distributed.Serializable.Binary

main :: IO ()
main = do
  [arg] <- getArgs
  case arg of
    "send" -> do
      let bs = serialize @Show (Some (100 :: Int64))
      conn <- connectTo "localhost" (PortNumber 8000)
      hPut conn bs
      hClose conn
    "recv" -> do
      servConn <- listenOn (PortNumber 8000)
      (conn, _, _) <- accept servConn
      bs <- hGetContents conn
      ds <- deserialize @Show bs
      case ds of
          Some d -> do
              print d
              sClose servConn
