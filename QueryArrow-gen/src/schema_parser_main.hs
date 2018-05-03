module Main where

import QueryArrow.Serialization ()
import Data.Yaml
import System.Environment

import SchemaParser



main :: IO ()
main = do
      [filename, preds, sql_mappings] <- getArgs
      let schm = schema filename
      let s = fst schm
      let t = snd schm
      encodeFile preds s
      encodeFile sql_mappings t
