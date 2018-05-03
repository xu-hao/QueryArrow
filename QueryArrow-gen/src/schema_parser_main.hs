module Main where

import QueryArrow.SQL.SQL
import QueryArrow.Serialization ()
import Data.Yaml
import GHC.Generics

import SchemaParser



main :: IO ()
main = do
      let s = fst schema
      let t = snd schema
      encodeFile "gen/ICATGen.yaml" s
      encodeFile "gen/SQL/ICATGen.yaml" t
