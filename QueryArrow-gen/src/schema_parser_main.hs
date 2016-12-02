{-# LANGUAGE TemplateHaskell #-}
module Main where

import qualified Text.Parsec.Token as T
import Data.List
import Control.Applicative ((*>), (<*), (<$>), (<*>))
import System.Environment
import Language.Haskell.TH
import QueryArrow.SQL.SQL hiding (schema)
import QueryArrow.FO.Data

import SchemaParser


showHaskellList :: Show a => [a] -> String
showHaskellList l = "[" ++ intercalate ",\n" (map show l) ++ "]"

main :: IO ()
main = do
      let s = $(fst schema)
      let t = $(snd schema)
      writeFile "gen/ICATGen" (showHaskellList s)
      writeFile "gen/SQL/ICATGen" (showHaskellList t)
