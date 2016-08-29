{-# LANGUAGE TemplateHaskell #-}
module SchemaParserMain where

import qualified Text.Parsec.Token as T
import Data.List
import Control.Applicative ((*>), (<*), (<$>), (<*>))
import System.Environment
import Language.Haskell.TH
import SQL.SQL hiding (schema)
import FO.Data

import SchemaParser


showHaskellList :: Show a => [a] -> String
showHaskellList l = "[\n" ++ intercalate ",\n" (map (\a -> "    " ++ show a) l) ++ "\n    ]"

showHaskellSQLMappingList :: [(String, (Table, [SQLQualifiedCol]))] -> String
showHaskellSQLMappingList l = "[\n" ++ intercalate ",\n" (map (\(p, (OneTable tn (SQLVar v), c)) -> "    (" ++ show p ++",(OneTable "++show tn++ " (SQLVar "++show v++"),["++intercalate "," (map (\(SQLVar v,s) -> "(SQLVar "++show v++","++show s++")") c)++"]))") l) ++ "\n    ]"

main :: IO ()
main = do
      let s = $(fst schema)
      let t = $(snd schema)
      writeFile "gen/ICATGen" (showHaskellList s)
      writeFile "gen/SQL/ICATGen" (showHaskellSQLMappingList t)
