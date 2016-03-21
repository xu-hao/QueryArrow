{-# LANGUAGE GADTs, FlexibleInstances, MultiParamTypeClasses #-}
module Main where

import Config

import System.Environment
import Database.HDBC
import Database.HDBC.PostgreSQL
import Data.List

main :: IO ()
main = do
    args <- getArgs
    ps0 <- getConfig (args !! 1)
    let ps = db_info (head (db_plugins ps0))
    conn <- connectPostgreSQL ("host="++db_host ps++ " port="++show (db_port ps)++" dbname="++db_name ps++" user="++db_username ps++" password="++db_password ps)
    stmt <- prepare conn (head args)
    execute stmt []
    rows <- fetchAllRows stmt
    print rows
