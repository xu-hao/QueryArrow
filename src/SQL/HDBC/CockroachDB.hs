{-# LANGUAGE GADTs, FlexibleInstances, MultiParamTypeClasses, OverloadedStrings #-}
module SQL.HDBC.CockroachDB where

import DBQuery
import QueryPlan
import FO.Data
import SQL.HDBC
import ICAT
import SQL.ICAT
import Config
import SQL.SQL
import Database.HDBC
import Control.Monad.IO.Class

import SQL.HDBC.PostgreSQL as PostgreSQL

getDB :: ICATDBConnInfo -> IO [Database DBAdapterMonad MapResultRow]
getDB = PostgreSQL.getDB
