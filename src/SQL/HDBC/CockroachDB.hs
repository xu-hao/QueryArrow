{-# LANGUAGE GADTs, FlexibleInstances, MultiParamTypeClasses, OverloadedStrings #-}
module SQL.HDBC.CockroachDB where

import DB.GenericDatabase
import DB.DB
import FO.Data
import SQL.HDBC
import ICAT
import SQL.ICAT
import Config
import SQL.SQL
import Database.HDBC
import Control.Monad.IO.Class

import SQL.HDBC.PostgreSQL as PostgreSQL

getDB :: ICATDBConnInfo -> [AbstractDatabase MapResultRow]
getDB = PostgreSQL.getDB
