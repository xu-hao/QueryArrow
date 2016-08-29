module SQL.HDBC.CockroachDB where

import FO.Data
import DB.DB
import Config

import SQL.HDBC.PostgreSQL as PostgreSQL

getDB :: ICATDBConnInfo -> IO (AbstractDatabase MapResultRow Formula)
getDB = PostgreSQL.getDB
