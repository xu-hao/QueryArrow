module SQL.HDBC.CockroachDB where

import DB.DB
import Config

import SQL.HDBC.PostgreSQL as PostgreSQL

getDB :: ICATDBConnInfo -> [AbstractDatabase MapResultRow]
getDB = PostgreSQL.getDB
