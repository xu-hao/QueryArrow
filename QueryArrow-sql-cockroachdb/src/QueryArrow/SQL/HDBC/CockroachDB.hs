module QueryArrow.SQL.HDBC.CockroachDB where

import QueryArrow.FO.Data
import QueryArrow.DB.DB
import QueryArrow.Config

import QueryArrow.SQL.HDBC.PostgreSQL as PostgreSQL

getDB :: ICATDBConnInfo -> IO (AbstractDatabase MapResultRow Formula)
getDB = PostgreSQL.getDB
