{-# LANGUAGE TypeSynonymInstances, MultiParamTypeClasses, FlexibleInstances #-}
module QueryArrow.SQL.HDBC.CockroachDB where

import QueryArrow.DB.DB
import QueryArrow.Plugin

import QueryArrow.SQL.HDBC.PostgreSQL as PostgreSQL

data CockroachDBPlugin = CockroachDBPlugin

instance Plugin CockroachDBPlugin MapResultRow where
  getDB _ ps adbs = getDB PostgreSQLPlugin ps adbs
