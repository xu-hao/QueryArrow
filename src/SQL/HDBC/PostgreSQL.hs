{-# LANGUAGE GADTs, FlexibleInstances, MultiParamTypeClasses #-}
module SQL.HDBC.PostgreSQL where

import SQL.SQL
import DBQuery
import FO
import SQL.HDBC

import Database.HDBC
import Database.HDBC.PostgreSQL
import Control.Applicative ((<$>))

newtype PostgreSQLDBConnInfo = PostgreSQLDBConnInfo DBConnInfo

data PostgreSQLConnection where
	PostgreSQLConnection :: IConnection conn => conn -> PostgreSQLConnection

instance QueryDB PostgreSQLDBConnInfo PostgreSQLConnection HDBCStatement ([Var], SQL) SQLExpr where
        dbConnect (PostgreSQLDBConnInfo (DBConnInfo host user password db)) = 
                PostgreSQLConnection <$> connectPostgreSQL ("host="++host++" dbname="++db++" user="++user++" password="++password)

instance HDBCConnection PostgreSQLConnection where
	extractHDBCConnection (PostgreSQLConnection conn) = ConnWrapper conn 