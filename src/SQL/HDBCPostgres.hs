module SQL.HDBCPostgreSQL where

import SQL.SQL

import Database.HDBC
import Database.HDBC.PostgreSQL

newtype PostgreSQLDBConnInfo = PostgreSQLDBConnInfo DBConnInfo

instance QueryDB PostgreSQLDBConnInfo ConnWrapper Statement SQL SQLExpr where
        dbConnect (PostgreSQLDBConnInfo (DBConnInfo host user password db)) = 
                connectPostgreSQL "host="++host++" dbname="++db++" user="++user++" password="++password
