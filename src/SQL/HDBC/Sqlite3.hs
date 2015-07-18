{-# LANGUAGE GADTs, TypeFamilies, TypeSynonymInstances, MultiParamTypeClasses, FlexibleInstances #-}
module SQL.HDBC.Sqlite3 where

import SQL.SQL
import DBQuery
import FO
import SQL.HDBC

import Database.HDBC
import Database.HDBC.Sqlite3
import Control.Applicative ((<$>))


newtype Sqlite3DBConnInfo = Sqlite3DBConnInfo String

data Sqlite3Connection where
        Sqlite3Connection :: IConnection conn => conn -> Sqlite3Connection

instance QueryDB Sqlite3DBConnInfo Sqlite3Connection HDBCStatement SQLQuery SQLExpr where
        dbConnect (Sqlite3DBConnInfo path) = Sqlite3Connection <$> connectSqlite3 path


instance HDBCConnection Sqlite3Connection where
        extractHDBCConnection (Sqlite3Connection conn) = ConnWrapper conn 