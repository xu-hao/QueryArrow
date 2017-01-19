module QueryArrow.FileSystem.FileSystem where

-- http://swizec.com/blog/writing-a-rest-client-in-haskell/swizec/6152

import Data.Char (toLower)

import QueryArrow.FO.Data
import QueryArrow.DB.DB
import QueryArrow.Config

import QueryArrow.FileSystem.Query
import QueryArrow.FileSystem.ICAT
import QueryArrow.FileSystem.Connection

import Debug.Trace

getDB :: ICATDBConnInfo -> IO (AbstractDatabase MapResultRow Formula)
getDB ps = do
    let conn = FileSystemConnInfo
    db <- makeFileSystemDBAdapter (db_namespace ps) (db_icat ps) conn
    return (AbstractDatabase db)
