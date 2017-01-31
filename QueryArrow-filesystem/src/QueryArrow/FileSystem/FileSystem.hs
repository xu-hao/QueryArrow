module QueryArrow.FileSystem.FileSystem where

import QueryArrow.FO.Data
import QueryArrow.DB.DB
import QueryArrow.Config

import QueryArrow.FileSystem.Query
import QueryArrow.FileSystem.ICAT
import QueryArrow.FileSystem.Connection ()

getDB :: ICATDBConnInfo -> IO (AbstractDatabase MapResultRow Formula)
getDB ps = do
    let conn = FileSystemConnInfo
    db <- makeFileSystemDBAdapter (db_namespace ps) (db_icat ps) (db_host ps) (db_port ps) conn
    return (AbstractDatabase db)
