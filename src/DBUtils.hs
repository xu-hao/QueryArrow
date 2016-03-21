module DBUtils where

import FO

type DBSession m row a = StateT [Database m row ] m a

dbWithSession :: (MonadCatch m) => [Database m row ] -> DBSession m row a -> m (Either SomeException a)
dbWithSession dbs action = do
    r <- catch (do
        mapM_ (\(Database db) -> dbBegin db) dbs
        r <- evalStateT action dbs
        mapM_ (\(Database db) -> dbPrepare db) dbs
        return (Right r)) (\e -> do
            mapM_ (\(Database db) -> dbRollback db) dbs
            return (Left e))
    mapM_ (\(Database db) -> dbCommit db) dbs
    return r

getDBsFromDBSession :: (Monad m) => DBSession m row [Database m row ]
getDBsFromDBSession = get

getAllResults :: (Monad m, ResultRow row) => Query -> DBSession m row [row]
getAllResults query = do
    dbs <- getDBsFromDBSession
    qp <- lift $ prepareQuery' dbs Nothing  [] [] [] [] query []
    let (_, stream) = execQueryPlan ([], pure mempty) qp
    lift $ getAllResultsInStream stream
