{-# LANGUAGE FlexibleInstances, TypeFamilies, MultiParamTypeClasses #-}

module QueryArrow.FileSystem.Connection where

import Control.Monad.Free
import Control.Monad.Trans.State
import Control.Monad.Trans.Reader
import Control.Monad.IO.Class (liftIO)

import QueryArrow.DB.DB
import QueryArrow.DB.GenericDatabase
import QueryArrow.DB.NoConnection
import QueryArrow.DB.ParametrizedStatement
import QueryArrow.FileSystem.Query
import QueryArrow.FileSystem.Commands
import QueryArrow.Semantics.ResultRow.VectorResultRow
import QueryArrow.Semantics.ResultValue.AbstractResultValue
import QueryArrow.Semantics.ResultSet.ResultStreamResultSet
import QueryArrow.Semantics.ResultSet.VectorResultSetTransformer
import QueryArrow.Semantics.ResultSet
import QueryArrow.Semantics.ResultHeader.VectorResultHeader
import QueryArrow.Semantics.ResultSet.ResultSetResultStreamResultSet
import QueryArrow.Semantics.ResultSet.ResultStreamResultSet
import QueryArrow.Semantics.ResultStream
import Data.Conduit
import Data.Conduit.List (sourceList)
import Data.Proxy
import QueryArrow.Data.Monoid.Action
import Data.Convertible

instance Convertible (ResultHeader, VectorResultRow AbstractResultValue) (ResultHeader, VectorResultRow AbstractResultValue) where
  safeConvert = return

instance IPSDBStatement (FileSystemConn, ResultHeader, FSProgram ()) where
  type ParameterType (FileSystemConn, ResultHeader, FSProgram ()) = (ResultHeader, VectorResultRow AbstractResultValue)
  type PSResultSetType (FileSystemConn, ResultHeader, FSProgram ()) = ResultStreamResultSet (ResultSetTransformer AbstractResultValue) (VectorResultRow AbstractResultValue)
  execWithParams (FileSystemConn hostmap hostmap2, hdr2, qu) (hdr, params) =
    return (ResultStreamResultSet RSId hdr2 (ResultStream (do
      rows <- liftIO $ runLIO (runReaderT (execStateT (iterM interpret qu) (hdr, params)) (hostmap, hostmap2))
      let rows2 = map (\(hdr, row) -> let trans = alignHeaders (Proxy :: Proxy (VectorResultRow AbstractResultValue)) hdr2 hdr :: ResultSetTransformer AbstractResultValue in
                                          (tconvert trans :: ResultRowTransformer AbstractResultValue) `act` row) rows
      sourceList (rows2 :: [VectorResultRow AbstractResultValue]))))
  psdbStmtClose _ = return ()

instance INoConnectionDatabase2 (GenericDatabase FileSystemTrans FileSystemConn) where
    type NoConnectionQueryType (GenericDatabase FileSystemTrans FileSystemConn) = (ResultHeader, FSProgram ())
    type NoConnectionResultSetType (GenericDatabase FileSystemTrans FileSystemConn) = ResultSetResultStreamResultSet (ResultSetTransformer AbstractResultValue) (ResultStreamResultSet (ResultSetTransformer AbstractResultValue) (VectorResultRow AbstractResultValue))
    type NoConnectionInputRowType (GenericDatabase FileSystemTrans FileSystemConn) = VectorResultRow AbstractResultValue
    noConnectionDBStmtExec (GenericDatabase _ fsconn _ _) (hdr, qu) rset = dbStmtExec (PSDBStatement (fsconn, hdr, qu)) rset
