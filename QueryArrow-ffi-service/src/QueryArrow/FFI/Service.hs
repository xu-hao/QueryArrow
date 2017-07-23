{-# LANGUAGE TypeFamilies #-}

module QueryArrow.FFI.Service where

import QueryArrow.Syntax.Data
import QueryArrow.Semantics.ResultRow.VectorResultRow
import QueryArrow.Semantics.ResultValue.AbstractResultValue
import QueryArrow.Semantics.ResultHeader.VectorResultHeader
import QueryArrow.DB.DB

import Data.Text (Text)
import Control.Monad.Trans.Either (EitherT)

type Error = (Int, Text)

data QueryArrowService a = QueryArrowService {
  execQuery :: a -> Formula -> ResultHeader -> VectorResultRow AbstractResultValue -> EitherT Error IO (),
  getAllResult :: a -> [Var] -> Formula -> ResultHeader -> VectorResultRow AbstractResultValue -> EitherT Error IO [VectorResultRow AbstractResultValue],
  qasConnect :: String -> EitherT Error IO a,
  qasDisconnect :: a -> EitherT Error IO (),
  qasPrepare :: a -> EitherT Error IO (),
  qasBegin :: a -> EitherT Error IO (),
  qasCommit :: a -> EitherT Error IO (),
  qasRollback :: a -> EitherT Error IO ()
}
