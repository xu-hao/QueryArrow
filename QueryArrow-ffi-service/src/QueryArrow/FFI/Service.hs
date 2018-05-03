{-# LANGUAGE TypeFamilies #-}

module QueryArrow.FFI.Service where

import QueryArrow.FO.Data
import QueryArrow.DB.DB
import QueryArrow.Serialization


import Control.Monad.Trans.Either (EitherT)



data QueryArrowService a = QueryArrowService {
  execQuery :: a -> Formula -> MapResultRow -> EitherT Error IO (),
  getAllResult :: a -> [Var] -> Formula -> MapResultRow -> EitherT Error IO [MapResultRow],
  qasConnect :: String -> EitherT Error IO a,
  qasDisconnect :: a -> EitherT Error IO (),
  qasPrepare :: a -> EitherT Error IO (),
  qasBegin :: a -> EitherT Error IO (),
  qasCommit :: a -> EitherT Error IO (),
  qasRollback :: a -> EitherT Error IO ()
}
