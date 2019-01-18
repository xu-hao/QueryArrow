{-# LANGUAGE TypeFamilies #-}

module QueryArrow.FFI.Service where

import QueryArrow.Syntax.Term
import QueryArrow.DB.DB
import QueryArrow.Serialization


import Control.Monad.Trans.Except (ExceptT)



data QueryArrowService a = QueryArrowService {
  execQuery :: a -> Formula -> MapResultRow -> ExceptT Error IO (),
  getAllResult :: a -> [Var] -> Formula -> MapResultRow -> ExceptT Error IO [MapResultRow],
  qasConnect :: String -> ExceptT Error IO a,
  qasDisconnect :: a -> ExceptT Error IO (),
  qasPrepare :: a -> ExceptT Error IO (),
  qasBegin :: a -> ExceptT Error IO (),
  qasCommit :: a -> ExceptT Error IO (),
  qasRollback :: a -> ExceptT Error IO ()
}
