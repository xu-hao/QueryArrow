module QueryArrow.FFI.Service where

import FO.Data
import DB.DB

import Data.Text (Text)
import Control.Monad.Trans.Either (EitherT)
import QueryArrow.Data.PredicatesGen

type Error = (Int, Text)

data QueryArrowService a = QueryArrowService {
  getPredicates :: a -> Predicates,
  execQuery :: a -> Formula -> MapResultRow -> EitherT Error IO (),
  getAllResult :: a -> [Var] -> Formula -> MapResultRow -> EitherT Error IO [MapResultRow],
  getSomeResults :: a -> [Var] -> Formula -> MapResultRow -> Int -> EitherT Error IO [MapResultRow],
  qasConnect :: String -> EitherT Error IO a,
  qasDisconnect :: a -> EitherT Error IO (),
  qasCommit :: a -> EitherT Error IO (),
  qasRollback :: a -> EitherT Error IO ()
}
