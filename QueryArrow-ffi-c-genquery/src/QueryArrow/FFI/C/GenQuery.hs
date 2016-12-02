{-# LANGUAGE FlexibleContexts, MultiParamTypeClasses, FlexibleInstances, OverloadedStrings, GADTs, ExistentialQuantification, ForeignFunctionInterface #-}

module QueryArrow.FFI.C.GenQuery where

import QueryArrow.FO.Data (Pred, Formula(..), Var(..), Expr(..), Atom(..), Aggregator(..), Summary(..), Lit(..), Sign(..))

import Prelude hiding (lookup)
import Data.Set (Set, singleton, fromList, empty)
import Data.Text (Text, pack)
import qualified Data.Text as Text
import Control.Exception (catch, SomeException)
import Control.Applicative (liftA2, pure)
import Data.Map.Strict (lookup)
import Foreign.StablePtr
import Foreign.Ptr
import Foreign.C.Types
import Foreign.C.String
import Foreign.Storable
import Foreign.Marshal.Array
import System.Log.Logger (errorM, infoM)
import Data.List (find, intercalate, nub)
import Data.Maybe (fromMaybe)
import Control.Monad.Trans.Either
import Text.Parsec (runParser)

import QueryArrow.FFI.GenQuery.Parser
import QueryArrow.FFI.Service
import QueryArrow.FFI.Auxiliary
import QueryArrow.FFI.GenQuery.Translate
import QueryArrow.Data.Abstract
import QueryArrow.Data.PredicatesGen
import QueryArrow.ICAT

foreign export ccall hs_gen_query :: StablePtr (QueryArrowService b) -> StablePtr b -> CString -> Ptr (Ptr CString) -> Ptr CInt -> Ptr CInt -> IO Int
hs_gen_query :: StablePtr (QueryArrowService b) -> StablePtr b -> CString -> Ptr (Ptr CString) -> Ptr CInt -> Ptr CInt -> IO Int
hs_gen_query svcptr sessionptr cqu cout ccol crow = do
  svc <- deRefStablePtr svcptr
  session <- deRefStablePtr sessionptr
  qu <- peekCString cqu
  putStrLn ("genquery = " ++ qu)
  let preds = getPredicates svc session
  let (vars, form) = case runParser genQueryP () "" qu of
                Left err -> error (show err)
                Right gq -> translateGenQueryToQAL gq
  res <- runEitherT (getAllResult svc session vars (formula preds form) mempty)
  case res of
    Left err -> error (show err)
    Right res -> do
      let col = length vars
      let row = length res
      if row == 0
        then return eCAT_NO_ROWS_FOUND
        else do
          poke ccol (fromIntegral col)
          poke crow (fromIntegral row)
          infoM "Plugin" ("GenQuery " ++ show res)
          arrelems <- mapM newCString (concatMap (\r -> map (\v -> Text.unpack (resultValueToString (fromMaybe (error ("cannot find column " ++ show v ++ show r)) (lookup v r)))) vars ) res)
          arr <- newArray arrelems
          poke cout arr
          return 0
