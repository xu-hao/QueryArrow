{-# LANGUAGE FlexibleContexts, MultiParamTypeClasses, FlexibleInstances, OverloadedStrings, GADTs, ExistentialQuantification, ForeignFunctionInterface #-}

module QueryArrow.FFI.C.GenQuery where

import QueryArrow.Syntax.Utils

import Prelude hiding (lookup)
import qualified Data.Text as Text
import Data.Map.Strict (lookup)
import Foreign.StablePtr
import Foreign.Ptr
import Foreign.C.Types
import Foreign.C.String
import Foreign.Storable
import Foreign.Marshal.Array
import System.Log.Logger (errorM, infoM)
import Data.Maybe (fromMaybe)
import Control.Monad.Trans.Either
import Text.Parsec (runParser)
import qualified Data.Vector as V

import QueryArrow.FFI.GenQuery.Parser
import QueryArrow.FFI.Service
import QueryArrow.FFI.Auxiliary
import QueryArrow.FFI.GenQuery.Translate

foreign export ccall hs_gen_query :: StablePtr (QueryArrowService b) -> StablePtr b -> CString -> Ptr (Ptr CString) -> Ptr CInt -> Ptr CInt -> IO Int
hs_gen_query :: StablePtr (QueryArrowService b) -> StablePtr b -> CString -> Ptr (Ptr CString) -> Ptr CInt -> Ptr CInt -> IO Int
hs_gen_query svcptr sessionptr cqu cout ccol crow = do
  svc <- deRefStablePtr svcptr
  session <- deRefStablePtr sessionptr
  qu <- peekCString cqu
  -- putStrLn ("genquery = " ++ qu)
  let (vars, form) = case runParser genQueryP () "" qu of
                Left err -> error (show err)
                Right gq -> translateGenQueryToQAL gq
  res <- runEitherT (getAllResult svc session vars ( form) mempty mempty)
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
          arrelems <- mapM newCString (concatMap (\r -> V.toList (V.map (\v -> Text.unpack (resultValueToString v) ) r)) res)
          arr <- newArray arrelems
          poke cout arr
          return 0
