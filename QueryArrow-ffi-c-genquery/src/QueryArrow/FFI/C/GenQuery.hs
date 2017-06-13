{-# LANGUAGE FlexibleContexts, MultiParamTypeClasses, FlexibleInstances, OverloadedStrings, GADTs, ExistentialQuantification, ForeignFunctionInterface #-}

module QueryArrow.FFI.C.GenQuery where

import QueryArrow.FO.Utils

import Prelude hiding (lookup)
import qualified Data.Text as Text
import Data.Map.Strict (lookup)
import Foreign.StablePtr
import Foreign.Ptr
import Foreign.C.Types
import Foreign.C.String
import Foreign.Storable
import Foreign.Marshal.Array
import System.Log.Logger (errorM, infoM, debugM)
import Data.Maybe (fromMaybe)
import Control.Monad.Trans.Either
import Text.Parsec (runParser)

import QueryArrow.FFI.GenQuery.Parser
import QueryArrow.FFI.Service
import QueryArrow.FFI.Auxiliary
import QueryArrow.FFI.GenQuery.Translate
import QueryArrow.FFI.GenQuery.Data
import QueryArrow.FO.Data

foreign export ccall hs_gen_query :: StablePtr (QueryArrowService b) -> StablePtr b -> CInt -> CInt -> CString -> CString -> CString -> CString -> Ptr (Ptr CString) -> Ptr CInt -> Ptr CInt -> IO Int
hs_gen_query :: StablePtr (QueryArrowService b) -> StablePtr b -> CInt -> CInt -> CString -> CString -> CString -> CString -> Ptr (Ptr CString) -> Ptr CInt -> Ptr CInt -> IO Int
hs_gen_query svcptr sessionptr cdist cacc cuserzone cusername cticket cqu cout ccol crow = do
  svc <- deRefStablePtr svcptr
  session <- deRefStablePtr sessionptr
  let dist = fromIntegral cdist
  let acc = fromIntegral cacc
  uz <- peekCString cuserzone
  un <- peekCString cusername
  ticket <- peekCString cticket
  qu <- peekCString cqu
--  putStrLn ("hs_gen_query: genquery = " ++ qu)
  debugM "GENQUERY" ("hs_gen_query: genquery = " ++ qu)
  let (gq, (vars, form)) = case runParser genQueryP () "" qu of
                Left err -> error (show err)
                Right gq -> (gq, translateGenQueryToQAL (dist /= 0) (case acc of
                                                                    1 -> if null ticket then UserAccessControl uz un else TicketAccessControl ticket 
                                                                    -1 -> NoAccessControl
                                                                    0 -> AccessControlTicket) gq)
  debugM "GENQUERY" ("translateGenQueryToQAL: \n------------------\n" ++ show gq ++ "\n----------------->\n" ++ serialize form ++ "\n------------------------" )
  res <- runEitherT (getAllResult svc session vars ( form) mempty)
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
