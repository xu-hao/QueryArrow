{-# LANGUAGE FlexibleContexts, MultiParamTypeClasses, FlexibleInstances, OverloadedStrings, GADTs, ExistentialQuantification, TemplateHaskell , ForeignFunctionInterface #-}

module QueryArrow.FFI.C.TCP where

import Foreign.StablePtr
import Foreign.Ptr
import Foreign.C.Types
import Foreign.C.String
import Foreign.Storable
import System.Log.Logger (errorM, infoM)
import QueryArrow.FFI.Service
import QueryArrow.FFI.Service.TCP

foreign export ccall hs_tcp ::  Ptr (StablePtr (QueryArrowService TcpServiceSession)) -> IO Int
hs_tcp ::  Ptr (StablePtr (QueryArrowService TcpServiceSession)) -> IO Int
hs_tcp  svcptrptr = do
  ptr <- newStablePtr tcpService
  poke svcptrptr ptr
  return 0
