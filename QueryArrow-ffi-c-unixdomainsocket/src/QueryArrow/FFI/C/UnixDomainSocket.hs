{-# LANGUAGE FlexibleContexts, MultiParamTypeClasses, FlexibleInstances, OverloadedStrings, GADTs, ExistentialQuantification, TemplateHaskell , ForeignFunctionInterface #-}

module QueryArrow.FFI.C.UnixDomainSocket where

import Foreign.StablePtr
import Foreign.Ptr
import Foreign.C.Types
import Foreign.C.String
import Foreign.Storable
import System.Log.Logger (errorM, infoM)
import QueryArrow.FFI.Service
import QueryArrow.FFI.Service.UnixDomainSocket
import QueryArrow.FFI.Service.Handle

foreign export ccall hs_unix_domain_socket ::  Ptr (StablePtr (QueryArrowService HandleSession)) -> IO Int
hs_unix_domain_socket ::  Ptr (StablePtr (QueryArrowService HandleSession)) -> IO Int
hs_unix_domain_socket  svcptrptr = do
  ptr <- newStablePtr unixDomainSocketService
  poke svcptrptr ptr
  return 0
