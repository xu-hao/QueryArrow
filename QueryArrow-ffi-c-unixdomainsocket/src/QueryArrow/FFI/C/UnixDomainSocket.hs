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

foreign export ccall hs_unix_domain_socket :: CString -> Ptr (StablePtr (QueryArrowService UnixDomainSocketServiceSession)) -> IO Int
hs_unix_domain_socket :: CString -> Ptr (StablePtr (QueryArrowService UnixDomainSocketServiceSession)) -> IO Int
hs_unix_domain_socket cpath svcptrptr = do
  path <- peekCString cpath
  ptr <- newStablePtr (unixDomainSocketService path)
  poke svcptrptr ptr
  return 0
