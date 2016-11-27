{-# LANGUAGE FlexibleContexts, MultiParamTypeClasses, FlexibleInstances, OverloadedStrings, GADTs, ExistentialQuantification, TemplateHaskell , ForeignFunctionInterface #-}

module QueryArrow.FFI.C.Local where

import Foreign.StablePtr
import Foreign.Ptr
import Foreign.C.Types
import Foreign.C.String
import Foreign.Storable
import System.Log.Logger (errorM, infoM)
import QueryArrow.FFI.Service
import QueryArrow.FFI.Service.Local

foreign export ccall hs_local :: Ptr (StablePtr (QueryArrowService Session)) -> IO Int
hs_local :: Ptr (StablePtr (QueryArrowService Session)) -> IO Int
hs_local svcptrptr = do
  ptr <- newStablePtr localService
  poke svcptrptr ptr
  return 0
