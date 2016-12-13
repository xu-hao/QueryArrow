{-# LANGUAGE OverloadedStrings #-}

module QueryArrow.FFI.Service.UnixDomainSocket where

import QueryArrow.FFI.Service

import Data.Text (pack)
import Control.Exception (SomeException, try)
import Control.Monad.Error.Class (throwError)
import Control.Monad.IO.Class (liftIO)
import System.Log.Logger (infoM, errorM)
import System.IO (Handle, IOMode(..))
import Network.Socket
import Control.Monad.Trans.Either
import QueryArrow.FFI.Service.Handle

unixDomainSocketConnect :: String -> EitherT Error IO Handle
unixDomainSocketConnect path = do
              handle <- liftIO $ try (do
                  sock <- socket AF_UNIX Stream defaultProtocol
                  connect sock (SockAddrUnix path)
                  socketToHandle sock ReadWriteMode)
              case handle of
                Left e -> throwError (-1, pack (show (e :: SomeException)))
                Right handle ->
                  return handle

unixDomainSocketService :: QueryArrowService HandleSession
unixDomainSocketService  = handleService unixDomainSocketConnect
