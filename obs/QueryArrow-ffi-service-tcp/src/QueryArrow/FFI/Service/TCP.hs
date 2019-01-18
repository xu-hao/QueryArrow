{-# LANGUAGE FlexibleContexts, MultiParamTypeClasses, FlexibleInstances, OverloadedStrings, GADTs, ExistentialQuantification, TypeApplications, ScopedTypeVariables, DeriveGeneric #-}

module QueryArrow.FFI.Service.TCP where

import Prelude hiding (lookup)
import Data.Text (pack)
import Control.Exception (SomeException, try)
import Control.Monad.Error.Class (throwError)
import Control.Monad.IO.Class (liftIO)
import System.Log.Logger (infoM, errorM)
import Data.Aeson
import QueryArrow.FFI.Service
import GHC.Generics
import System.IO (Handle)
import qualified Data.Text as T
import Data.Text.Encoding
import Control.Monad.Trans.Either (EitherT)
import Network
import QueryArrow.FFI.Service.Handle
import QueryArrow.Serialization
import Data.ByteString.Lazy (fromStrict)

data TcpClientConfig = TcpClientConfig {
    tcpServerAddr :: String,
    tcpServerPort :: Int
} deriving (Generic, Show)

instance FromJSON TcpClientConfig

tcpConnect :: String -> EitherT Error IO Handle
tcpConnect path = do
    liftIO $ infoM "TCP Service" ("parsing configuration from " ++ path)
    let ps = decode (fromStrict (encodeUtf8 (T.pack path))) :: Maybe TcpClientConfig
    case ps of
        Nothing -> do
            let msg = "cannot parser json"
            liftIO $ errorM "TCP Service" msg
            throwError (-1, pack msg)
        Just ps -> do
            handle <- liftIO $ try (connectTo (tcpServerAddr ps) (PortNumber (fromIntegral (tcpServerPort ps))))
            case handle of
              Left e -> throwError (-1, pack (show (e :: SomeException)))
              Right handle -> return handle

tcpService :: QueryArrowService HandleSession
tcpService = handleService tcpConnect
