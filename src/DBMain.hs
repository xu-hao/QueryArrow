{-# LANGUAGE MonadComprehensions, ForeignFunctionInterface #-}
module DBMain where

import FO
import FO.Data
import Parser
import DBQuery
import ICAT
import ResultStream
import FO.Parser
import FO.Config
import Plugins
import FFI

import Prelude hiding (lookup)
import Data.Map.Strict (empty, lookup)
import Text.Parsec (runParser)
import Control.Monad.IO.Class (liftIO)
import Control.Monad.Trans.State.Strict
import Control.Monad.Trans.Class (lift)
import Data.Aeson
import Control.Applicative ((<$>))
import qualified Data.ByteString.Lazy as B
import qualified Data.ByteString.Lazy.Char8 as B8
import System.Plugins.Load
import Data.Either.Utils (fromEither)
import Data.Maybe (fromMaybe)
import Foreign
import Foreign.C
