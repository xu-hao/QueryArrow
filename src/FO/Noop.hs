module FO.Noop where

import FO.Data
import FO.Parser
import FO.Config

import System.IO.Temp
import GHC.IO.Handle
import System.Process
import Data.List (isInfixOf, intercalate)

data Noop = Noop

getVerifier :: VerificationInfo -> IO TheoremProver
getVerifier _ = return (TheoremProver Noop)

instance TheoremProver_ Noop where
    prove _ _ _ =
        return (Just True)
