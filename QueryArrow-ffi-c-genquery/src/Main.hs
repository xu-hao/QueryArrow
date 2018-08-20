{-# LANGUAGE FlexibleContexts, MultiParamTypeClasses, FlexibleInstances, OverloadedStrings, GADTs, ExistentialQuantification, ForeignFunctionInterface #-}

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
import System.Log.Logger (errorM, infoM, debugM)
import Data.Maybe (fromMaybe)
import Control.Monad.Trans.Either
import Text.Parsec (runParser)

import QueryArrow.FFI.GenQuery.Parser
import QueryArrow.FFI.Service
import QueryArrow.FFI.Auxiliary
import QueryArrow.FFI.GenQuery.Translate
import QueryArrow.FFI.GenQuery.Data
import QueryArrow.Syntax.Term
import System.Environment

main :: IO ()
main = do
  [qu, dists, accs, ticket, uz, un] <- getArgs
  let dist = read dists
  let acc = read accs
  let (vars, form) = case runParser genQueryP () "" qu of
                Left err -> error (show err)
                Right gq -> translateGenQueryToQAL (dist /= 0) (case acc of
                                                                    1 -> if null ticket then UserAccessControl uz un else TicketAccessControl ticket 
                                                                    -1 -> NoAccessControl
                                                                    0 -> AccessControlTicket) gq
  putStrLn (show vars ++ " " ++ serialize form) 
