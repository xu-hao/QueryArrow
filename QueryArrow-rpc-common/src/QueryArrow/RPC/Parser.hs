{-# LANGUAGE TypeFamilies, MultiParamTypeClasses, FlexibleContexts, ExistentialQuantification, FlexibleInstances, OverloadedStrings #-}
module QueryArrow.RPC.Parser where

import QueryArrow.Syntax.Term
import QueryArrow.DB.DB
import QueryArrow.Syntax.Type
import QueryArrow.Parser
import QueryArrow.RPC.Data

import Prelude hiding (lookup)
import Text.ParserCombinators.Parsec hiding (State)
import Control.Applicative ((<$>), (<*>), (<*), (*>))
import qualified Text.Parsec.Token as T
import qualified Data.Text as TE

-- parser

progp :: FOParser [Command]
progp = do
    commands <- many (do
      whiteSpace
      (reserved "begin" *> return Begin)
          <|> (reserved "prepare" *> return Prepare)
          <|> (reserved "commit" *> return Commit)
          <|> (reserved "rollback" *> return Rollback)
          <|> (Execute <$> formulap))
    eof
    return commands
