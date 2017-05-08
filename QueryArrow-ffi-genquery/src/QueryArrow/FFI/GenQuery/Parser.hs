{-# LANGUAGE FlexibleContexts, MultiParamTypeClasses, FlexibleInstances, OverloadedStrings, GADTs, ExistentialQuantification, TemplateHaskell , ForeignFunctionInterface #-}

module QueryArrow.FFI.GenQuery.Parser where

import Prelude hiding (lookup)
import Control.Applicative (pure)
import System.Log.Logger (errorM, infoM)
import Text.Parsec
import Text.ParserCombinators.Parsec (GenParser)
import qualified Text.Parsec.Token as T
import QueryArrow.FFI.GenQuery.Data

lexer = T.makeTokenParser T.LanguageDef {
    T.commentStart = "/*",
    T.commentEnd = "*/",
    T.commentLine = "//",
    T.nestedComments = False,
    T.identStart = letter <|> char '_',
    T.identLetter = alphaNum <|> char '_',
    T.opStart = oneOf "=<>|",
    T.opLetter = oneOf "=<>|",
    T.reservedNames = ["select", "where", "and", "like", "parent_of", "order", "SELECT", "WHERE", "AND", "LIKE", "PARENT_OF", "ORDER"],
    T.reservedOpNames = ["=", "<>", "||", "!="],
    T.caseSensitive = True
}

identifier = T.identifier lexer
reserved = T.reserved lexer
reservedOp = T.reservedOp lexer

parens :: GenQueryParser a -> GenQueryParser a
parens = T.parens lexer

brackets :: GenQueryParser a -> GenQueryParser a
brackets = T.brackets lexer

braces :: GenQueryParser a -> GenQueryParser a
braces = T.braces lexer

integer = T.integer lexer
stringp :: GenQueryParser String
stringp = do
  char '\''
  str <- many (noneOf ['\''])
  char '\''
  whiteSpace
  return str
comma = T.comma lexer
semi = T.semi lexer
symbol = T.symbol lexer
whiteSpace = T.whiteSpace lexer
dot = T.dot lexer

-- parser
type GenQueryParser = GenParser Char ()

selectP = reserved "select" <|> reserved "SELECT"
whereP = reserved "where" <|> reserved "WHERE"
andP = reserved "and" <|> reserved "AND"
likeP = reserved "like" <|> reserved "LIKE"
parentOfP = reserved "parent_of" <|> reserved "PARENT_OF"
orderP = reserved "order" <|> reserved "ORDER"

genQueryP :: GenQueryParser GenQuery
genQueryP = do
    selectP
    cols <- sepBy selP comma
    conds <- (whereP >> sepBy condP andP) <|> return []
    eof
    return (GenQuery cols conds)

selP :: GenQueryParser Sel
selP =
  try (orderP >> Sel <$> parens identifier <*> pure Order) <|>
    (Sel <$> identifier <*> pure None)

condP :: GenQueryParser Cond
condP = do
    col <- identifier
    Cond col <$> cond2P

cond2P :: GenQueryParser Cond2
cond2P = do
    cond2 <- sepBy1 cond2P' (reservedOp "||")
    return (foldl1 OrCond cond2)

cond2P' :: GenQueryParser Cond2
cond2P' = try (EqString <$> (reservedOp "=" >> stringp)) <|>
          try (EqInteger <$> (reservedOp "=" >> integer)) <|>
          try (NotEqString <$> (reservedOp "<>" >> stringp)) <|>
          try (NotEqInteger <$> (reservedOp "<>" >> integer)) <|>
          try (NotEqString <$> (reservedOp "!=" >> stringp)) <|>
          try (NotEqInteger <$> (reservedOp "!=" >> integer)) <|>
          try (LikeCond <$> (likeP >> stringp)) <|>
          ParentOfCond <$> (parentOfP >> stringp)
