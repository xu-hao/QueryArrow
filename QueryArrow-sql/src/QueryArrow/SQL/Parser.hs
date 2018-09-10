{-# LANGUAGE FlexibleContexts, MultiParamTypeClasses, FlexibleInstances, OverloadedStrings, GADTs, ExistentialQuantification, TemplateHaskell , ForeignFunctionInterface #-}

module QueryArrow.SQL.Parser where

import Prelude hiding (lookup)
import Control.Applicative (pure)
import System.Log.Logger (errorM, infoM)
import Text.Parsec
import Text.ParserCombinators.Parsec (GenParser)
import qualified Text.Parsec.Token as T
import Algebra.Lattice
import QueryArrow.SQL.SQL
import QueryArrow.Syntax.Term
import Data.List (intercalate)

lexer = T.makeTokenParser T.LanguageDef {
    T.commentStart = "/*",
    T.commentEnd = "*/",
    T.commentLine = "//",
    T.nestedComments = False,
    T.identStart = letter <|> char '_',
    T.identLetter = alphaNum <|> char '_',
    T.opStart = oneOf "=<>|",
    T.opLetter = oneOf "=<>|",
    T.reservedNames = ["select", "where", "and", "like", "order", "in", "max", "min", "sum", "SELECT", "WHERE", "AND", "LIKE", "ORDER", "IN", "MAX", "MIN", "SUM"],
    T.reservedOpNames = ["=", "<>", ">", "<", ">=", "<="],
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
  str <- many (noneOf ['\''] <|> try (do
                                    char '\''
                                    char '\''
                                    return '\''))
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
distinctP = reserved "distinct" <|> reserved "DISTINCT"
groupP = reserved "group" <|> reserved "GROUP"
byP = reserved "by" <|> reserved "BY"
fromP = reserved "from" <|> reserved "FROM"
whereP = reserved "where" <|> reserved "WHERE"
andP = reserved "and" <|> reserved "AND"
notP = reserved "not" <|> reserved "NOT"
havingP = reserved "having" <|> reserved "HAVING"
orderP = reserved "order" <|> reserved "ORDER"
sumP = reserved "sum" <|> reserved "SUM"
countP = reserved "count" <|> reserved "COUNT"
maxP = reserved "max" <|> reserved "MAX"
minP = reserved "min" <|> reserved "MIN"
inP = reserved "in" <|> reserved "IN"
ascP = reserved "asc" <|> reserved "ASC"
descP = reserved "desc" <|> reserved "DESC"
orP = reserved "or" <|> reserved "OR"
asP = reserved "as" <|> reserved "AS"

orderByP :: GenQueryParser SQLOrder
orderByP = 
    try (ascP >> return ASC) <|> (descP >> return DESC)

sqlP :: GenQueryParser SQL
sqlP = do
    selectP
    distinct <- try (distinctP >> return True) <|> return False
    cols <- sepBy (do
        e <- exprP
        try (do
            asP
            var <- Var <$> identifier
            return (var, e)) <|> return (Var "", e)
        ) comma
    tables <- try (fromP >> sepBy tableP comma) <|> return []
    conds <- try (whereP >> condP) <|> return SQLTrueCond
    groups <- try (groupP >> byP >> sepBy colP comma) <|> return []
    havings <- try (havingP >> condP) <|> return SQLTrueCond
    orderBys <- try (orderP >> byP >> sepBy ((,) <$> colP <*> (try orderByP <|> return ASC)) comma) <|> return [] 
    eof
    return (SQLQuery cols tables conds orderBys top distinct groups)

exprP :: GenQueryParser SQLExpr
exprP =
  try (do
    countP
    e <- parens exprP
    return (SQLFuncExpr "count" [e])) <|>
  try (do
    sumP
    e <- parens exprP
    return (SQLFuncExpr "sum" [e])) <|>
  try (do 
    maxP
    e <- parens exprP
    return (SQLFuncExpr "max" [e])) <|>
  try (do
    minP
    e <- parens exprP
    return (SQLFuncExpr "min" [e])) <|>
  try (do
    tablename <- identifier
    dot
    colname <- identifier
    return (SQLColExpr (SQLQualifiedCol (SQLVar tablename) colname))) <|>
    (SQLVarExpr <$> identifier)

colP :: GenQueryParser SQLExpr
colP = try (do
    tablename <- identifier
    dot
    colname <- identifier
    return (SQLColExpr (SQLQualifiedCol (SQLVar tablename) colname))) <|>
    (SQLVarExpr <$> identifier)

condP2 :: GenQueryParser SQLCond
condP2 = try (do
    notP 
    cond <- condP
    return (SQLNotCond cond)) <|> 
    try (parens condP) <|>
    do
        col <- exprP
        op <- condOpP
        col2 <- exprP
        return (SQLCompCond op col col2)

condAndP :: GenQueryParser SQLCond
condAndP = do
    conds <- sepBy1 condP2 andP
    return (foldl1 SQLAndCond conds) 
        
condP :: GenQueryParser SQLCond
condP = do
    conds <- sepBy1 condAndP orP
    return (foldl1 SQLOrCond conds) 

condOpP :: GenQueryParser String
condOpP = try (reservedOp "=" >> return "=") <|>
          try (reservedOp "<>" >> return "<>") <|>
          try (reservedOp ">" >> return ">") <|>
          try (reservedOp "<" >> return "<") <|>
          try (reservedOp ">=" >> return ">=") <|>
            (reservedOp "<=" >> return "<=")
          
tableP :: GenQueryParser FromTable
tableP = do
    tablename <- identifier
    try (do
        asP
        var <- SQLVar <$> identifier
        return (SimpleTable tablename var)) <|> return (SimpleTable tablename (SQLVar ""))
        
